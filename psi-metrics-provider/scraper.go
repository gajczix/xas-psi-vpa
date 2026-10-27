package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

type SampleState struct {
	Timestamp int64
	Value     float64
}

// PSINodeMetricsProvider mirrors xAS's CoreNodeMetricsProvider (scraper.go + kubelet.go),
// reading cgroup v2 PSI and memory.stat counters instead of Kubelet's /metrics/resource.
type PSINodeMetricsProvider struct {
	kubeClient     kubernetes.Interface
	providerLister listers.MetricProviderClassLister

	grpcConn   *grpc.ClientConn
	grpcClient pb.XASServerClient

	clusterName    string
	nodeName       string
	cgroupRoot     string
	scrapeInterval time.Duration

	lastSamples map[string]SampleState
}

func NewPSINodeMetricsProvider(kubeClient kubernetes.Interface, providerLister listers.MetricProviderClassLister, serverAddress, nodeName, clusterName, cgroupRoot string, scrapeInterval time.Duration) *PSINodeMetricsProvider {
	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("did not connect", "error", err)
		os.Exit(1)
	}
	client := pb.NewXASServerClient(conn)

	return &PSINodeMetricsProvider{
		kubeClient:     kubeClient,
		providerLister: providerLister,
		grpcConn:       conn,
		grpcClient:     client,
		clusterName:    clusterName,
		nodeName:       nodeName,
		cgroupRoot:     cgroupRoot,
		scrapeInterval: scrapeInterval,
		lastSamples:    make(map[string]SampleState),
	}
}

func (a *PSINodeMetricsProvider) Run(ctx context.Context) {
	defer a.grpcConn.Close()
	ticker := time.NewTicker(a.scrapeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.scrapeAndSend()
		case <-ctx.Done():
			return
		}
	}
}

func (a *PSINodeMetricsProvider) scrapeAndSend() {
	slog.Info("Scrape cycle starting...")

	// 1. Get Policies from Server (identical to scraper.go)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := a.grpcClient.ListPolicies(ctx, &pb.ListPoliciesRequest{
		ClusterName: a.clusterName,
	})
	if err != nil {
		slog.Error("Error listing policies", "error", err)
		return
	}

	slog.Info("Found policies", "count", len(resp.Policies))

	// 2. Group PSI metrics by Policy (identical to scraper.go)
	policies := make(map[string]*groupedPolicy)

	for _, pol := range resp.Policies {
		var relevantMetrics []*pb.MetricDefinition
		for _, m := range metricDefinitions(pol) {
			class, err := a.providerLister.Get(m.Provider)
			if err != nil {
				slog.Warn("MetricProviderClass not found for metric", "class", m.Provider, "metric", m.Name)
				continue
			}

			if class.Spec.Type == "PSI" {
				relevantMetrics = append(relevantMetrics, m)
			}
		}

		if len(relevantMetrics) > 0 {
			key := fmt.Sprintf("%s/%s", pol.Id.Namespace, pol.Id.Name)
			policies[key] = &groupedPolicy{
				Namespace: pol.Id.Namespace,
				Name:      pol.Id.Name,
				Selector:  pol.Selector,
				Metrics:   relevantMetrics,
			}
		}
	}

	seenKeys := make(map[string]bool)

	// 3. Process each policy (identical to scraper.go)
	for _, pol := range policies {
		a.processGroupedPolicy(pol, seenKeys)
	}

	for k := range a.lastSamples {
		if !seenKeys[k] {
			delete(a.lastSamples, k)
		}
	}
}

type groupedPolicy struct {
	Namespace string
	Name      string
	Selector  string
	Metrics   []*pb.MetricDefinition
}

func (a *PSINodeMetricsProvider) processGroupedPolicy(policy *groupedPolicy, seenKeys map[string]bool) {
	if policy.Selector == "" {
		return
	}

	opts := metav1.ListOptions{
		LabelSelector: policy.Selector,
	}
	if a.nodeName != "" {
		opts.FieldSelector = fmt.Sprintf("spec.nodeName=%s", a.nodeName)
	}

	pods, err := a.kubeClient.CoreV1().Pods(policy.Namespace).List(context.TODO(), opts)
	if err != nil {
		slog.Error("Error listing pods", "error", err)
		return
	}

	var podMetrics []*pb.MetricBatch

	for _, pod := range pods.Items {
		if pod.Status.Phase != "Running" || !isPodReady(&pod) {
			continue
		}

		cgDir, err := findPodCgroupDir(a.cgroupRoot, string(pod.UID))
		if err != nil {
			continue
		}

		for _, m := range policy.Metrics {
			batches := a.processPSIMetric(pod, cgDir, m, seenKeys)
			if len(batches) > 0 {
				podMetrics = append(podMetrics, batches...)
			}
		}
	}

	if len(podMetrics) > 0 {
		req := &pb.IngestMetricsRequest{
			Timestamp: time.Now().Unix(),
			Policies: []*pb.PolicyBatch{
				{
					Namespace: policy.Namespace,
					Name:      policy.Name,
					Batches:   podMetrics,
				},
			},
		}
		a.sendBatch(req)
	}
}

// processPSIMetric mirrors processKubeletMetric & containerUsage in kubelet.go:
// computes windowed rate from cumulative counters using a.lastSamples.
func (a *PSINodeMetricsProvider) processPSIMetric(pod corev1.Pod, cgDir string, m *pb.MetricDefinition, seenKeys map[string]bool) []*pb.MetricBatch {
	metricType := m.Params["type"]
	ts := time.Now().Unix()

	var batches []*pb.MetricBatch
	for _, c := range pod.Spec.Containers {
		val, ok := a.containerPSIMetric(pod, c.Name, cgDir, metricType, m.Name, ts, seenKeys)
		if !ok {
			continue
		}

		batches = append(batches, &pb.MetricBatch{
			PodName:       pod.Name,
			ContainerName: c.Name,
			Samples: []*pb.MetricSample{
				{
					Name:            m.Name,
					RecommenderName: m.RecommenderName,
					ResourceName:    metricType,
					Value:           val,
					Timestamp:       ts,
				},
			},
		})
	}
	return batches
}

// containerPSIMetric mirrors containerUsage() in official kubelet.go:
// converts cumulative counters (memory.pressure, io.pressure, memory.stat) into rates
// using a.lastSamples, and reports current_memory_limit (memory.max) as a gauge.
func (a *PSINodeMetricsProvider) containerPSIMetric(pod corev1.Pod, containerName, cgDir, metricType, metricName string, ts int64, seenKeys map[string]bool) (float64, bool) {
	rateFromCounter := func(rawVal float64, scale float64) (float64, bool) {
		stateKey := fmt.Sprintf("%s/%s/%s/%s/%s", pod.Namespace, pod.Name, containerName, metricType, metricName)
		seenKeys[stateKey] = true

		last, ok := a.lastSamples[stateKey]
		a.lastSamples[stateKey] = SampleState{Timestamp: ts, Value: rawVal}
		if !ok || ts <= last.Timestamp {
			return 0, false
		}
		diff := rawVal - last.Value
		if diff < 0 {
			diff = rawVal
		}
		return (diff / float64(ts-last.Timestamp)) * scale, true
	}

	switch metricType {
	case "memory_psi":
		us, ok := readPressureSomeTotal(filepath.Join(cgDir, "memory.pressure"))
		if !ok {
			return 0, false
		}
		// Convert microseconds/sec to percentage (0..100%)
		pct, ok := rateFromCounter(us, 100.0/1e6)
		return math.Min(100.0, pct), ok

	case "io_psi":
		us, ok := readPressureSomeTotal(filepath.Join(cgDir, "io.pressure"))
		if !ok {
			return 0, false
		}
		pct, ok := rateFromCounter(us, 100.0/1e6)
		return math.Min(100.0, pct), ok

	case "refault_rate":
		refaults, ok := readMemoryStatRefaults(filepath.Join(cgDir, "memory.stat"))
		if !ok {
			return 0, false
		}
		return rateFromCounter(refaults, 1.0)

	case "current_memory_limit":
		if maxBytes, ok := readMemoryMax(filepath.Join(cgDir, "memory.max")); ok && maxBytes > 0 {
			return maxBytes, true
		}
		for _, c := range pod.Spec.Containers {
			if c.Name == containerName {
				if q, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
					return float64(q.Value()), true
				}
			}
		}
	}
	return 0, false
}

func (a *PSINodeMetricsProvider) sendBatch(pbReq *pb.IngestMetricsRequest) {
	pbReq.ClusterName = a.clusterName

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := a.grpcClient.IngestMetrics(ctx, pbReq)
	if err != nil {
		slog.Error("Failed to send batch", "error", err)
		return
	}
	slog.Info("Batch sent", "success", resp.Success)
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func metricDefinitions(p *pb.Policy) []*pb.MetricDefinition {
	metrics := make([]*pb.MetricDefinition, 0, len(p.Metrics))
	metrics = append(metrics, p.Metrics...)
	for _, ml := range p.RecommenderMetrics {
		metrics = append(metrics, ml.GetDefinitions()...)
	}
	return metrics
}

// --- Cgroup v2 File Helpers ---

func findPodCgroupDir(cgroupRoot, podUID string) (string, error) {
	uidUnderscore := strings.ReplaceAll(podUID, "-", "_")
	var found string
	_ = filepath.WalkDir(cgroupRoot, func(path string, d os.DirEntry, err error) error {
		if err == nil && d.IsDir() && (strings.Contains(d.Name(), podUID) || strings.Contains(d.Name(), uidUnderscore)) {
			found = path
			return filepath.SkipAll
		}
		return nil
	})
	if found == "" {
		return "", fmt.Errorf("cgroup not found for pod %s", podUID)
	}
	return found, nil
}

func readPressureSomeTotal(path string) (float64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == "some" {
			for _, kv := range fields[1:] {
				if v, ok := strings.CutPrefix(kv, "total="); ok {
					f, err := strconv.ParseFloat(v, 64)
					return f, err == nil
				}
			}
		}
	}
	return 0, false
}

func readMemoryStatRefaults(path string) (float64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	var sum float64
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && (fields[0] == "workingset_refault_file" || fields[0] == "pswpin") {
			if f, err := strconv.ParseFloat(fields[1], 64); err == nil {
				sum += f
			}
		}
	}
	return sum, true
}

func readMemoryMax(path string) (float64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
	return f, err == nil
}
