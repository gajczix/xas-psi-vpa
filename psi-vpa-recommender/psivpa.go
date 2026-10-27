package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

type scaleHistory struct {
	lastScaleTime time.Time
	targetMiB     int64
}

type PSIVPARecommender struct {
	lastScales map[string]scaleHistory
}

// config holds the parsed configuration from the ScalingPolicy RecommenderDefinition.
// Zero hardcoded defaults: all thresholds, intervals, and metric names are read from def.Params.
type config struct {
	containerName  string
	memPSIMetric   string
	ioPSIMetric    string
	refaultMetric  string
	memLimitMetric string
	targetMemPSI   float64
	targetIOPSI    float64
	minRefaultRate float64
	stepUpFactor   float64
	cooldown       time.Duration
}

// Recommend calculates the resource recommendations based on control metrics,
// mirroring xAS's official internal/recommenders/vpa/vpa.go structure while
// replacing working_set * safetyMargin with Slide 27's 4-metric PSI decision tree.
func (r *PSIVPARecommender) Recommend(def *pb.RecommenderDefinition, state, _ *pb.ControlMetrics) *pb.Recommendation {
	cfg, err := parseConfig(def)
	if err != nil {
		return &pb.Recommendation{
			IsActive: false,
			Message:  fmt.Sprintf("Unable to parse recommender configuration: %v", err),
		}
	}

	if state == nil {
		return &pb.Recommendation{
			IsActive: false,
			Message:  "ControlMetrics is missing",
		}
	}
	if len(state.PodContainerMetrics) == 0 {
		return &pb.Recommendation{
			IsActive: false,
			Message:  "PodMetrics is empty (ensure metrics are configured with scope: PodContainer)",
		}
	}

	// Get the max values across pods for the target container (same pattern as getMaxMemVal in vpa.go)
	memPSI, ok1 := getMaxContainerVal(state.PodContainerMetrics, cfg.containerName, cfg.memPSIMetric)
	ioPSI, ok2 := getMaxContainerVal(state.PodContainerMetrics, cfg.containerName, cfg.ioPSIMetric)
	refaultRate, ok3 := getMaxContainerVal(state.PodContainerMetrics, cfg.containerName, cfg.refaultMetric)
	curLimitBytes, ok4 := getMaxContainerVal(state.PodContainerMetrics, cfg.containerName, cfg.memLimitMetric)

	if !ok1 || !ok2 || !ok3 || !ok4 || curLimitBytes <= 0 {
		return &pb.Recommendation{
			IsActive: false,
			Message:  "Waiting for all 4 PSI metrics (memory_psi, io_psi, refault_rate, current_memory_limit)",
		}
	}

	curMiB := int64(math.Ceil(curLimitBytes / 1024 / 1024))
	if r.lastScales == nil {
		r.lastScales = make(map[string]scaleHistory)
	}
	key := fmt.Sprintf("%s/%s", def.Name, cfg.containerName)
	if prev, ok := r.lastScales[key]; ok {
		// Reset history if the pod was recreated at a smaller initial limit
		if curMiB < prev.targetMiB && time.Since(prev.lastScaleTime) > cfg.cooldown {
			delete(r.lastScales, key)
		} else if time.Since(prev.lastScaleTime) < cfg.cooldown {
			memValString := fmt.Sprintf("%dMi", prev.targetMiB)
			return &pb.Recommendation{
				IsActive: true,
				WorkloadResources: []*pb.ContainerResource{
					{
						ContainerName: cfg.containerName,
						Requests:      map[string]string{"memory": memValString},
						Limits:        map[string]string{"memory": memValString},
					},
				},
				Message: fmt.Sprintf("Recommendation generated (Stabilizing cooldown): %s", memValString),
			}
		}
	}

	// --- Slide 27 Decision Tree ---
	growMemory := false
	var reason string

	if ioPSI > cfg.targetIOPSI {
		// Diamond 1 (YES) -> Diamond 2: Check refault_rate
		if refaultRate > cfg.minRefaultRate {
			// Branch A: Disguised memory starvation -> Grow memory
			growMemory = true
			reason = "Disguised memory starvation (high io_psi + high refault_rate)"
		} else {
			// Branch B: Storage bottleneck without refaults -> Hold memory
			growMemory = false
			reason = "Storage I/O bottleneck without refaults (holding memory)"
		}
	} else if memPSI > cfg.targetMemPSI {
		// Diamond 1 (NO) -> Diamond 3 (YES): Pure memory pressure -> Grow memory
		growMemory = true
		reason = "Pure memory pressure (high memory_psi)"
	} else {
		// Diamond 3 (NO): Healthy -> Hold memory
		growMemory = false
		reason = "Healthy (holding memory)"
	}

	// --- Step-Up Formula (replaces vpa.go's val * cfg.memSafetyMargin) ---
	targetBytes := curLimitBytes
	if growMemory {
		targetBytes = curLimitBytes * (1.0 + cfg.stepUpFactor)
	}

	memMiB := int64(math.Ceil(targetBytes / 1024 / 1024))
	if growMemory {
		r.lastScales[key] = scaleHistory{
			lastScaleTime: time.Now(),
			targetMiB:     memMiB,
		}
	}
	memValString := fmt.Sprintf("%dMi", memMiB)

	requests := map[string]string{"memory": memValString}
	limits := map[string]string{"memory": memValString}

	return &pb.Recommendation{
		IsActive: true,
		WorkloadResources: []*pb.ContainerResource{
			{
				ContainerName: cfg.containerName,
				Requests:      requests,
				Limits:        limits,
			},
		},
		Message: fmt.Sprintf("Recommendation generated (%s): %s", reason, memValString),
	}
}

func getMaxContainerVal(podMetrics map[string]*pb.ContainerMetrics, containerName, metricName string) (float64, bool) {
	var maxVal float64
	found := false

	for _, podMetric := range podMetrics {
		containerMetric, ok := podMetric.GetContainerMetrics()[containerName]
		if !ok || containerMetric == nil {
			continue
		}
		val, ok := containerMetric.Values[metricName]
		if ok {
			if !found || val > maxVal {
				maxVal = val
			}
			found = true
		}
	}
	return maxVal, found
}

// parseConfig extracts and validates parameters from the recommender definition.
// Every parameter is explicitly required from the ScalingPolicy YAML (no hardcoded defaults).
func parseConfig(def *pb.RecommenderDefinition) (*config, error) {
	container := strings.TrimSpace(def.Params["container"])
	if container == "" {
		return nil, fmt.Errorf("container is undefined")
	}

	memPSIMetric := strings.TrimSpace(def.Params["memoryPSIMetric"])
	ioPSIMetric := strings.TrimSpace(def.Params["ioPSIMetric"])
	refaultMetric := strings.TrimSpace(def.Params["refaultRateMetric"])
	memLimitMetric := strings.TrimSpace(def.Params["memoryLimitMetric"])
	if memPSIMetric == "" || ioPSIMetric == "" || refaultMetric == "" || memLimitMetric == "" {
		return nil, fmt.Errorf("all 4 metric params (memoryPSIMetric, ioPSIMetric, refaultRateMetric, memoryLimitMetric) must be defined")
	}

	targetMemPSI, err := strconv.ParseFloat(strings.TrimSpace(def.Params["targetMemPSI"]), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid or missing targetMemPSI: %w", err)
	}

	targetIOPSI, err := strconv.ParseFloat(strings.TrimSpace(def.Params["targetIOPSI"]), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid or missing targetIOPSI: %w", err)
	}

	minRefaultRate, err := strconv.ParseFloat(strings.TrimSpace(def.Params["minRefaultRate"]), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid or missing minRefaultRate: %w", err)
	}

	stepUpFactor, err := strconv.ParseFloat(strings.TrimSpace(def.Params["stepUpFactor"]), 64)
	if err != nil || stepUpFactor <= 0 {
		return nil, fmt.Errorf("invalid or missing stepUpFactor: %v", err)
	}

	cooldown, err := time.ParseDuration(strings.TrimSpace(def.Params["cooldown"]))
	if err != nil || cooldown < 0 {
		return nil, fmt.Errorf("invalid or missing cooldown: %v", err)
	}

	return &config{
		containerName:  container,
		memPSIMetric:   memPSIMetric,
		ioPSIMetric:    ioPSIMetric,
		refaultMetric:  refaultMetric,
		memLimitMetric: memLimitMetric,
		targetMemPSI:   targetMemPSI,
		targetIOPSI:    targetIOPSI,
		minRefaultRate: minRefaultRate,
		stepUpFactor:   stepUpFactor,
		cooldown:       cooldown,
	}, nil
}
