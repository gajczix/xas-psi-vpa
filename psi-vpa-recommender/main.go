package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	clientset "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/clientset/versioned"
	informers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/informers/externalversions"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

func main() {
	serverAddress := flag.String("server-address", "localhost:8080", "XAS Server Address (host:port)")
	clusterName := flag.String("cluster-name", "default", "Name of the cluster")
	kubeconfig := flag.String("kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	evalInterval := flag.Duration("eval-interval", 5*time.Second, "Evaluation interval")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	logLevel := slog.LevelInfo
	if *debug {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))

	slog.Info("Starting PSI-VPA Recommender", "address", *serverAddress, "cluster", *clusterName)

	var cfg *rest.Config
	var err error
	if *kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		slog.Error("Error building kubeconfig", "error", err)
		os.Exit(1)
	}

	xasClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		slog.Error("Error building xas clientset", "error", err)
		os.Exit(1)
	}

	factory := informers.NewSharedInformerFactory(xasClient, time.Second*30)
	recommenderLister := factory.Xas().V1().RecommenderClasses().Lister()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	runEngine(ctx, recommenderLister, *serverAddress, *clusterName, *evalInterval)
}

func runEngine(ctx context.Context, recommenderClassLister listers.RecommenderClassLister, serverAddress, clusterName string, evalInterval time.Duration) {
	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("did not connect", "error", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewXASServerClient(conn)

	vpa := &PSIVPARecommender{}
	ticker := time.NewTicker(evalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tick(client, recommenderClassLister, vpa, clusterName)
		case <-ctx.Done():
			return
		}
	}
}

func tick(client pb.XASServerClient, lister listers.RecommenderClassLister, vpa *PSIVPARecommender, clusterName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ListPolicies(ctx, &pb.ListPoliciesRequest{ClusterName: clusterName})
	if err != nil {
		slog.Error("Error fetching policies", "error", err)
		return
	}

	for _, policy := range resp.Policies {
		metrics, err := client.GetControlMetrics(ctx, &pb.GetControlMetricsRequest{
			Id: &pb.PolicyId{ClusterName: clusterName, Namespace: policy.Id.Namespace, Name: policy.Id.Name},
		})
		if err != nil {
			continue
		}

		for _, def := range slices.Concat(policy.Activation, policy.Scaling) {
			class, err := lister.Get(def.Recommender)
			if err != nil || class.Spec.Type != "PSIVPA" {
				continue
			}

			rec := vpa.Recommend(def, metrics, nil)
			if rec == nil {
				continue
			}

			_, err = client.UpdateRecommenderState(ctx, &pb.UpdateRecommenderStateRequest{
				Id:              &pb.PolicyId{ClusterName: clusterName, Namespace: policy.Id.Namespace, Name: policy.Id.Name},
				RecommenderName: def.Name,
				Recommendation:  rec,
			})
			if err != nil {
				slog.Error("Failed to push recommendation", "policy", policy.Id.Name, "error", err)
			}
		}
	}
}
