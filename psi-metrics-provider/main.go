package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	clientset "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/clientset/versioned"
	informers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/informers/externalversions"
)

func main() {
	var serverAddress string
	var nodeName string
	var clusterName string
	var cgroupRoot string
	var kubeconfig string
	var scrapeInterval time.Duration
	var debug bool

	flag.StringVar(&serverAddress, "server-address", "xas-server:8080", "Address of the XAS Server (host:port)")
	flag.StringVar(&nodeName, "node-name", "", "Name of the node this provider is running on (for DaemonSet mode)")
	flag.StringVar(&clusterName, "cluster-name", "default", "Name of the cluster this provider is running in")
	flag.StringVar(&cgroupRoot, "cgroup-root", "/sys/fs/cgroup", "Host cgroup v2 mount path")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.DurationVar(&scrapeInterval, "scrape-interval", 5*time.Second, "Scrape interval")
	flag.BoolVar(&debug, "debug", false, "Enable debug logging")
	flag.Parse()

	logLevel := slog.LevelInfo
	if debug {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var cfg *rest.Config
	var err error
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		slog.Error("Error building kubeconfig", "error", err)
		os.Exit(1)
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		slog.Error("Error building kubernetes clientset", "error", err)
		os.Exit(1)
	}

	xasClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		slog.Error("Error building xas clientset", "error", err)
		os.Exit(1)
	}

	factory := informers.NewSharedInformerFactory(xasClient, time.Second*30)
	providerLister := factory.Xas().V1().MetricProviderClasses().Lister()

	p := NewPSINodeMetricsProvider(kubeClient, providerLister, serverAddress, nodeName, clusterName, cgroupRoot, scrapeInterval)

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	slog.Info("XAS PSI Node Metrics Provider starting...")
	p.Run(ctx)
}
