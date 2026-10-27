package main

import (
	"testing"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

func TestRecommendSlide27Branches(t *testing.T) {
	recEngine := &PSIVPARecommender{}
	def := &pb.RecommenderDefinition{
		Name:        "psi_memory_rescue",
		Recommender: "psi-vpa",
		Params: map[string]string{
			"container":         "app",
			"memoryPSIMetric":   "memory_psi",
			"ioPSIMetric":       "io_psi",
			"refaultRateMetric": "refault_rate",
			"memoryLimitMetric": "current_memory_limit",
			"targetMemPSI":      "10.0",
			"targetIOPSI":       "15.0",
			"minRefaultRate":    "100.0",
			"stepUpFactor":      "0.15",
			"cooldown":          "0s",
		},
	}

	const mb256 = float64(256 * 1024 * 1024)

	t.Run("Branch A: Disguised Memory Starvation (High IO PSI + High Refaults -> Grow +15%)", func(t *testing.T) {
		state := &pb.ControlMetrics{
			PodContainerMetrics: map[string]*pb.ContainerMetrics{
				"thrashing-pod": {
					ContainerMetrics: map[string]*pb.MetricValues{
						"app": {
							Values: map[string]float64{
								"memory_psi":           1.3,
								"io_psi":               67.6,
								"refault_rate":         42000,
								"current_memory_limit": mb256,
							},
						},
					},
				},
			},
		}

		rec := recEngine.Recommend(def, state, nil)
		if !rec.IsActive || len(rec.WorkloadResources) != 1 {
			t.Fatalf("expected active recommendation, got %+v", rec)
		}
		got := rec.WorkloadResources[0].Limits["memory"]
		if got != "295Mi" {
			t.Errorf("expected 295Mi, got %s", got)
		}
	})

	t.Run("Branch B: Storage Bottleneck (High IO PSI + Low Refaults -> Hold Memory)", func(t *testing.T) {
		state := &pb.ControlMetrics{
			PodContainerMetrics: map[string]*pb.ContainerMetrics{
				"disk-bound-pod": {
					ContainerMetrics: map[string]*pb.MetricValues{
						"app": {
							Values: map[string]float64{
								"memory_psi":           0.5,
								"io_psi":               55.0,
								"refault_rate":         2.0,
								"current_memory_limit": mb256,
							},
						},
					},
				},
			},
		}

		rec := recEngine.Recommend(def, state, nil)
		got := rec.WorkloadResources[0].Limits["memory"]
		if got != "256Mi" {
			t.Errorf("expected 256Mi, got %s", got)
		}
	})

	t.Run("Branch C: Pure Memory Pressure (Low IO PSI + High Memory PSI -> Grow +15%)", func(t *testing.T) {
		state := &pb.ControlMetrics{
			PodContainerMetrics: map[string]*pb.ContainerMetrics{
				"zram-pod": {
					ContainerMetrics: map[string]*pb.MetricValues{
						"app": {
							Values: map[string]float64{
								"memory_psi":           43.0,
								"io_psi":               0.0,
								"refault_rate":         0.0,
								"current_memory_limit": mb256,
							},
						},
					},
				},
			},
		}

		rec := recEngine.Recommend(def, state, nil)
		got := rec.WorkloadResources[0].Limits["memory"]
		if got != "295Mi" {
			t.Errorf("expected 295Mi, got %s", got)
		}
	})

	t.Run("Branch D: Healthy Pod (Low Memory PSI + Low IO PSI -> Hold Memory)", func(t *testing.T) {
		state := &pb.ControlMetrics{
			PodContainerMetrics: map[string]*pb.ContainerMetrics{
				"healthy-pod": {
					ContainerMetrics: map[string]*pb.MetricValues{
						"app": {
							Values: map[string]float64{
								"memory_psi":           0.0,
								"io_psi":               0.1,
								"refault_rate":         0.0,
								"current_memory_limit": mb256,
							},
						},
					},
				},
			},
		}

		rec := recEngine.Recommend(def, state, nil)
		got := rec.WorkloadResources[0].Limits["memory"]
		if got != "256Mi" {
			t.Errorf("expected 256Mi, got %s", got)
		}
	})
}
