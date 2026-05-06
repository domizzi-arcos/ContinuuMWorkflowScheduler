package scheduler

import (
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

func TestTOPSISName(t *testing.T) {
	if NewTOPSIS(1, 1, 1).Name() != "topsis" {
		t.Fatal("unexpected name")
	}
}

func TestTOPSISSelectNilOnEmpty(t *testing.T) {
	tp := NewTOPSIS(1, 1, 1)
	if tp.Select(nil, nil) != nil {
		t.Fatal("expected nil for nil candidates")
	}
	if tp.Select(nil, []*models.Server{}) != nil {
		t.Fatal("expected nil for empty slice")
	}
}

func TestTOPSISSelectSingleCandidateReturnedDirectly(t *testing.T) {
	srv := &models.Server{ID: "only", Latency: 5, CostPerHour: 1.0, PowerWatts: 50}
	task := &models.Task{Duration: 1000}
	tp := NewTOPSIS(1, 1, 1)
	got := tp.Select(task, []*models.Server{srv})
	if got != srv {
		t.Fatal("expected single candidate returned unchanged")
	}
}

func TestTOPSISSelectPicksDominantServer(t *testing.T) {
	// Server "good" is strictly better on all three TOPSIS criteria
	// (lower execution time, lower cost, lower energy) — TOPSIS must pick it.
	task := &models.Task{Duration: 2000}
	tests := []struct {
		name    string
		servers []*models.Server
		wantID  string
	}{
		{
			name: "good dominates bad on all criteria",
			servers: []*models.Server{
				{ID: "good", Latency: 10, CostPerHour: 0.5, PowerWatts: 100},
				{ID: "bad", Latency: 500, CostPerHour: 5.0, PowerWatts: 1000},
			},
			wantID: "good",
		},
		{
			name: "three candidates — lowest latency+cost+power wins",
			servers: []*models.Server{
				{ID: "mid", Latency: 50, CostPerHour: 1.0, PowerWatts: 200},
				{ID: "best", Latency: 5, CostPerHour: 0.1, PowerWatts: 50},
				{ID: "worst", Latency: 300, CostPerHour: 8.0, PowerWatts: 800},
			},
			wantID: "best",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tp := NewTOPSIS(1, 1, 1)
			got := tp.Select(task, tc.servers)
			if got == nil {
				t.Fatal("got nil, want server")
			}
			if got.ID != tc.wantID {
				t.Fatalf("got %s, want %s", got.ID, tc.wantID)
			}
		})
	}
}

func TestTOPSISWeightNormalisationWithZeroTotal(t *testing.T) {
	// Zero-weight constructor should fall back to equal weights (1/3, 1/3, 1/3).
	task := &models.Task{Duration: 1000}
	servers := []*models.Server{
		{ID: "good", Latency: 10, CostPerHour: 0.5, PowerWatts: 100},
		{ID: "bad", Latency: 500, CostPerHour: 5.0, PowerWatts: 1000},
	}
	tp := NewTOPSIS(0, 0, 0)
	got := tp.Select(task, servers)
	if got == nil || got.ID != "good" {
		t.Fatalf("expected good with equal weights after zero fallback, got %v", got)
	}
}

func TestTOPSISWeightBiasTowardsLatency(t *testing.T) {
	// When time weight is dominant, the server with the lowest latency wins
	// even if its cost is slightly higher.
	task := &models.Task{Duration: 1000}
	servers := []*models.Server{
		{ID: "fast_expensive", Latency: 5, CostPerHour: 10.0, PowerWatts: 200},
		{ID: "slow_cheap", Latency: 200, CostPerHour: 0.1, PowerWatts: 50},
	}
	tp := NewTOPSIS(10, 1, 1) // time weight dominates
	got := tp.Select(task, servers)
	if got == nil || got.ID != "fast_expensive" {
		t.Fatalf("expected fast_expensive when time weight dominates, got %v", got)
	}
}
