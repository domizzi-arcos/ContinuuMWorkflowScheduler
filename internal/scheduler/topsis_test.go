package scheduler

import (
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// TestTOPSISName pins the algorithm identifier used in config.yaml.
func TestTOPSISName(t *testing.T) {
	if NewTOPSIS(1, 1, 1, 0).Name() != "topsis" {
		t.Fatal("unexpected name")
	}
}

// TestTOPSISSelectNilOnEmpty guards the empty-candidate path so callers can
// rely on a nil return rather than a division-by-zero in the normalisation step.
func TestTOPSISSelectNilOnEmpty(t *testing.T) {
	tp := NewTOPSIS(1, 1, 1, 0)
	if tp.Select(nil, nil) != nil {
		t.Fatal("expected nil for nil candidates")
	}
	if tp.Select(nil, []*models.Server{}) != nil {
		t.Fatal("expected nil for empty slice")
	}
}

// TestTOPSISSelectSingleCandidateReturnedDirectly covers the degenerate case
// where the candidate list has one server — there is nothing to compare, so
// Select must skip the normalisation step and return it directly.
func TestTOPSISSelectSingleCandidateReturnedDirectly(t *testing.T) {
	srv := &models.Server{ID: "only", Latency: 5, CostPerHour: 1.0, PowerWatts: 50}
	task := &models.Task{Duration: 1000}
	tp := NewTOPSIS(1, 1, 1, 0)
	got := tp.Select(task, []*models.Server{srv})
	if got != srv {
		t.Fatal("expected single candidate returned unchanged")
	}
}

// TestTOPSISSelectPicksDominantServer asserts the basic monotonicity of
// TOPSIS: when one candidate is strictly better on every weighted criterion
// (and the util weight is disabled), it must win regardless of slice order.
func TestTOPSISSelectPicksDominantServer(t *testing.T) {
	// "good" is strictly better on time/cost/energy; util weight is 0 so it
	// should still be picked regardless of its capacity.
	task := &models.Task{Duration: 2000}
	tests := []struct {
		name    string
		servers []*models.Server
		wantID  string
	}{
		{
			name: "good dominates bad on all criteria",
			servers: []*models.Server{
				{ID: "good", Latency: 10, CostPerHour: 0.5, PowerWatts: 100, MaxCPU: 4, MaxMemory: 4096, MaxDisk: 100},
				{ID: "bad", Latency: 500, CostPerHour: 5.0, PowerWatts: 1000, MaxCPU: 4, MaxMemory: 4096, MaxDisk: 100},
			},
			wantID: "good",
		},
		{
			name: "three candidates — lowest latency+cost+power wins",
			servers: []*models.Server{
				{ID: "mid", Latency: 50, CostPerHour: 1.0, PowerWatts: 200, MaxCPU: 4, MaxMemory: 4096, MaxDisk: 100},
				{ID: "best", Latency: 5, CostPerHour: 0.1, PowerWatts: 50, MaxCPU: 4, MaxMemory: 4096, MaxDisk: 100},
				{ID: "worst", Latency: 300, CostPerHour: 8.0, PowerWatts: 800, MaxCPU: 4, MaxMemory: 4096, MaxDisk: 100},
			},
			wantID: "best",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tp := NewTOPSIS(1, 1, 1, 0)
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

// TestTOPSISWeightNormalisationWithZeroTotal pins the zero-weight fallback:
// when every weight is zero, NewTOPSIS substitutes equal weights so the
// algorithm still discriminates between candidates instead of dividing by zero.
func TestTOPSISWeightNormalisationWithZeroTotal(t *testing.T) {
	// Zero-weight constructor should fall back to equal weights (1/4 each).
	task := &models.Task{Duration: 1000}
	servers := []*models.Server{
		{ID: "good", Latency: 10, CostPerHour: 0.5, PowerWatts: 100, MaxCPU: 4, MaxMemory: 4096, MaxDisk: 100},
		{ID: "bad", Latency: 500, CostPerHour: 5.0, PowerWatts: 1000, MaxCPU: 4, MaxMemory: 4096, MaxDisk: 100},
	}
	tp := NewTOPSIS(0, 0, 0, 0)
	got := tp.Select(task, servers)
	if got == nil || got.ID != "good" {
		t.Fatalf("expected good with equal weights after zero fallback, got %v", got)
	}
}

// TestTOPSISWeightBiasTowardsLatency confirms that a dominant time weight
// can flip the decision: a high-latency server cannot win even if its cost
// is dramatically lower. Exercises the time-dominant TOPSIS variant used in
// the thesis weight-sensitivity grid.
func TestTOPSISWeightBiasTowardsLatency(t *testing.T) {
	// When time weight is dominant, the server with the lowest latency wins
	// even if its cost is slightly higher.
	task := &models.Task{Duration: 1000}
	servers := []*models.Server{
		{ID: "fast_expensive", Latency: 5, CostPerHour: 10.0, PowerWatts: 200, MaxCPU: 4, MaxMemory: 4096, MaxDisk: 100},
		{ID: "slow_cheap", Latency: 200, CostPerHour: 0.1, PowerWatts: 50, MaxCPU: 4, MaxMemory: 4096, MaxDisk: 100},
	}
	tp := NewTOPSIS(10, 1, 1, 0)
	got := tp.Select(task, servers)
	if got == nil || got.ID != "fast_expensive" {
		t.Fatalf("expected fast_expensive when time weight dominates, got %v", got)
	}
}

// TestTOPSISUtilSidestepsSaturatedServer locks in the saturation fix
// described in §7.3 of the thesis. With the utilisation weight enabled,
// TOPSIS must avoid a server whose live resource counters show it is
// fully booked, even when its static cost/energy/time are identical to a
// free peer. Without this signal the previous implementation would keep
// piling tasks onto the same saturated edge node until its queue blocked.
func TestTOPSISUtilSidestepsSaturatedServer(t *testing.T) {
	// Two servers identical on cost/energy/time. "cheap_full" is fully booked
	// (used == max); "cheap_free" is empty. With wUtil > 0 the empty one must win.
	// This reproduces the saturation pathology that §7.3 of the thesis report
	// blamed for TOPSIS underperforming in tight-edge configs.
	task := &models.Task{Duration: 1000, CPU: 1, Memory: 128, Disk: 1}
	servers := []*models.Server{
		{
			ID: "cheap_full", Latency: 5, CostPerHour: 0.02, PowerWatts: 15,
			MaxCPU: 1, MaxMemory: 128, MaxDisk: 10,
			UsedCPU: 1, UsedMemory: 128, UsedDisk: 10, // saturated
		},
		{
			ID: "cheap_free", Latency: 5, CostPerHour: 0.02, PowerWatts: 15,
			MaxCPU: 1, MaxMemory: 128, MaxDisk: 10,
			UsedCPU: 0, UsedMemory: 0, UsedDisk: 0, // free
		},
	}
	// Without util weight the two would tie on TOPSIS score; with util the
	// less-utilised one must win.
	tp := NewTOPSIS(0.3, 0.225, 0.225, 0.25)
	got := tp.Select(task, servers)
	if got == nil || got.ID != "cheap_free" {
		t.Fatalf("util-aware TOPSIS must pick the unsaturated server, got %v", got)
	}
}

// TestTOPSISUtilDisabledWhenWeightZero documents the pre-fix behaviour kept
// available via wUtil=0: cost/energy/time are tied, no util signal exists,
// so the first-best tie-break wins. Setting wUtil=0 in config.yaml is the
// way to reproduce baseline-TOPSIS results for fair comparison.
func TestTOPSISUtilDisabledWhenWeightZero(t *testing.T) {
	// Same setup as TestTOPSISUtilSidestepsSaturatedServer but wUtil=0 —
	// the algorithm must reproduce the pre-fix behaviour: cost/energy/time
	// are tied, so the tie-break falls to first-best (deterministic order).
	task := &models.Task{Duration: 1000, CPU: 1, Memory: 128, Disk: 1}
	servers := []*models.Server{
		{
			ID: "cheap_full", Latency: 5, CostPerHour: 0.02, PowerWatts: 15,
			MaxCPU: 1, MaxMemory: 128, MaxDisk: 10,
			UsedCPU: 1, UsedMemory: 128, UsedDisk: 10,
		},
		{
			ID: "cheap_free", Latency: 5, CostPerHour: 0.02, PowerWatts: 15,
			MaxCPU: 1, MaxMemory: 128, MaxDisk: 10,
		},
	}
	tp := NewTOPSIS(1, 1, 1, 0)
	got := tp.Select(task, servers)
	if got == nil {
		t.Fatal("got nil")
	}
	// With wUtil=0 there is no signal to disambiguate; the tie-break is
	// deterministic lexicographic order by server ID (so two reseeded runs
	// produce identical placements even with no util signal).
	if got.ID != "cheap_free" {
		t.Fatalf("with util disabled, expected lexicographic tie-break = cheap_free, got %v", got)
	}
}
