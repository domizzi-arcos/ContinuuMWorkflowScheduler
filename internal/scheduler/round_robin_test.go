package scheduler

import (
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// TestRoundRobinName pins the algorithm identifier used in config.yaml.
func TestRoundRobinName(t *testing.T) {
	if NewRoundRobin().Name() != "round_robin" {
		t.Fatal("unexpected name")
	}
}

// TestRoundRobinSelectNilOnEmpty guards the empty-candidate branch.
func TestRoundRobinSelectNilOnEmpty(t *testing.T) {
	if NewRoundRobin().Select(nil, nil) != nil {
		t.Fatal("expected nil for empty candidates")
	}
	if NewRoundRobin().Select(nil, []*models.Server{}) != nil {
		t.Fatal("expected nil for empty slice")
	}
}

// TestRoundRobinSelectCyclesInSortedOrder pins that successive Select calls
// rotate through candidates in deterministic ID-sorted order so the rotation
// is reproducible regardless of how the caller orders the slice.
func TestRoundRobinSelectCyclesInSortedOrder(t *testing.T) {
	// Provide servers in unsorted order; RoundRobin must sort by ID before cycling.
	servers := []*models.Server{
		{ID: "srv-c"},
		{ID: "srv-a"},
		{ID: "srv-b"},
	}
	// Sorted order: srv-a, srv-b, srv-c
	want := []string{"srv-a", "srv-b", "srv-c", "srv-a", "srv-b", "srv-c"}

	rr := NewRoundRobin()
	for i, wantID := range want {
		got := rr.Select(nil, servers)
		if got == nil {
			t.Fatalf("call %d: got nil", i)
		}
		if got.ID != wantID {
			t.Fatalf("call %d: got %s, want %s", i, got.ID, wantID)
		}
	}
}

// TestRoundRobinSelectSingleCandidateAlwaysReturnsIt covers the degenerate
// single-server case — there's nothing to rotate, so Select must return it
// every call.
func TestRoundRobinSelectSingleCandidateAlwaysReturnsIt(t *testing.T) {
	srv := &models.Server{ID: "solo"}
	rr := NewRoundRobin()
	for i := 0; i < 5; i++ {
		got := rr.Select(nil, []*models.Server{srv})
		if got != srv {
			t.Fatalf("call %d: expected solo server", i)
		}
	}
}

// TestRoundRobinCounterPersistsAcrossCalls verifies the rotation counter is
// instance-scoped, not call-scoped — two consecutive Selects must return
// different servers.
func TestRoundRobinCounterPersistsAcrossCalls(t *testing.T) {
	// Counter must advance globally, not reset per call.
	s1 := &models.Server{ID: "s1"}
	s2 := &models.Server{ID: "s2"}
	rr := NewRoundRobin()

	first := rr.Select(nil, []*models.Server{s1, s2})
	second := rr.Select(nil, []*models.Server{s1, s2})
	if first.ID == second.ID {
		t.Fatalf("counter did not advance: both calls returned %s", first.ID)
	}
}
