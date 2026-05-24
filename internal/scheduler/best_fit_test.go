package scheduler

import (
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// TestBestFitName pins the algorithm identifier used in config.yaml.
func TestBestFitName(t *testing.T) {
	if NewBestFit().Name() != "best_fit" {
		t.Fatal("unexpected name")
	}
}

// TestBestFitSelectNilOnEmpty asserts Select returns nil when there are no
// candidates, so the dispatcher can branch safely on a nil return.
func TestBestFitSelectNilOnEmpty(t *testing.T) {
	if NewBestFit().Select(nil, nil) != nil {
		t.Fatal("expected nil for empty candidates")
	}
	if NewBestFit().Select(nil, []*models.Server{}) != nil {
		t.Fatal("expected nil for empty slice")
	}
}

// TestBestFitSelectSingleCandidate guards the degenerate case where only one
// server is eligible — Select must return it unchanged.
func TestBestFitSelectSingleCandidate(t *testing.T) {
	srv := &models.Server{ID: "only", MaxCPU: 4, MaxMemory: 512, MaxDisk: 5}
	if NewBestFit().Select(nil, []*models.Server{srv}) != srv {
		t.Fatal("expected single candidate returned unchanged")
	}
}

// TestBestFitSelectPicksMostPacked exercises the tight-fit invariant across
// candidate orderings and load profiles: BestFit must always return the
// server with the smallest FreeCapacity, regardless of slice order.
func TestBestFitSelectPicksMostPacked(t *testing.T) {
	// FreeCapacity = (MaxCPU-UsedCPU) + (MaxMemory-UsedMemory)/100 + (MaxDisk-UsedDisk)
	// a: 1 + 1 + 1 = 3   (most packed, BestFit should choose this)
	// b: 2 + 5 + 5 = 12
	// c: 4 + 10 + 10 = 24 (least packed)
	tests := []struct {
		name       string
		candidates []*models.Server
		wantID     string
	}{
		{
			name: "picks most packed when first in slice",
			candidates: []*models.Server{
				{ID: "a", MaxCPU: 4, UsedCPU: 3, MaxMemory: 1000, UsedMemory: 900, MaxDisk: 10, UsedDisk: 9},
				{ID: "b", MaxCPU: 4, UsedCPU: 2, MaxMemory: 1000, UsedMemory: 500, MaxDisk: 10, UsedDisk: 5},
				{ID: "c", MaxCPU: 4, UsedCPU: 0, MaxMemory: 1000, UsedMemory: 0, MaxDisk: 10, UsedDisk: 0},
			},
			wantID: "a",
		},
		{
			name: "picks most packed when last in slice",
			candidates: []*models.Server{
				{ID: "c", MaxCPU: 4, UsedCPU: 0, MaxMemory: 1000, UsedMemory: 0, MaxDisk: 10, UsedDisk: 0},
				{ID: "b", MaxCPU: 4, UsedCPU: 2, MaxMemory: 1000, UsedMemory: 500, MaxDisk: 10, UsedDisk: 5},
				{ID: "a", MaxCPU: 4, UsedCPU: 3, MaxMemory: 1000, UsedMemory: 900, MaxDisk: 10, UsedDisk: 9},
			},
			wantID: "a",
		},
		{
			name: "two candidates — picks tighter one",
			candidates: []*models.Server{
				{ID: "loose", MaxCPU: 8, UsedCPU: 0, MaxMemory: 4096, UsedMemory: 0, MaxDisk: 20, UsedDisk: 0},
				{ID: "tight", MaxCPU: 8, UsedCPU: 7, MaxMemory: 4096, UsedMemory: 3996, MaxDisk: 20, UsedDisk: 19},
			},
			wantID: "tight",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NewBestFit().Select(nil, tc.candidates)
			if got == nil {
				t.Fatal("got nil, want server")
			}
			if got.ID != tc.wantID {
				t.Fatalf("got %s, want %s", got.ID, tc.wantID)
			}
		})
	}
}
