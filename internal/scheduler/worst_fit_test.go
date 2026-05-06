package scheduler

import (
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

func TestWorstFitName(t *testing.T) {
	if NewWorstFit().Name() != "worst_fit" {
		t.Fatal("unexpected name")
	}
}

func TestWorstFitSelectNilOnEmpty(t *testing.T) {
	if NewWorstFit().Select(nil, nil) != nil {
		t.Fatal("expected nil for empty candidates")
	}
	if NewWorstFit().Select(nil, []*models.Server{}) != nil {
		t.Fatal("expected nil for empty slice")
	}
}

func TestWorstFitSelectSingleCandidate(t *testing.T) {
	srv := &models.Server{ID: "only", MaxCPU: 4, MaxMemory: 512, MaxDisk: 5}
	if NewWorstFit().Select(nil, []*models.Server{srv}) != srv {
		t.Fatal("expected single candidate returned unchanged")
	}
}

func TestWorstFitSelectPicksLeastPacked(t *testing.T) {
	// FreeCapacity = (MaxCPU-UsedCPU) + (MaxMemory-UsedMemory)/100 + (MaxDisk-UsedDisk)
	// a: 1 + 1 + 1 = 3   (most packed)
	// b: 2 + 5 + 5 = 12
	// c: 4 + 10 + 10 = 24 (least packed, WorstFit should choose this)
	tests := []struct {
		name       string
		candidates []*models.Server
		wantID     string
	}{
		{
			name: "picks least packed when last in slice",
			candidates: []*models.Server{
				{ID: "a", MaxCPU: 4, UsedCPU: 3, MaxMemory: 1000, UsedMemory: 900, MaxDisk: 10, UsedDisk: 9},
				{ID: "b", MaxCPU: 4, UsedCPU: 2, MaxMemory: 1000, UsedMemory: 500, MaxDisk: 10, UsedDisk: 5},
				{ID: "c", MaxCPU: 4, UsedCPU: 0, MaxMemory: 1000, UsedMemory: 0, MaxDisk: 10, UsedDisk: 0},
			},
			wantID: "c",
		},
		{
			name: "picks least packed when first in slice",
			candidates: []*models.Server{
				{ID: "c", MaxCPU: 4, UsedCPU: 0, MaxMemory: 1000, UsedMemory: 0, MaxDisk: 10, UsedDisk: 0},
				{ID: "b", MaxCPU: 4, UsedCPU: 2, MaxMemory: 1000, UsedMemory: 500, MaxDisk: 10, UsedDisk: 5},
				{ID: "a", MaxCPU: 4, UsedCPU: 3, MaxMemory: 1000, UsedMemory: 900, MaxDisk: 10, UsedDisk: 9},
			},
			wantID: "c",
		},
		{
			name: "two candidates — picks roomier one",
			candidates: []*models.Server{
				{ID: "tight", MaxCPU: 8, UsedCPU: 7, MaxMemory: 4096, UsedMemory: 3996, MaxDisk: 20, UsedDisk: 19},
				{ID: "loose", MaxCPU: 8, UsedCPU: 0, MaxMemory: 4096, UsedMemory: 0, MaxDisk: 20, UsedDisk: 0},
			},
			wantID: "loose",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NewWorstFit().Select(nil, tc.candidates)
			if got == nil {
				t.Fatal("got nil, want server")
			}
			if got.ID != tc.wantID {
				t.Fatalf("got %s, want %s", got.ID, tc.wantID)
			}
		})
	}
}
