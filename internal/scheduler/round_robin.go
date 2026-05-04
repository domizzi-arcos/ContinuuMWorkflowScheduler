package scheduler

import (
	"sort"
	"sync"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// RoundRobin cycles through candidate servers in sorted order.
// The counter is global across all tasks so every assignment advances the cursor.
type RoundRobin struct {
	mu      sync.Mutex
	counter int
}

func NewRoundRobin() *RoundRobin { return &RoundRobin{} }

func (rr *RoundRobin) Name() string { return "round_robin" }

func (rr *RoundRobin) Select(_ *models.Task, candidates []*models.Server) *models.Server {
	if len(candidates) == 0 {
		return nil
	}
	// Deterministic order so the rotation is reproducible
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ID < candidates[j].ID
	})
	rr.mu.Lock()
	idx := rr.counter % len(candidates)
	rr.counter++
	rr.mu.Unlock()
	return candidates[idx]
}
