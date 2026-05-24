// Round-robin scheduling: walk the candidate list in a fixed (ID-sorted) order
// and hand out the next server on every Select call. The cursor is shared
// across all tasks, so the distribution stays even even when candidate sets
// vary tick to tick. Used as the default policy and as a baseline in
// experiments; it ignores task affinity and server load entirely.
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

// NewRoundRobin builds a RoundRobin scheduler with its cursor at zero.
func NewRoundRobin() *RoundRobin { return &RoundRobin{} }

// Name returns the algorithm identifier used in config.yaml and logs.
func (rr *RoundRobin) Name() string { return "round_robin" }

// Select returns the next candidate in ID-sorted order and advances the cursor.
// Returns nil if candidates is empty. The task argument is unused.
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
