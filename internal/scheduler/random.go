// Random scheduling: pick uniformly from the capable candidates. Under load
// this spreads tasks evenly in expectation (same as round-robin in the limit)
// but provides no smoothing on bursts; it is included mainly as a sanity
// baseline for comparing the cost/energy-aware policies.
package scheduler

import (
	"math/rand"
	"sync"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// Random picks a capable server uniformly at random using a seeded RNG
// so results are reproducible given the same seed in config.yaml.
type Random struct {
	rng *rand.Rand
	mu  sync.Mutex
}

// NewRandom seeds a Random scheduler. Pass a fixed seed for reproducible
// experiment runs, or 0 from the caller for a wall-clock seed.
func NewRandom(seed int64) *Random {
	return &Random{rng: rand.New(rand.NewSource(seed))}
}

// Name returns the algorithm identifier used in config.yaml and logs.
func (r *Random) Name() string { return "random" }

// Select returns one candidate chosen uniformly at random. Returns nil if
// candidates is empty. The task argument is unused.
func (r *Random) Select(_ *models.Task, candidates []*models.Server) *models.Server {
	if len(candidates) == 0 {
		return nil
	}
	r.mu.Lock()
	idx := r.rng.Intn(len(candidates))
	r.mu.Unlock()
	return candidates[idx]
}
