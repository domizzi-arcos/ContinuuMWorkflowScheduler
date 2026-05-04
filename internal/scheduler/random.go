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

func NewRandom(seed int64) *Random {
	return &Random{rng: rand.New(rand.NewSource(seed))}
}

func (r *Random) Name() string { return "random" }

func (r *Random) Select(_ *models.Task, candidates []*models.Server) *models.Server {
	if len(candidates) == 0 {
		return nil
	}
	r.mu.Lock()
	idx := r.rng.Intn(len(candidates))
	r.mu.Unlock()
	return candidates[idx]
}
