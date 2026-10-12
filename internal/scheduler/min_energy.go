package scheduler

import "github.com/pcturnes/distributed-workflow-scheduler/internal/models"

// MinEnergy is the simplest energy-aware greedy policy. Over the feasible
// candidates it picks the server with the lowest estimated energy power ×
// (d+ℓ) / 3.6e6 (Wh, d = task duration, ℓ = server latency, ms). Ties
// (within tieTolerance) are broken by lower estimated cost, then lower
// latency, then lower server ID. The candidate slice is not modified.
type MinEnergy struct{}

// NewMinEnergy builds a stateless MinEnergy policy.
func NewMinEnergy() *MinEnergy { return &MinEnergy{} }

// Name returns the identifier used in configs and results.
func (MinEnergy) Name() string { return "min_energy" }

// Select returns the lowest-energy candidate, or nil when there are none.
func (MinEnergy) Select(task *models.Task, candidates []*models.Server) *models.Server {
	if len(candidates) <= 1 {
		return single(candidates)
	}
	sorted := sortedByID(candidates)
	_, cost, energy, _ := computeCriteria(task, sorted)
	best := 0
	for i := 1; i < len(sorted); i++ {
		if lexLess(
			[]float64{energy[i], cost[i], float64(sorted[i].Latency)},
			[]float64{energy[best], cost[best], float64(sorted[best].Latency)},
		) {
			best = i
		}
	}
	return sorted[best]
}
