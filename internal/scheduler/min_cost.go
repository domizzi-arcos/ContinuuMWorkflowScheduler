package scheduler

import "github.com/pcturnes/distributed-workflow-scheduler/internal/models"

// MinCost is the simplest price-aware greedy policy. Over the feasible
// candidates it picks the server with the lowest estimated cost price ×
// (d+ℓ) / 3.6e6 (USD, d = task duration, ℓ = server latency, ms). Ties
// (within tieTolerance) are broken by lower estimated energy, then lower
// latency, then lower server ID. The candidate slice is not modified.
type MinCost struct{}

// NewMinCost builds a stateless MinCost policy.
func NewMinCost() *MinCost { return &MinCost{} }

// Name returns the identifier used in configs and results.
func (MinCost) Name() string { return "min_cost" }

// Select returns the cheapest candidate, or nil when there are none.
func (MinCost) Select(task *models.Task, candidates []*models.Server) *models.Server {
	if len(candidates) <= 1 {
		return single(candidates)
	}
	sorted := sortedByID(candidates)
	_, cost, energy, _ := computeCriteria(task, sorted)
	best := 0
	for i := 1; i < len(sorted); i++ {
		if lexLess(
			[]float64{cost[i], energy[i], float64(sorted[i].Latency)},
			[]float64{cost[best], energy[best], float64(sorted[best].Latency)},
		) {
			best = i
		}
	}
	return sorted[best]
}
