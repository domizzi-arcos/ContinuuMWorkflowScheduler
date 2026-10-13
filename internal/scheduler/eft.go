package scheduler

import "github.com/pcturnes/distributed-workflow-scheduler/internal/models"

// EFT is an online earliest-finish-time greedy policy: the server-selection
// step of HEFT applied to each task as it becomes ready. It is not HEFT:
// tasks arrive online, so HEFT's upward-rank ordering phase does not apply.
//
// Over the feasible candidates it picks the server minimising the estimated
// finish time d/s+ℓ (task duration over server speed, plus server latency,
// ms). A feasible server starts the task immediately, so with all speeds 1
// (the default) this is the lowest-latency server. Ties (within tieTolerance) are broken by
// lower post-placement utilisation (worst of CPU, memory, disk), then lower
// server ID. The candidate slice is not modified.
type EFT struct{}

// NewEFT builds a stateless EFT policy.
func NewEFT() *EFT { return &EFT{} }

// Name returns the identifier used in configs and results.
func (EFT) Name() string { return "eft" }

// Select returns the earliest-finishing candidate, or nil when there are none.
func (EFT) Select(task *models.Task, candidates []*models.Server) *models.Server {
	if len(candidates) <= 1 {
		return single(candidates)
	}
	sorted := sortedByID(candidates)
	finish, _, _, util := computeCriteria(task, sorted)
	best := 0
	for i := 1; i < len(sorted); i++ {
		if lexLess([]float64{finish[i], util[i]}, []float64{finish[best], util[best]}) {
			best = i
		}
	}
	return sorted[best]
}
