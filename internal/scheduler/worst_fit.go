// Worst-fit scheduling: place the task on the candidate with the MOST free
// capacity. Spreads load across servers, avoiding hot spots at the cost of
// fragmentation — large servers may end up underused, and large jobs that
// arrive later can find themselves with no room.
package scheduler

import (
	"sort"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// WorstFit picks the candidate with the most remaining free capacity —
// spreading load across servers to avoid hot spots.
type WorstFit struct{}

// NewWorstFit builds a stateless WorstFit scheduler.
func NewWorstFit() *WorstFit { return &WorstFit{} }

// Name returns the algorithm identifier used in config.yaml and logs.
func (w *WorstFit) Name() string { return "worst_fit" }

// Select returns the candidate with the largest FreeCapacity. Returns nil
// if candidates is empty. The task argument is unused — the caller has
// already filtered for capacity.
func (w *WorstFit) Select(_ *models.Task, candidates []*models.Server) *models.Server {
	// Sort by ID first so ties break deterministically — Redis SCAN order is
	// not stable, and a thesis claiming reproducibility cannot inherit that.
	sorted := make([]*models.Server, len(candidates))
	copy(sorted, candidates)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	var best *models.Server
	for _, srv := range sorted {
		if best == nil || srv.FreeCapacity() > best.FreeCapacity() {
			best = srv
		}
	}
	return best
}
