// Best-fit scheduling: place the task on the candidate with the least free
// capacity that still fits. Tightens packing, keeps large servers idle for
// future heavy jobs, and tends to push small tasks toward already-busy nodes,
// favouring resource locality over throughput balance.
package scheduler

import (
	"sort"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// BestFit picks the candidate with the least remaining free capacity —
// packing tasks tightly to leave larger servers free for heavier jobs.
type BestFit struct{}

// NewBestFit builds a stateless BestFit scheduler.
func NewBestFit() *BestFit { return &BestFit{} }

// Name returns the algorithm identifier used in config.yaml and logs.
func (b *BestFit) Name() string { return "best_fit" }

// Select returns the candidate with the smallest FreeCapacity (the tightest
// fit). Returns nil if candidates is empty. The task argument is unused — the
// caller has already filtered for capacity.
func (b *BestFit) Select(_ *models.Task, candidates []*models.Server) *models.Server {
	// Sort by ID first so ties break deterministically — Redis SCAN order is
	// not stable, and a thesis claiming reproducibility cannot inherit that.
	sorted := make([]*models.Server, len(candidates))
	copy(sorted, candidates)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	var best *models.Server
	for _, srv := range sorted {
		if best == nil || srv.FreeCapacity() < best.FreeCapacity() {
			best = srv
		}
	}
	return best
}
