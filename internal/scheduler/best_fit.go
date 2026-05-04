package scheduler

import "github.com/pcturnes/distributed-workflow-scheduler/internal/models"

// BestFit picks the candidate with the least remaining free capacity —
// packing tasks tightly to leave larger servers free for heavier jobs.
type BestFit struct{}

func NewBestFit() *BestFit { return &BestFit{} }

func (b *BestFit) Name() string { return "best_fit" }

func (b *BestFit) Select(_ *models.Task, candidates []*models.Server) *models.Server {
	var best *models.Server
	for _, srv := range candidates {
		if best == nil || srv.FreeCapacity() < best.FreeCapacity() {
			best = srv
		}
	}
	return best
}
