package scheduler

import "github.com/pcturnes/distributed-workflow-scheduler/internal/models"

// WorstFit picks the candidate with the most remaining free capacity —
// spreading load across servers to avoid hot spots.
type WorstFit struct{}

func NewWorstFit() *WorstFit { return &WorstFit{} }

func (w *WorstFit) Name() string { return "worst_fit" }

func (w *WorstFit) Select(_ *models.Task, candidates []*models.Server) *models.Server {
	var best *models.Server
	for _, srv := range candidates {
		if best == nil || srv.FreeCapacity() > best.FreeCapacity() {
			best = srv
		}
	}
	return best
}
