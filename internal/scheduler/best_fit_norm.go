package scheduler

import "github.com/pcturnes/distributed-workflow-scheduler/internal/models"

// BestFitNorm is Best Fit without the unit-mixing score of
// Server.FreeCapacity. Over the feasible candidates it picks the server
// with the lowest mean free fraction, the mean over CPU, memory and disk of
// free_r / max_r (resources with max_r = 0 are left out of the mean). Ties
// (scores equal within tieTolerance) are broken by lower server ID. The candidate slice is not modified.
type BestFitNorm struct{}

// NewBestFitNorm builds a stateless BestFitNorm policy.
func NewBestFitNorm() *BestFitNorm { return &BestFitNorm{} }

// Name returns the identifier used in configs and results.
func (BestFitNorm) Name() string { return "best_fit_norm" }

// Select returns the tightest candidate by mean free fraction, or nil when
// there are none.
func (BestFitNorm) Select(_ *models.Task, candidates []*models.Server) *models.Server {
	if len(candidates) <= 1 {
		return single(candidates)
	}
	sorted := sortedByID(candidates)
	best, bestScore := 0, meanFreeFraction(sorted[0])
	for i := 1; i < len(sorted); i++ {
		if score := meanFreeFraction(sorted[i]); lexLess([]float64{score}, []float64{bestScore}) {
			best, bestScore = i, score
		}
	}
	return sorted[best]
}

func meanFreeFraction(s *models.Server) float64 {
	sum, n := 0.0, 0
	for _, r := range [][2]int{{s.MaxCPU, s.UsedCPU}, {s.MaxMemory, s.UsedMemory}, {s.MaxDisk, s.UsedDisk}} {
		if r[0] > 0 {
			sum += float64(r[0]-r[1]) / float64(r[0])
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return sum / float64(n)
}
