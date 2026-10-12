package scheduler

import "github.com/pcturnes/distributed-workflow-scheduler/internal/models"

// WeightedSum scores candidates on exactly the inputs TOPSIS uses — the four
// criteria of computeCriteria, vector-normalised per criterion, with the same
// renormalised weights — but aggregates them as a plain weighted sum
// Σⱼ wⱼ·rᵢⱼ and picks the minimum. Comparing it with TOPSIS isolates the
// effect of TOPSIS's distance-to-ideal aggregation. A criterion whose
// column norm is 0 contributes 0, as in TOPSIS. Ties (scores equal within
// tieTolerance) are broken by lower server ID. The candidate slice is not modified.
type WeightedSum struct {
	wTime, wCost, wEnergy, wUtil float64
}

// NewWeightedSum renormalises the weights exactly as NewTOPSIS does.
func NewWeightedSum(wTime, wCost, wEnergy, wUtil float64) *WeightedSum {
	t := NewTOPSIS(wTime, wCost, wEnergy, wUtil)
	return &WeightedSum{wTime: t.wTime, wCost: t.wCost, wEnergy: t.wEnergy, wUtil: t.wUtil}
}

// Name returns the identifier used in configs and results.
func (*WeightedSum) Name() string { return "weighted_sum" }

// Select returns the candidate with the lowest weighted sum, or nil when
// there are none.
func (w *WeightedSum) Select(task *models.Task, candidates []*models.Server) *models.Server {
	if len(candidates) <= 1 {
		return single(candidates)
	}
	sorted := sortedByID(candidates)
	rawTime, rawCost, rawEnergy, rawUtil := computeCriteria(task, sorted)
	wt := weightedNorm(rawTime, w.wTime)
	wc := weightedNorm(rawCost, w.wCost)
	we := weightedNorm(rawEnergy, w.wEnergy)
	wu := weightedNorm(rawUtil, w.wUtil)
	best, bestScore := 0, wt[0]+wc[0]+we[0]+wu[0]
	for i := 1; i < len(sorted); i++ {
		if score := wt[i] + wc[i] + we[i] + wu[i]; lexLess([]float64{score}, []float64{bestScore}) {
			best, bestScore = i, score
		}
	}
	return sorted[best]
}
