package scheduler

import "github.com/pcturnes/distributed-workflow-scheduler/internal/models"

// TOPSISU0 is TOPSIS with the utilisation criterion disabled: the time, cost
// and energy weights are renormalised on their own (the campaign's
// 0.4/0.3/0.3 stay 0.4/0.3/0.3). It ablates the utilisation criterion.
// Selection, normalisation and tie-breaking are those of TOPSIS.
type TOPSISU0 struct {
	inner *TOPSIS
}

// NewTOPSISU0 is NewTOPSIS(wTime, wCost, wEnergy, 0) under its own name.
func NewTOPSISU0(wTime, wCost, wEnergy float64) *TOPSISU0 {
	return &TOPSISU0{inner: NewTOPSIS(wTime, wCost, wEnergy, 0)}
}

// Name returns the identifier used in configs and results.
func (*TOPSISU0) Name() string { return "topsis_u0" }

// Select delegates to TOPSIS with zero utilisation weight.
func (t *TOPSISU0) Select(task *models.Task, candidates []*models.Server) *models.Server {
	return t.inner.Select(task, candidates)
}
