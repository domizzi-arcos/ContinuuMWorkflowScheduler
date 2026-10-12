package scheduler

import (
	"sort"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// computeCriteria returns, for each server in sorted (same order), the four
// "lower is better" criteria shared by TOPSIS and the greedy baselines:
//
//   - time:   effective wall time d+ℓ in ms (task duration + server latency);
//   - cost:   price × (d+ℓ) / 3.6e6, in USD (price is per hour, d+ℓ in ms);
//   - energy: power × (d+ℓ) / 3.6e6, in Wh;
//   - util:   worst post-placement fractional usage over CPU, memory, disk.
//
// Cost and energy are charged against the same effective time the worker
// sleeps for, matching the recorded results.
func computeCriteria(task *models.Task, sorted []*models.Server) (time, cost, energy, util []float64) {
	n := len(sorted)
	time = make([]float64, n)
	cost = make([]float64, n)
	energy = make([]float64, n)
	util = make([]float64, n)
	for i, srv := range sorted {
		effectiveMS := task.Duration + srv.Latency
		effectiveSec := float64(effectiveMS) / 1000.0
		time[i] = float64(effectiveMS)
		cost[i] = srv.CostPerHour * effectiveSec / 3600.0
		energy[i] = float64(srv.PowerWatts) * effectiveSec / 3600.0
		util[i] = postPlacementUtil(srv, task)
	}
	return time, cost, energy, util
}

// sortedByID returns a copy of candidates ordered by server ID. Policies
// select from this copy so the caller's slice is never mutated and equal
// inputs always resolve ties the same way.
func sortedByID(candidates []*models.Server) []*models.Server {
	sorted := make([]*models.Server, len(candidates))
	copy(sorted, candidates)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	return sorted
}
