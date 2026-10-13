package scheduler

import (
	"math"
	"sort"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// computeCriteria returns, for each server in sorted (same order), the four
// "lower is better" criteria shared by TOPSIS and the greedy baselines:
//
//   - time:   effective wall time d/s+ℓ in ms (task duration over server
//     speed, plus server latency; see Server.ExecMS);
//   - cost:   price × (d/s+ℓ) / 3.6e6, in USD (price is per hour);
//   - energy: power × (d/s+ℓ) / 3.6e6, in Wh;
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
		effectiveMS := srv.ExecMS(task.Duration)
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

// single returns the only candidate, or nil for an empty slice.
func single(candidates []*models.Server) *models.Server {
	if len(candidates) == 0 {
		return nil
	}
	return candidates[0]
}

// tieTolerance is the relative difference below which two scores are
// treated as equal by the v2 baselines. Mathematically equal scores can
// differ in the last bits depending on how they were computed; treating them
// as ties lets the documented tie-break (next key, then lower ID) decide.
const tieTolerance = 1e-9

// lexLess reports whether key a sorts strictly before key b, comparing
// element by element; elements within tieTolerance (relative) are equal.
// Equal keys leave the earlier (lower-ID) candidate in place.
func lexLess(a, b []float64) bool {
	for i := range a {
		if math.Abs(a[i]-b[i]) > tieTolerance*math.Max(math.Abs(a[i]), math.Abs(b[i])) {
			return a[i] < b[i]
		}
	}
	return false
}
