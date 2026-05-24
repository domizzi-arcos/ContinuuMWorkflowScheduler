package scheduler

import (
	"math"
	"sort"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// TOPSIS scores each candidate server on up to four criteria — estimated
// execution time, monetary cost, energy consumption, and post-placement
// capacity utilisation — using the Technique for Order of Preference by
// Similarity to Ideal Solution (TOPSIS) method.
// All criteria are "lower is better", so the positive-ideal solution is
// the minimum on every axis and the negative-ideal is the maximum.
//
// The utilisation criterion (wUtil) breaks the saturation pathology where
// a static-cost-only score keeps picking the same edge node until its
// queue blocks. When the chosen tier saturates, wUtil pushes successive
// tasks to underused servers on adjacent tiers. Set wUtil=0 to recover
// the original 3-criterion behaviour.
//
// Weight semantics: NewTOPSIS renormalises all four weights so they sum to
// 1.0. Users may supply weights on any scale (the canonical example in
// config.yaml lists 0.4/0.3/0.3/0.25, sum 1.25) — the runtime weights are
// recovered by dividing each by the total. The renormalised weights are
// what every result and log line refers to. See the doc comment on
// config.yaml's topsis_weights.
type TOPSIS struct {
	wTime   float64
	wCost   float64
	wEnergy float64
	wUtil   float64
}

// NewTOPSIS constructs a TOPSIS instance with the four criterion weights
// renormalised to sum to 1.0. A non-positive total falls back to equal
// quarters so the scheduler stays deterministic on misconfiguration.
func NewTOPSIS(wTime, wCost, wEnergy, wUtil float64) *TOPSIS {
	total := wTime + wCost + wEnergy + wUtil
	if total <= 0 {
		wTime, wCost, wEnergy, wUtil = 0.25, 0.25, 0.25, 0.25
		total = 1
	}
	return &TOPSIS{
		wTime:   wTime / total,
		wCost:   wCost / total,
		wEnergy: wEnergy / total,
		wUtil:   wUtil / total,
	}
}

func (t *TOPSIS) Name() string { return "topsis" }

func (t *TOPSIS) Select(task *models.Task, candidates []*models.Server) *models.Server {
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0]
	}

	// Sort candidates by ID up front so two runs with identical inputs always
	// produce the same matrix order and the same ties resolve the same way.
	sorted := make([]*models.Server, len(candidates))
	copy(sorted, candidates)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	n := len(sorted)

	rawTime := make([]float64, n)
	rawCost := make([]float64, n)
	rawEnergy := make([]float64, n)
	rawUtil := make([]float64, n)
	for i, srv := range sorted {
		// Effective wall time = task.Duration + server.Latency (ms). Cost and
		// energy are charged against that same effective time so the criteria
		// match the wall-time figures recorded in the result dataset.
		effectiveMS := task.Duration + srv.Latency
		effectiveSec := float64(effectiveMS) / 1000.0
		rawTime[i] = float64(effectiveMS)
		rawCost[i] = srv.CostPerHour * effectiveSec / 3600.0
		rawEnergy[i] = float64(srv.PowerWatts) * effectiveSec / 3600.0
		rawUtil[i] = postPlacementUtil(srv, task)
	}

	wt := weightedNorm(rawTime, t.wTime)
	wc := weightedNorm(rawCost, t.wCost)
	we := weightedNorm(rawEnergy, t.wEnergy)
	wu := weightedNorm(rawUtil, t.wUtil)

	idealT, antiT := minMaxSlice(wt)
	idealC, antiC := minMaxSlice(wc)
	idealE, antiE := minMaxSlice(we)
	idealU, antiU := minMaxSlice(wu)

	best, bestScore := 0, -1.0
	for i := range sorted {
		dPlus := math.Sqrt(sq(wt[i]-idealT) + sq(wc[i]-idealC) + sq(we[i]-idealE) + sq(wu[i]-idealU))
		dMinus := math.Sqrt(sq(wt[i]-antiT) + sq(wc[i]-antiC) + sq(we[i]-antiE) + sq(wu[i]-antiU))
		var score float64
		if dPlus+dMinus > 0 {
			score = dMinus / (dPlus + dMinus)
		}
		if score > bestScore {
			best = i
			bestScore = score
		}
	}
	return sorted[best]
}

// postPlacementUtil returns the worst fractional usage across CPU/memory/disk
// after hypothetically placing the task on the server. Range is roughly
// [0, 1]; >1 only happens if filterByCapacity is bypassed.
func postPlacementUtil(srv *models.Server, task *models.Task) float64 {
	worst := 0.0
	if srv.MaxCPU > 0 {
		if u := float64(srv.UsedCPU+task.CPU) / float64(srv.MaxCPU); u > worst {
			worst = u
		}
	}
	if srv.MaxMemory > 0 {
		if u := float64(srv.UsedMemory+task.Memory) / float64(srv.MaxMemory); u > worst {
			worst = u
		}
	}
	if srv.MaxDisk > 0 {
		if u := float64(srv.UsedDisk+task.Disk) / float64(srv.MaxDisk); u > worst {
			worst = u
		}
	}
	return worst
}

func weightedNorm(v []float64, w float64) []float64 {
	norm := vecNorm(v)
	out := make([]float64, len(v))
	for i, x := range v {
		if norm > 0 {
			out[i] = w * x / norm
		}
	}
	return out
}

func vecNorm(v []float64) float64 {
	sum := 0.0
	for _, x := range v {
		sum += x * x
	}
	return math.Sqrt(sum)
}

func minMaxSlice(v []float64) (float64, float64) {
	mn, mx := v[0], v[0]
	for _, x := range v[1:] {
		if x < mn {
			mn = x
		}
		if x > mx {
			mx = x
		}
	}
	return mn, mx
}

func sq(x float64) float64 { return x * x }
