package scheduler

import (
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/algos"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// baseline describes one v2 policy and an independent oracle for its rule:
// key returns the lexicographic sort key of a server for a task (lower
// wins; the oracle breaks remaining ties by lower ID).
type baseline struct {
	name string
	algo Algorithm
	key  func(task *models.Task, all []*models.Server, s *models.Server) []float64
}

func effSec(task *models.Task, s *models.Server) float64 {
	return float64(task.Duration+s.Latency) / 1000.0
}

func costOf(task *models.Task, s *models.Server) float64 {
	return s.CostPerHour * effSec(task, s) / 3600.0
}

func energyOf(task *models.Task, s *models.Server) float64 {
	return float64(s.PowerWatts) * effSec(task, s) / 3600.0
}

func baselines() []baseline {
	return []baseline{
		{"min_cost", NewMinCost(), func(task *models.Task, _ []*models.Server, s *models.Server) []float64 {
			return []float64{costOf(task, s), energyOf(task, s), float64(s.Latency)}
		}},
		{"min_energy", NewMinEnergy(), func(task *models.Task, _ []*models.Server, s *models.Server) []float64 {
			return []float64{energyOf(task, s), costOf(task, s), float64(s.Latency)}
		}},
		{"eft", NewEFT(), func(task *models.Task, _ []*models.Server, s *models.Server) []float64 {
			return []float64{float64(task.Duration + s.Latency), postPlacementUtil(s, task)}
		}},
		{"weighted_sum", NewWeightedSum(0.4, 0.3, 0.3, 0.25), func(task *models.Task, all []*models.Server, s *models.Server) []float64 {
			// Independent recomputation: vector-normalise each criterion over
			// all candidates and apply the renormalised weights.
			w := []float64{0.4 / 1.25, 0.3 / 1.25, 0.3 / 1.25, 0.25 / 1.25}
			crit := func(x *models.Server) []float64 {
				return []float64{float64(task.Duration + x.Latency), costOf(task, x), energyOf(task, x), postPlacementUtil(x, task)}
			}
			norms := make([]float64, 4)
			for _, x := range all {
				for j, v := range crit(x) {
					norms[j] += v * v
				}
			}
			score := 0.0
			for j, v := range crit(s) {
				if norms[j] > 0 {
					score += w[j] * v / math.Sqrt(norms[j])
				}
			}
			return []float64{score}
		}},
		{"best_fit_norm", NewBestFitNorm(), func(_ *models.Task, _ []*models.Server, s *models.Server) []float64 {
			return []float64{meanFreeFraction(s)}
		}},
	}
}

// oracle returns the ID the rule should choose. Keys within tieTolerance
// (relative) are ties, as the policies document.
func oracle(b baseline, task *models.Task, cands []*models.Server) string {
	var best *models.Server
	var bestKey []float64
	for _, s := range cands {
		k := b.key(task, cands, s)
		if best == nil || keyLess(k, bestKey) || (!keyLess(bestKey, k) && s.ID < best.ID) {
			best, bestKey = s, k
		}
	}
	return best.ID
}

func keyLess(a, b []float64) bool {
	for i := range a {
		if math.Abs(a[i]-b[i]) > tieTolerance*math.Max(math.Abs(a[i]), math.Abs(b[i])) {
			return a[i] < b[i]
		}
	}
	return false
}

func TestBaselinesRegistered(t *testing.T) {
	for _, b := range baselines() {
		if b.algo.Name() != b.name {
			t.Errorf("Name() = %q, want %q", b.algo.Name(), b.name)
		}
		if !algos.Valid(b.name) {
			t.Errorf("%s not registered in internal/algos", b.name)
		}
		if got := NewAlgorithm(b.name, 1, 0.4, 0.3, 0.3, 0.25).Name(); got != b.name {
			t.Errorf("NewAlgorithm(%q) built %q", b.name, got)
		}
	}
	if got := NewAlgorithm("topsis_u0", 1, 0.4, 0.3, 0.3, 0.25).Name(); got != "topsis_u0" {
		t.Errorf("NewAlgorithm(topsis_u0) built %q", got)
	}
	for _, name := range algos.All() {
		if got := NewAlgorithm(name, 1, 0.4, 0.3, 0.3, 0.25).Name(); got != name {
			t.Errorf("registered %q but NewAlgorithm builds %q", name, got)
		}
	}
}

func TestBaselinesEmptyAndSingle(t *testing.T) {
	task := &models.Task{ID: "t", CPU: 1, Memory: 128, Duration: 10}
	only := &models.Server{ID: "only", MaxCPU: 4, MaxMemory: 512, MaxDisk: 5}
	for _, b := range append(baselines(), baseline{name: "topsis_u0", algo: NewTOPSISU0(0.4, 0.3, 0.3)}) {
		if b.algo.Select(task, nil) != nil || b.algo.Select(task, []*models.Server{}) != nil {
			t.Errorf("%s: want nil on empty input", b.name)
		}
		if b.algo.Select(task, []*models.Server{only}) != only {
			t.Errorf("%s: single candidate not returned unchanged", b.name)
		}
	}
}

// TestBaselinesMatchOracleAnyOrder checks every policy against its oracle on
// the 500 golden cases, under the original order, reversed order and a
// shuffle, and that the caller's slice is never modified.
func TestBaselinesMatchOracleAnyOrder(t *testing.T) {
	cases := goldenCases(500)
	rng := rand.New(rand.NewSource(3))
	for _, b := range baselines() {
		for i, c := range cases {
			want := oracle(b, c.task, c.candidates)
			orders := [][]*models.Server{append([]*models.Server(nil), c.candidates...)}
			rev := append([]*models.Server(nil), c.candidates...)
			for l, r := 0, len(rev)-1; l < r; l, r = l+1, r-1 {
				rev[l], rev[r] = rev[r], rev[l]
			}
			shuf := append([]*models.Server(nil), c.candidates...)
			rng.Shuffle(len(shuf), func(a, z int) { shuf[a], shuf[z] = shuf[z], shuf[a] })
			orders = append(orders, rev, shuf)
			for _, in := range orders {
				before := append([]*models.Server(nil), in...)
				got := b.algo.Select(c.task, in)
				if got == nil || got.ID != want {
					t.Fatalf("%s case %d: got %v, oracle %s", b.name, i, got, want)
				}
				if !reflect.DeepEqual(before, in) {
					t.Fatalf("%s case %d: input slice was modified", b.name, i)
				}
			}
		}
	}
}

func TestBaselineTieBreaks(t *testing.T) {
	task := &models.Task{ID: "t", CPU: 1, Memory: 100, Disk: 1, Duration: 1000}
	s := func(id string, lat int, cost float64, power, usedCPU int) *models.Server {
		return &models.Server{ID: id, MaxCPU: 4, UsedCPU: usedCPU, MaxMemory: 1000, MaxDisk: 10, Latency: lat, CostPerHour: cost, PowerWatts: power}
	}
	tests := []struct {
		name  string
		algo  Algorithm
		cands []*models.Server
		want  string
	}{
		{"min_cost: equal cost, lower energy wins", NewMinCost(),
			[]*models.Server{s("a", 10, 0.25, 200, 0), s("b", 10, 0.25, 100, 0)}, "b"},
		{"min_cost: equal cost and energy, lower latency wins", NewMinCost(),
			// Same effective time for cost/energy needs same latency, so use
			// zero price and power: all keys tie except latency.
			[]*models.Server{s("a", 20, 0, 0, 0), s("b", 10, 0, 0, 0)}, "b"},
		{"min_cost: full tie, lower ID wins", NewMinCost(),
			[]*models.Server{s("b", 10, 0.25, 100, 0), s("a", 10, 0.25, 100, 0)}, "a"},
		{"min_energy: equal energy, lower cost wins", NewMinEnergy(),
			[]*models.Server{s("a", 10, 1.5, 100, 0), s("b", 10, 0.02, 100, 0)}, "b"},
		{"min_energy: full tie, lower ID wins", NewMinEnergy(),
			[]*models.Server{s("z", 10, 0.25, 100, 0), s("y", 10, 0.25, 100, 0)}, "y"},
		{"eft: equal finish, lower utilisation wins", NewEFT(),
			[]*models.Server{s("a", 10, 0, 0, 3), s("b", 10, 0, 0, 1)}, "b"},
		{"eft: full tie, lower ID wins", NewEFT(),
			[]*models.Server{s("b", 10, 0, 0, 1), s("a", 10, 0, 0, 1)}, "a"},
		{"weighted_sum: full tie, lower ID wins", NewWeightedSum(0.4, 0.3, 0.3, 0.25),
			[]*models.Server{s("b", 10, 0.25, 100, 1), s("a", 10, 0.25, 100, 1)}, "a"},
		{"best_fit_norm: full tie, lower ID wins", NewBestFitNorm(),
			[]*models.Server{s("b", 10, 0, 0, 2), s("a", 10, 0, 0, 2)}, "a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.algo.Select(task, tc.cands); got.ID != tc.want {
				t.Fatalf("got %s, want %s", got.ID, tc.want)
			}
		})
	}
}

// TestWeightedSumDiffersFromTOPSIS documents that the aggregation matters:
// on identical criteria, normalisation and weights, the two policies choose
// different servers. a: mid latency, expensive, low power; b: fast, mid
// price, mid power; c: slow, cheapest, highest power.
func TestWeightedSumDiffersFromTOPSIS(t *testing.T) {
	task := &models.Task{ID: "t", CPU: 1, Memory: 100, Duration: 1000}
	cands := []*models.Server{
		{ID: "a", MaxCPU: 4, UsedCPU: 1, MaxMemory: 1000, MaxDisk: 10, Latency: 50, CostPerHour: 1.5, PowerWatts: 60},
		{ID: "b", MaxCPU: 4, UsedCPU: 2, MaxMemory: 1000, MaxDisk: 10, Latency: 20, CostPerHour: 0.25, PowerWatts: 140},
		{ID: "c", MaxCPU: 4, UsedCPU: 0, MaxMemory: 1000, MaxDisk: 10, Latency: 80, CostPerHour: 0.02, PowerWatts: 240},
	}
	tp := NewTOPSIS(0.4, 0.3, 0.3, 0.25).Select(task, cands)
	ws := NewWeightedSum(0.4, 0.3, 0.3, 0.25).Select(task, cands)
	if tp.ID != "b" || ws.ID != "c" {
		t.Fatalf("topsis chose %s (want b), weighted_sum chose %s (want c)", tp.ID, ws.ID)
	}
}

// TestTOPSISU0EqualsTOPSISWithZeroUtil checks the ablation is exactly
// NewTOPSIS(0.4, 0.3, 0.3, 0) on the golden cases.
func TestTOPSISU0EqualsTOPSISWithZeroUtil(t *testing.T) {
	cases := goldenCases(500)
	u0 := NewTOPSISU0(0.4, 0.3, 0.3)
	ref := NewTOPSIS(0.4, 0.3, 0.3, 0)
	if u0.inner.wTime != 0.4 || u0.inner.wCost != 0.3 || u0.inner.wEnergy != 0.3 || u0.inner.wUtil != 0 {
		t.Fatalf("weights = %+v, want (0.4, 0.3, 0.3, 0)", *u0.inner)
	}
	differsFromTopsis := 0
	full := NewTOPSIS(0.4, 0.3, 0.3, 0.25)
	for i, c := range cases {
		got, want := u0.Select(c.task, c.candidates), ref.Select(c.task, c.candidates)
		if got.ID != want.ID {
			t.Fatalf("case %d: topsis_u0 %s, NewTOPSIS(...,0) %s", i, got.ID, want.ID)
		}
		if got.ID != full.Select(c.task, c.candidates).ID {
			differsFromTopsis++
		}
	}
	if differsFromTopsis == 0 {
		t.Error("topsis_u0 never differs from topsis on the golden cases; the ablation would be vacuous")
	}
}
