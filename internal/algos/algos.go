// Package algos is the single source of truth for scheduling-policy names.
// It is a leaf package (no imports) so config, scheduler and the runners
// can all depend on it without import cycles. scripts/algorithms.py mirrors
// these lists; a test in this package keeps the two in sync.
package algos

// legacy are the five v1 policies, in v1 run order.
var legacy = []string{"round_robin", "random", "best_fit", "worst_fit", "topsis"}

// core is the v2 default set, in run and plot order: the v1 policies, then
// the price/energy-aware baselines and the TOPSIS ablations.
var core = append(append([]string(nil), legacy...),
	"min_cost", "min_energy", "eft", "weighted_sum", "topsis_u0")

// optional policies are registered (valid in configs and -algorithms) but
// not run unless asked for.
var optional = []string{"best_fit_norm"}

func clone(s []string) []string { return append([]string(nil), s...) }

// Names returns a copy of the default ordered policy list (v2 core set).
func Names() []string { return clone(core) }

// Legacy returns a copy of the five v1 policies in v1 order.
func Legacy() []string { return clone(legacy) }

// Optional returns a copy of the registered non-default policies.
func Optional() []string { return clone(optional) }

// All returns Names followed by Optional.
func All() []string { return append(Names(), optional...) }

// Valid reports whether name is a registered policy (exact, lower-case match).
func Valid(name string) bool {
	for _, n := range All() {
		if n == name {
			return true
		}
	}
	return false
}
