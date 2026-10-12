// Package algos is the single source of truth for scheduling-policy names.
// It is a leaf package (no imports) so config, scheduler and the runners
// can all depend on it without import cycles.
package algos

// names is the canonical, ordered list of registered policies. The order is
// the order in which the runners execute them and in which plots list them.
var names = []string{"round_robin", "random", "best_fit", "worst_fit", "topsis"}

// Names returns a copy of the canonical ordered policy list.
func Names() []string {
	out := make([]string, len(names))
	copy(out, names)
	return out
}

// Valid reports whether name is a registered policy (exact, lower-case match).
func Valid(name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}
