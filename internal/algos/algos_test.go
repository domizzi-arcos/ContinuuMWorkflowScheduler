package algos

import (
	"reflect"
	"testing"
)

var wantCore = []string{"round_robin", "random", "best_fit", "worst_fit", "topsis",
	"min_cost", "min_energy", "eft", "weighted_sum", "topsis_u0"}

func TestListsAndCopies(t *testing.T) {
	if got := Names(); !reflect.DeepEqual(got, wantCore) {
		t.Fatalf("Names() = %v, want %v", got, wantCore)
	}
	if got := Legacy(); !reflect.DeepEqual(got, wantCore[:5]) {
		t.Errorf("Legacy() = %v", got)
	}
	if got := All(); !reflect.DeepEqual(got, append(append([]string(nil), wantCore...), "best_fit_norm")) {
		t.Errorf("All() = %v", got)
	}
	n := Names()
	n[0] = "mutated"
	l := Legacy()
	l[0] = "mutated"
	if Names()[0] != "round_robin" || Legacy()[0] != "round_robin" {
		t.Fatal("lists must be returned as copies")
	}
}

func TestValid(t *testing.T) {
	for _, n := range All() {
		if !Valid(n) {
			t.Errorf("Valid(%q) = false", n)
		}
	}
	for _, n := range []string{"", "TOPSIS", "heft", "round-robin"} {
		if Valid(n) {
			t.Errorf("Valid(%q) = true", n)
		}
	}
}
