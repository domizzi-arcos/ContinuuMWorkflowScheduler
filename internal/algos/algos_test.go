package algos

import (
	"reflect"
	"testing"
)

func TestNamesOrderAndCopy(t *testing.T) {
	want := []string{"round_robin", "random", "best_fit", "worst_fit", "topsis"}
	got := Names()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Names() = %v, want %v", got, want)
	}
	got[0] = "mutated"
	if Names()[0] != "round_robin" {
		t.Fatal("Names() must return a copy")
	}
}

func TestValid(t *testing.T) {
	for _, n := range Names() {
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
