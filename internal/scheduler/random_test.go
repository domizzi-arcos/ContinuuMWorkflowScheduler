package scheduler

import (
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// TestRandomName pins the algorithm identifier used in config.yaml.
func TestRandomName(t *testing.T) {
	if NewRandom(1).Name() != "random" {
		t.Fatal("unexpected name")
	}
}

// TestRandomSelectNilOnEmpty guards that an empty or nil candidate slice
// produces a nil server, not a panic on an out-of-bounds rng.Intn.
func TestRandomSelectNilOnEmpty(t *testing.T) {
	r := NewRandom(42)
	if r.Select(nil, nil) != nil {
		t.Fatal("expected nil for nil candidates")
	}
	if r.Select(nil, []*models.Server{}) != nil {
		t.Fatal("expected nil for empty slice")
	}
}

// TestRandomSelectReturnsCandidateFromList samples Select 20 times and
// asserts every return value belongs to the original candidate slice — no
// off-by-one or aliasing should ever produce a foreign server.
func TestRandomSelectReturnsCandidateFromList(t *testing.T) {
	servers := []*models.Server{
		{ID: "a"},
		{ID: "b"},
		{ID: "c"},
	}
	ids := map[string]bool{"a": true, "b": true, "c": true}
	r := NewRandom(7)
	for i := 0; i < 20; i++ {
		got := r.Select(nil, servers)
		if got == nil {
			t.Fatalf("call %d: got nil", i)
		}
		if !ids[got.ID] {
			t.Fatalf("call %d: returned server %q not in candidate list", i, got.ID)
		}
	}
}

// TestRandomSelectIsDeterministicWithSameSeed pins reproducibility: two
// Random schedulers built with the same seed must produce identical
// sequences. This is what makes config.servers.seed!=0 runs replayable.
func TestRandomSelectIsDeterministicWithSameSeed(t *testing.T) {
	servers := []*models.Server{
		{ID: "x"},
		{ID: "y"},
		{ID: "z"},
	}
	r1 := NewRandom(42)
	r2 := NewRandom(42)
	for i := 0; i < 15; i++ {
		got1 := r1.Select(nil, servers)
		got2 := r2.Select(nil, servers)
		if got1.ID != got2.ID {
			t.Fatalf("call %d: seed 42 produced different results: %s vs %s", i, got1.ID, got2.ID)
		}
	}
}

// TestRandomSelectDifferentSeedsProduceDifferentSequences is a sanity check:
// independent seeds must not produce identical sequences over 20 trials.
// Identical sequences would indicate the seed is being ignored.
func TestRandomSelectDifferentSeedsProduceDifferentSequences(t *testing.T) {
	servers := []*models.Server{
		{ID: "a"},
		{ID: "b"},
		{ID: "c"},
		{ID: "d"},
	}
	r1 := NewRandom(1)
	r2 := NewRandom(9999)
	same := 0
	const calls = 20
	for i := 0; i < calls; i++ {
		if r1.Select(nil, servers).ID == r2.Select(nil, servers).ID {
			same++
		}
	}
	// Two independent random sequences over 4 choices should not agree 100% of the time
	if same == calls {
		t.Fatal("different seeds produced identical sequences — seeding appears broken")
	}
}
