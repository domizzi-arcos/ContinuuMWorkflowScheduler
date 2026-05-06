package scheduler

import (
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

func TestRandomName(t *testing.T) {
	if NewRandom(1).Name() != "random" {
		t.Fatal("unexpected name")
	}
}

func TestRandomSelectNilOnEmpty(t *testing.T) {
	r := NewRandom(42)
	if r.Select(nil, nil) != nil {
		t.Fatal("expected nil for nil candidates")
	}
	if r.Select(nil, []*models.Server{}) != nil {
		t.Fatal("expected nil for empty slice")
	}
}

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
