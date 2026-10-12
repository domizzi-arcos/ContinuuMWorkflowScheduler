package scheduler

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

var update = flag.Bool("update", false, "rewrite golden files")

// goldenCase is one (task, candidate set) input. Candidates are generated in
// shuffled order and with deliberately coarse values so ties occur.
type goldenCase struct {
	task       *models.Task
	candidates []*models.Server
}

func goldenCases(n int) []goldenCase {
	rng := rand.New(rand.NewSource(20261012))
	tiers := []struct {
		name  string
		cost  float64
		cpu   []int
		lat   []int
		power []int
	}{
		{"edge", 0.02, []int{1, 2}, []int{3, 5, 5, 12}, []int{10, 15, 25}},
		{"fog", 0.25, []int{4, 6, 8}, []int{20, 30, 30, 45}, []int{100, 150, 200}},
		{"cloud", 1.50, []int{16, 24, 32}, []int{80, 120, 160}, []int{300, 450, 600}},
	}
	pick := func(v []int) int { return v[rng.Intn(len(v))] }
	cases := make([]goldenCase, n)
	for i := range cases {
		task := &models.Task{
			ID:       fmt.Sprintf("t%03d", i),
			CPU:      1 + rng.Intn(2),
			Memory:   128 * (1 + rng.Intn(4)),
			Disk:     rng.Intn(5),
			Duration: []int{0, 1, 30, 990, 5500, 400000}[rng.Intn(6)],
		}
		k := 1 + rng.Intn(24)
		cands := make([]*models.Server, k)
		for j := range cands {
			tr := tiers[rng.Intn(len(tiers))]
			// Candidates always fit the task, as filterByCapacity guarantees.
			maxCPU := max(pick(tr.cpu), task.CPU)
			maxMem := 1024 * (1 + rng.Intn(8))
			maxDisk := 20 + 10*rng.Intn(5)
			cands[j] = &models.Server{
				ID:          fmt.Sprintf("%s_%02d", tr.name, j),
				Type:        strings.ToUpper(tr.name),
				MaxCPU:      maxCPU,
				MaxMemory:   maxMem,
				MaxDisk:     maxDisk,
				UsedCPU:     rng.Intn(maxCPU - task.CPU + 1),
				UsedMemory:  128 * rng.Intn((maxMem-task.Memory)/128+1),
				UsedDisk:    rng.Intn(maxDisk - task.Disk + 1),
				Latency:     pick(tr.lat),
				CostPerHour: tr.cost,
				PowerWatts:  pick(tr.power),
			}
		}
		rng.Shuffle(len(cands), func(a, b int) { cands[a], cands[b] = cands[b], cands[a] })
		cases[i] = goldenCase{task, cands}
	}
	return cases
}

// selections runs every case through algo in order (stateful policies keep
// their state across cases) and returns the chosen IDs. Each call gets its
// own copy of the candidate slice; the caller's order is preserved.
func selections(algo Algorithm, cases []goldenCase) []string {
	out := make([]string, len(cases))
	for i, c := range cases {
		cands := append([]*models.Server(nil), c.candidates...)
		if s := algo.Select(c.task, cands); s != nil {
			out[i] = s.ID
		}
	}
	return out
}

// legacyPolicies are the five v1 policies, with the campaign's weights and
// seed. Their selections are pinned by testdata/policies.golden.
func legacyPolicies() []Algorithm {
	return []Algorithm{
		NewRoundRobin(), NewRandom(42), NewBestFit(), NewWorstFit(),
		NewTOPSIS(0.4, 0.3, 0.3, 0.25),
	}
}

// TestLegacyPoliciesGolden pins the choices of the five v1 policies on 500
// seeded random cases. Regenerate only with intent:
// go test ./internal/scheduler -run LegacyPoliciesGolden -update
func TestLegacyPoliciesGolden(t *testing.T) {
	cases := goldenCases(500)
	var b strings.Builder
	for _, algo := range legacyPolicies() {
		fmt.Fprintf(&b, "%s %s\n", algo.Name(), strings.Join(selections(algo, cases), " "))
	}
	path := filepath.Join("testdata", "policies.golden")
	if *update {
		if err := os.WriteFile(path, []byte(b.String()), 0644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (run with -update to create): %v", err)
	}
	if string(want) != b.String() {
		wl, gl := strings.Split(string(want), "\n"), strings.Split(b.String(), "\n")
		for i := range gl {
			if i < len(wl) && wl[i] != gl[i] {
				t.Errorf("policy line %d differs: %.40s...", i, gl[i])
			}
		}
		t.Fatalf("selections differ from %s", path)
	}
}
