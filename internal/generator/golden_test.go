package generator

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

var update = flag.Bool("update", false, "rewrite golden files")

type drawCase struct {
	seed             int64
	prioMin, prioMax int
	nTasks           int
}

// drawCases cover the campaign range (1..10), a degenerate range (no
// priority draw), the clamping paths of New, and small/large pools.
var drawCases = []drawCase{
	{1, 1, 10, 120}, {2, 1, 10, 120}, {3, 1, 10, 120}, {42, 1, 10, 120},
	{7, 5, 5, 120}, {7, 0, 3, 50}, {7, 6, 2, 7},
}

const goldenWorkflows = 20

func syntheticTasks(n int) []*models.Task {
	tasks := make([]*models.Task, n)
	for i := range tasks {
		id := fmt.Sprintf("task_%04d", i+1)
		tasks[i] = &models.Task{ID: id, Name: id}
	}
	return tasks
}

func caseLabel(c drawCase) string {
	return fmt.Sprintf("seed=%d prio=[%d,%d] tasks=%d", c.seed, c.prioMin, c.prioMax, c.nTasks)
}

// liveDraws runs the real Generator.instantiateWorkflow against miniredis
// and reads back, per workflow in creation order, the template, priority and
// task ID of every slot.
func liveDraws(t *testing.T, c drawCase, n int) []string {
	t.Helper()
	mr := miniredis.RunT(t)
	s, err := store.NewRedisStore(mr.Addr(), 0)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	out := make(chan string, 64*n)
	g := New(s, syntheticTasks(c.nTasks), out, nil, c.seed, c.prioMin, c.prioMax)
	done := make(chan struct{})
	for i := 0; i < n; i++ {
		if err := g.instantiateWorkflow(done); err != nil {
			t.Fatalf("instantiateWorkflow: %v", err)
		}
	}
	wfs, err := s.GetAllWorkflowInstances()
	if err != nil {
		t.Fatalf("GetAllWorkflowInstances: %v", err)
	}
	// Workflow IDs embed UnixNano; execution IDs are allocated sequentially,
	// so order workflows by their slot-A execution ID.
	sort.Slice(wfs, func(i, j int) bool { return wfs[i].SlotExecIDs["A"] < wfs[j].SlotExecIDs["A"] })
	lines := make([]string, 0, len(wfs))
	for _, wi := range wfs {
		slots := make([]string, 0, len(wi.SlotExecIDs))
		for slot := range wi.SlotExecIDs {
			slots = append(slots, slot)
		}
		sort.Strings(slots)
		ids := make([]string, len(slots))
		for k, slot := range slots {
			ex, err := s.GetExecution(wi.SlotExecIDs[slot])
			if err != nil {
				t.Fatalf("GetExecution: %v", err)
			}
			ids[k] = ex.TaskID
		}
		lines = append(lines, fmt.Sprintf("%s p=%d %s", wi.TemplateName, wi.Priority, strings.Join(ids, ",")))
	}
	return lines
}

// TestGeneratorDrawsGolden pins the RNG consumption of the live generator.
// Regenerate only with intent: go test ./internal/generator -run Golden -update
func TestGeneratorDrawsGolden(t *testing.T) {
	var b strings.Builder
	for _, c := range drawCases {
		b.WriteString(caseLabel(c) + "\n")
		for _, l := range liveDraws(t, c, goldenWorkflows) {
			b.WriteString("  " + l + "\n")
		}
	}
	path := filepath.Join("testdata", "draws.golden")
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
		t.Fatalf("live draws differ from %s", path)
	}
}

// TestReplayMatchesLiveDraws checks ReplayWorkflows reproduces the live
// generator's draws for every golden case.
func TestReplayMatchesLiveDraws(t *testing.T) {
	for _, c := range drawCases {
		live := liveDraws(t, c, goldenWorkflows)
		draws, err := ReplayWorkflows(syntheticTasks(c.nTasks), c.seed, c.prioMin, c.prioMax, goldenWorkflows)
		if err != nil {
			t.Fatalf("%s: ReplayWorkflows: %v", caseLabel(c), err)
		}
		if len(draws) != len(live) {
			t.Fatalf("%s: %d replayed, %d live", caseLabel(c), len(draws), len(live))
		}
		for i, d := range draws {
			ids := make([]string, len(d.Tasks))
			for k, task := range d.Tasks {
				ids[k] = task.ID
			}
			got := fmt.Sprintf("%s p=%d %s", d.Template.Name, d.Priority, strings.Join(ids, ","))
			if got != live[i] || d.Seq != i {
				t.Fatalf("%s workflow %d: replay %q (seq %d), live %q", caseLabel(c), i, got, d.Seq, live[i])
			}
		}
	}
}

func TestReplayRejectsSeedZero(t *testing.T) {
	if _, err := ReplayWorkflows(syntheticTasks(3), 0, 1, 10, 1); err == nil {
		t.Fatal("want error for seed 0")
	}
}
