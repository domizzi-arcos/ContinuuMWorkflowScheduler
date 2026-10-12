package audit

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/generator"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/workflow"
)

func srv(id string, cpu, mem, disk, lat int) *models.Server {
	return &models.Server{ID: id, MaxCPU: cpu, MaxMemory: mem, MaxDisk: disk, Latency: lat}
}

func task(id string, cpu, mem, disk, dur int) *models.Task {
	return &models.Task{ID: id, CPU: cpu, Memory: mem, Disk: disk, Duration: dur}
}

// fleet mirrors the pilot's medium cloud tier: every resource fits on some
// node, but the large nodes trade disk for memory.
var fleet = []*models.Server{
	srv("c0", 24, 40960, 1100, 80),
	srv("c2", 32, 49152, 500, 100),
	srv("c4", 22, 36864, 1300, 90),
	srv("e0", 2, 768, 20, 5),
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name        string
		task        *models.Task
		schedulable bool
		exceeds     []string
		minEff      float64
	}{
		{"fits small node, fastest wins", task("a", 1, 512, 10, 1000), true, nil, 1005},
		{"fits one node exactly", task("b", 32, 49152, 500, 0), true, nil, 100},
		{"memory over every node", task("c", 24, 65536, 1600, 0), false, []string{Memory, Disk}, math.Inf(1)},
		{"cpu over every node", task("d", 40, 1, 1, 0), false, []string{CPU}, math.Inf(1)},
		{"joint only (pilot case)", task("e", 24, 49152, 1200, 0), false, nil, math.Inf(1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := Classify(tc.task, fleet)
			if f.Schedulable != tc.schedulable {
				t.Errorf("Schedulable = %v", f.Schedulable)
			}
			if !reflect.DeepEqual(f.Exceeds, tc.exceeds) {
				t.Errorf("Exceeds = %v, want %v", f.Exceeds, tc.exceeds)
			}
			if f.MinEffectiveMS != tc.minEff {
				t.Errorf("MinEffectiveMS = %v, want %v", f.MinEffectiveMS, tc.minEff)
			}
		})
	}
}

func TestTemplatesAreTopologicallyOrdered(t *testing.T) {
	for _, tmpl := range workflow.Templates() {
		seen := map[string]bool{}
		for _, s := range tmpl.Slots {
			for _, d := range s.DependsOn {
				if !seen[d] {
					t.Errorf("%s: slot %s depends on later slot %s", tmpl.Name, s.ID, d)
				}
			}
			seen[s.ID] = true
		}
	}
}

func TestJudgeWorkflow(t *testing.T) {
	fanOut, _ := workflow.TemplateByName("fan_out_in") // A -> {B,C,D} -> E
	timing := Timing{Interval: 2 * time.Second, Window: 30 * time.Second, Drain: 180 * time.Second}
	short := task("short", 1, 128, 0, 1000)  // 1005 ms on e0
	long := task("long", 1, 128, 0, 100000)  // 100005 ms
	huge := task("huge", 24, 65536, 1600, 1) // fits nowhere
	feas := map[string]TaskFeasibility{}
	for _, tk := range []*models.Task{short, long, huge} {
		feas[tk.ID] = Classify(tk, fleet)
	}
	mk := func(seq int, tasks ...*models.Task) generator.WorkflowDraw {
		return generator.WorkflowDraw{Seq: seq, Template: fanOut, Tasks: tasks}
	}

	// Critical path A + max(B,C,D) + E = 1005 + 100005 + 1005, submitted at 2 s.
	v := JudgeWorkflow(mk(1, short, short, long, short, short), feas, timing)
	if want := 2000.0 + 1005 + 100005 + 1005; v.EarliestFinishMS != want || v.Infeasible() {
		t.Errorf("parallel branch: EF = %v (want %v), infeasible = %v", v.EarliestFinishMS, want, v.Infeasible())
	}

	// Two long tasks in series exceed 210 s.
	v = JudgeWorkflow(mk(0, long, long, short, short, long), feas, timing)
	if !v.TimeInfeasible || v.HasUnschedulable {
		t.Errorf("series of long tasks: %+v", v)
	}

	// Submission time counts: 1 long task fits at seq 0 but not at seq 60.
	if v := JudgeWorkflow(mk(0, long, short, short, short, short), feas, timing); v.Infeasible() {
		t.Errorf("seq 0 should be feasible: %+v", v)
	}
	if v := JudgeWorkflow(mk(60, long, short, short, short, short), feas, timing); !v.TimeInfeasible {
		t.Errorf("seq 60 should be time-infeasible: %+v", v)
	}

	v = JudgeWorkflow(mk(0, short, huge, short, short, short), feas, timing)
	if !v.HasUnschedulable || v.TimeInfeasible || !math.IsInf(v.EarliestFinishMS, 1) {
		t.Errorf("unschedulable slot: %+v", v)
	}
}
