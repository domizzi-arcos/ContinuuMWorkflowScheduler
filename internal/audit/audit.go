// Package audit checks, without running the emulator, which sampled tasks
// and replayed workflows can never complete in a given configuration. Two
// independent causes are reported:
//
//   - capacity: the task fits no server (no single server has enough CPU,
//     memory and disk at once, even when idle);
//   - time: even with every server idle and the fastest feasible server for
//     each task, the work cannot finish before the run's drain deadline.
//
// Both are policy-independent lower bounds: a workflow flagged here cannot
// complete under any placement policy.
package audit

import (
	"math"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/generator"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// Resource names used in TaskFeasibility.Exceeds.
const (
	CPU    = "cpu"
	Memory = "memory"
	Disk   = "disk"
)

// TaskFeasibility is the capacity verdict for one task template.
type TaskFeasibility struct {
	Task *models.Task
	// Schedulable is true when at least one server can hold the task alone.
	Schedulable bool
	// Exceeds lists the resources whose demand is above the maximum of that
	// resource over all servers. An unschedulable task with an empty Exceeds
	// fails only jointly: each resource fits somewhere, never all on one node.
	Exceeds []string
	// MinEffectiveMS is the smallest duration+latency over servers that fit
	// the task; +Inf when none does.
	MinEffectiveMS float64
}

// Classify returns the capacity verdict for task against servers.
func Classify(task *models.Task, servers []*models.Server) TaskFeasibility {
	f := TaskFeasibility{Task: task, MinEffectiveMS: math.Inf(1)}
	var maxCPU, maxMem, maxDisk int
	for _, s := range servers {
		maxCPU = max(maxCPU, s.MaxCPU)
		maxMem = max(maxMem, s.MaxMemory)
		maxDisk = max(maxDisk, s.MaxDisk)
		if task.CPU <= s.MaxCPU && task.Memory <= s.MaxMemory && task.Disk <= s.MaxDisk {
			f.Schedulable = true
			f.MinEffectiveMS = math.Min(f.MinEffectiveMS, float64(task.Duration+s.Latency))
		}
	}
	if task.CPU > maxCPU {
		f.Exceeds = append(f.Exceeds, CPU)
	}
	if task.Memory > maxMem {
		f.Exceeds = append(f.Exceeds, Memory)
	}
	if task.Disk > maxDisk {
		f.Exceeds = append(f.Exceeds, Disk)
	}
	return f
}

// Timing describes the run the workflows are judged against.
type Timing struct {
	// Interval between workflow submissions; workflow seq k is created at
	// k*Interval after generation starts.
	Interval time.Duration
	// Window is the generation window; Drain the deadline after it.
	Window, Drain time.Duration
}

// Deadline is the latest completion time, measured from generation start.
func (t Timing) Deadline() time.Duration { return t.Window + t.Drain }

// WorkflowVerdict is the audit result for one replayed workflow.
type WorkflowVerdict struct {
	Seq      int
	Template string
	// HasUnschedulable is true when some slot's task fits no server.
	HasUnschedulable bool
	// EarliestFinishMS is the lower bound on completion time from generation
	// start: submission time plus the critical path of minimum effective
	// durations. +Inf when HasUnschedulable.
	EarliestFinishMS float64
	// TimeInfeasible is true when the workflow is schedulable but
	// EarliestFinishMS exceeds the deadline.
	TimeInfeasible bool
}

// Infeasible reports whether the workflow cannot complete for any reason.
func (v WorkflowVerdict) Infeasible() bool { return v.HasUnschedulable || v.TimeInfeasible }

// JudgeWorkflow computes the verdict for draw using per-task feasibility.
// It ignores queueing, dispatch polling and result propagation, so it never
// flags a workflow that could actually complete.
func JudgeWorkflow(draw generator.WorkflowDraw, feas map[string]TaskFeasibility, t Timing) WorkflowVerdict {
	v := WorkflowVerdict{Seq: draw.Seq, Template: draw.Template.Name}
	finish := make(map[string]float64, len(draw.Template.Slots))
	// Template slots are listed in topological order (dependencies first).
	for i, slot := range draw.Template.Slots {
		f := feas[draw.Tasks[i].ID]
		if !f.Schedulable {
			v.HasUnschedulable = true
		}
		ready := 0.0
		for _, dep := range slot.DependsOn {
			ready = math.Max(ready, finish[dep])
		}
		finish[slot.ID] = ready + f.MinEffectiveMS
	}
	last := 0.0
	for _, ef := range finish {
		last = math.Max(last, ef)
	}
	v.EarliestFinishMS = float64(draw.Seq)*float64(t.Interval.Milliseconds()) + last
	v.TimeInfeasible = !v.HasUnschedulable && v.EarliestFinishMS > float64(t.Deadline().Milliseconds())
	return v
}
