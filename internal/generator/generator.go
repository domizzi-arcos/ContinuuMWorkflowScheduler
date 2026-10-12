package generator

import (
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/control"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/workflow"
)

const workflowSubmissionInterval = 2 * time.Second

// SubmissionInterval is the fixed gap between workflow submissions; workflow
// k of a run is created k intervals after generation starts.
func SubmissionInterval() time.Duration { return workflowSubmissionInterval }

// Generator instantiates workflow instances from the three templates at a
// fixed rate and streams the entry execution IDs to the scheduler channel.
type Generator struct {
	store       *store.RedisStore
	tasks       []*models.Task
	templates   []models.WorkflowTemplate
	templateIdx int
	priorityMin int
	priorityMax int
	rng         *rand.Rand
	output      chan<- string
	control     *control.Controller
}

// New builds a Generator from the full task catalog returned by SeedTasks.
// seed=0 produces a non-deterministic run; any other value is reproducible.
func New(
	s *store.RedisStore,
	tasks []*models.Task,
	output chan<- string,
	ctrl *control.Controller,
	seed int64,
	priorityMin, priorityMax int,
) *Generator {
	if seed == 0 {
		seed = time.Now().UTC().UnixNano()
	}
	priorityMin, priorityMax = normalisePriority(priorityMin, priorityMax)

	return &Generator{
		store:       s,
		tasks:       tasks,
		templates:   workflow.Templates(),
		priorityMin: priorityMin,
		priorityMax: priorityMax,
		rng:         rand.New(rand.NewSource(seed)),
		output:      output,
		control:     ctrl,
	}
}

// Start instantiates workflow instances in a round-robin loop until stopped.
func (g *Generator) Start(done <-chan struct{}) {
	slog.Info("generator started", "component", "generator",
		"templates", len(g.templates), "tasks", len(g.tasks), "interval", workflowSubmissionInterval)

	created := 0
	for {
		if g.control != nil && !g.control.Wait(done) {
			slog.Info("generator stopped", "component", "generator", "workflows_created", created)
			return
		}

		if err := g.instantiateWorkflow(done); err != nil {
			slog.Error("instantiateWorkflow failed", "component", "generator", "err", err)
		} else {
			created++
		}

		if !g.sleep(done, workflowSubmissionInterval) {
			slog.Info("generator stopped", "component", "generator", "workflows_created", created)
			return
		}
	}
}

// instantiateWorkflow creates one concrete workflow run from the next template
// in rotation. All executions are stored upfront: entry slots as PENDING,
// downstream slots as BLOCKED. Entry execution IDs are sent to the scheduler.
func (g *Generator) instantiateWorkflow(done <-chan struct{}) error {
	draw, err := drawWorkflow(g.rng, g.templates, g.templateIdx, g.tasks, g.priorityMin, g.priorityMax)
	g.templateIdx++
	if err != nil {
		return err
	}
	tmpl, priority := draw.Template, draw.Priority
	taskIDs := make([]string, len(draw.Tasks))
	for i, task := range draw.Tasks {
		taskIDs[i] = task.ID
	}

	// Allocate execution IDs in a single pipeline call.
	execIDs, err := g.store.AllocateExecutionIDs(len(tmpl.Slots))
	if err != nil {
		return fmt.Errorf("allocate execution IDs: %w", err)
	}

	// Build the slot→execID mapping needed to translate template-level
	// DependsOn (slot names) into execution-level DependsOn (exec IDs).
	slotExecIDs := make(map[string]string, len(tmpl.Slots))
	for i, slot := range tmpl.Slots {
		slotExecIDs[slot.ID] = execIDs[i]
	}

	wfID := fmt.Sprintf("wf_%s_%d", tmpl.Name, time.Now().UnixNano())
	now := time.Now().UTC()

	// Create and store every execution for this workflow.
	for i, slot := range tmpl.Slots {
		deps := make([]string, len(slot.DependsOn))
		for j, depSlotID := range slot.DependsOn {
			deps[j] = slotExecIDs[depSlotID]
		}

		status := "BLOCKED"
		if len(deps) == 0 {
			status = "PENDING"
		}

		exec := &models.Execution{
			ID:         execIDs[i],
			TaskID:     taskIDs[i],
			Status:     status,
			WorkflowID: wfID,
			SlotID:     slot.ID,
			Priority:   priority,
			DependsOn:  deps,
			CreatedAt:  now,
		}
		if err := g.store.StoreExecution(exec); err != nil {
			return fmt.Errorf("store execution %s: %w", exec.ID, err)
		}
	}

	// Persist the workflow instance so the tracker can resolve it later.
	wi := &models.WorkflowInstance{
		ID:           wfID,
		TemplateName: tmpl.Name,
		Priority:     priority,
		Status:       "RUNNING",
		SlotExecIDs:  slotExecIDs,
		CreatedAt:    now,
	}
	if err := g.store.StoreWorkflowInstance(wi); err != nil {
		return fmt.Errorf("store workflow instance: %w", err)
	}

	// Deliver entry executions (PENDING, no deps) to the scheduler.
	for i, slot := range tmpl.Slots {
		if len(slot.DependsOn) == 0 {
			select {
			case g.output <- execIDs[i]:
			case <-done:
				return nil
			}
		}
	}

	slog.Debug("workflow instance created", "component", "generator",
		"wf_id", wfID, "template", tmpl.Name, "priority", priority, "slots", len(tmpl.Slots))
	return nil
}

// WorkflowDraw is the random part of one workflow instance: which template,
// which priority, and which task backs each slot (in template slot order).
type WorkflowDraw struct {
	Seq      int
	Template models.WorkflowTemplate
	Priority int
	Tasks    []*models.Task
}

// normalisePriority applies the clamping New has always applied: a
// non-positive minimum becomes 1 and an inverted range collapses to min.
func normalisePriority(min, max int) (int, int) {
	if min <= 0 {
		min = 1
	}
	if max < min {
		max = min
	}
	return min, max
}

// drawWorkflow consumes rng exactly as the generator always has, for the
// workflow at position templateIdx in the rotation:
//  1. one rng.Intn for the priority, only when prioMax > prioMin;
//  2. one rng.Intn(len(tasks)) per slot, in template slot order.
//
// prioMin/prioMax must already be normalised. Draw order is load-bearing:
// changing it changes every seeded workload.
func drawWorkflow(rng *rand.Rand, templates []models.WorkflowTemplate, templateIdx int, tasks []*models.Task, prioMin, prioMax int) (WorkflowDraw, error) {
	tmpl := templates[templateIdx%len(templates)]

	priority := prioMin
	if prioMax > prioMin {
		priority += rng.Intn(prioMax - prioMin + 1)
	}

	// Pick one task per slot from the full pool. The scheduler decides which
	// server (and tier) will execute each task based on resource fit alone.
	if len(tasks) == 0 {
		return WorkflowDraw{}, fmt.Errorf("no tasks seeded (template=%s)", tmpl.Name)
	}
	picked := make([]*models.Task, len(tmpl.Slots))
	for i := range tmpl.Slots {
		picked[i] = tasks[rng.Intn(len(tasks))]
	}
	return WorkflowDraw{Seq: templateIdx, Template: tmpl, Priority: priority, Tasks: picked}, nil
}

// ReplayWorkflows reproduces, without Redis or wall-clock time, the first n
// workflows a Generator built with the same tasks, seed and priority range
// would create. seed must be non-zero (seed 0 is time-based in New).
func ReplayWorkflows(tasks []*models.Task, seed int64, prioMin, prioMax, n int) ([]WorkflowDraw, error) {
	if seed == 0 {
		return nil, fmt.Errorf("seed 0 is non-deterministic and cannot be replayed")
	}
	prioMin, prioMax = normalisePriority(prioMin, prioMax)
	rng := rand.New(rand.NewSource(seed))
	templates := workflow.Templates()
	draws := make([]WorkflowDraw, 0, n)
	for i := 0; i < n; i++ {
		d, err := drawWorkflow(rng, templates, i, tasks, prioMin, prioMax)
		if err != nil {
			return nil, err
		}
		draws = append(draws, d)
	}
	return draws, nil
}

func (g *Generator) sleep(done <-chan struct{}, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	if g.control != nil {
		return g.control.Sleep(done, d)
	}
	timer := time.NewTimer(d)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-done:
		return false
	case <-timer.C:
		return true
	}
}
