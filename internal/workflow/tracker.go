package workflow

import (
	"log"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/control"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

// Tracker watches for completed executions and promotes any workflow execution
// whose upstream dependencies are all DONE from BLOCKED to PENDING, then
// forwards the newly unblocked execution ID to the scheduler channel.
type Tracker struct {
	store         *store.RedisStore
	completedExec <-chan string
	readyExec     chan<- string
	control       *control.Controller
}

func NewTracker(s *store.RedisStore, completed <-chan string, ready chan<- string, ctrl *control.Controller) *Tracker {
	return &Tracker{
		store:         s,
		completedExec: completed,
		readyExec:     ready,
		control:       ctrl,
	}
}

// Start runs until done is closed. It first bootstraps any BLOCKED executions
// that may already have all dependencies satisfied (handles restarts), then
// enters the event loop.
func (t *Tracker) Start(done <-chan struct{}) {
	t.bootstrap(done)
	log.Println("[Tracker] Started — watching for dependency resolution")
	for {
		select {
		case execID, ok := <-t.completedExec:
			if !ok {
				return
			}
			if t.control != nil && !t.control.Wait(done) {
				log.Println("[Tracker] Stopped")
				return
			}
			t.handleDone(execID, done)
		case <-done:
			log.Println("[Tracker] Stopped")
			return
		}
	}
}

// bootstrap scans all BLOCKED executions on startup and promotes those whose
// dependencies are already DONE. This handles scheduler restarts mid-run.
func (t *Tracker) bootstrap(done <-chan struct{}) {
	executions, err := t.store.GetAllExecutions()
	if err != nil {
		log.Printf("[Tracker] Bootstrap scan failed: %v", err)
		return
	}
	promoted := 0
	for _, exec := range executions {
		if exec.Status != "BLOCKED" || len(exec.DependsOn) == 0 {
			continue
		}
		if !t.allDepsDone(exec.DependsOn) {
			continue
		}
		if err := t.store.PromoteExecution(exec.ID); err != nil {
			log.Printf("[Tracker] Bootstrap promote %s: %v", exec.ID, err)
			continue
		}
		select {
		case t.readyExec <- exec.ID:
			promoted++
		case <-done:
			return
		}
	}
	if promoted > 0 {
		log.Printf("[Tracker] Bootstrap promoted %d executions to PENDING", promoted)
	}
}

// handleDone resolves dependencies for the workflow that owns the just-completed
// execution, promoting any newly unblocked downstream executions.
func (t *Tracker) handleDone(execID string, done <-chan struct{}) {
	exec, err := t.store.GetExecution(execID)
	if err != nil {
		log.Printf("[Tracker] GetExecution %s: %v", execID, err)
		return
	}
	if exec.WorkflowID == "" {
		return
	}

	wi, err := t.store.GetWorkflowInstance(exec.WorkflowID)
	if err != nil {
		log.Printf("[Tracker] GetWorkflowInstance %s: %v", exec.WorkflowID, err)
		return
	}

	tmpl, ok := TemplateByName(wi.TemplateName)
	if !ok {
		log.Printf("[Tracker] Unknown template %q for workflow %s", wi.TemplateName, wi.ID)
		return
	}

	allDone := true
	for _, slot := range tmpl.Slots {
		// Only process slots that directly depend on the slot that just finished.
		if !containsStr(slot.DependsOn, exec.SlotID) {
			// Still need to check for overall workflow completion.
			slotExecID := wi.SlotExecIDs[slot.ID]
			downstream, err := t.store.GetExecution(slotExecID)
			if err != nil || downstream.Status != "DONE" {
				allDone = false
			}
			continue
		}

		downstreamExecID := wi.SlotExecIDs[slot.ID]
		downstream, err := t.store.GetExecution(downstreamExecID)
		if err != nil {
			log.Printf("[Tracker] GetExecution downstream %s: %v", downstreamExecID, err)
			allDone = false
			continue
		}

		if downstream.Status != "DONE" {
			allDone = false
		}

		if downstream.Status != "BLOCKED" {
			continue
		}

		if !t.allDepsDone(downstream.DependsOn) {
			continue
		}

		if err := t.store.PromoteExecution(downstreamExecID); err != nil {
			log.Printf("[Tracker] PromoteExecution %s: %v", downstreamExecID, err)
			continue
		}

		log.Printf("[Tracker] Promoted exec=%-12s slot=%s wf=%s priority=%d → PENDING",
			downstreamExecID, slot.ID, wi.ID, wi.Priority)

		select {
		case t.readyExec <- downstreamExecID:
		case <-done:
			return
		}
	}

	if allDone {
		if err := t.store.MarkWorkflowDone(wi.ID); err != nil {
			log.Printf("[Tracker] MarkWorkflowDone %s: %v", wi.ID, err)
		} else {
			log.Printf("[Tracker] Workflow DONE wf=%-30s template=%s priority=%d",
				wi.ID, wi.TemplateName, wi.Priority)
		}
	}
}

func (t *Tracker) allDepsDone(depExecIDs []string) bool {
	for _, depID := range depExecIDs {
		dep, err := t.store.GetExecution(depID)
		if err != nil || dep.Status != "DONE" {
			return false
		}
	}
	return true
}

func containsStr(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
