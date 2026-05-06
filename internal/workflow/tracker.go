package workflow

import (
	"log/slog"

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
	slog.Info("tracker started", "component", "tracker")
	for {
		select {
		case execID, ok := <-t.completedExec:
			if !ok {
				return
			}
			if t.control != nil && !t.control.Wait(done) {
				slog.Info("tracker stopped", "component", "tracker")
				return
			}
			t.handleDone(execID, done)
		case <-done:
			slog.Info("tracker stopped", "component", "tracker")
			return
		}
	}
}

// bootstrap scans all BLOCKED executions on startup and promotes those whose
// dependencies are already DONE. This handles scheduler restarts mid-run.
func (t *Tracker) bootstrap(done <-chan struct{}) {
	executions, err := t.store.GetAllExecutions()
	if err != nil {
		slog.Error("bootstrap scan failed", "component", "tracker", "err", err)
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
			slog.Error("bootstrap promote failed", "component", "tracker", "execution_id", exec.ID, "err", err)
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
		slog.Info("bootstrap promoted executions to PENDING", "component", "tracker", "count", promoted)
	}
}

// handleDone resolves dependencies for the workflow that owns the just-completed
// execution, promoting any newly unblocked downstream executions.
func (t *Tracker) handleDone(execID string, done <-chan struct{}) {
	exec, err := t.store.GetExecution(execID)
	if err != nil {
		slog.Error("GetExecution failed", "component", "tracker", "execution_id", execID, "err", err)
		return
	}
	if exec.WorkflowID == "" {
		return
	}

	wi, err := t.store.GetWorkflowInstance(exec.WorkflowID)
	if err != nil {
		slog.Error("GetWorkflowInstance failed", "component", "tracker", "workflow_id", exec.WorkflowID, "err", err)
		return
	}

	tmpl, ok := TemplateByName(wi.TemplateName)
	if !ok {
		slog.Error("unknown template", "component", "tracker", "template", wi.TemplateName, "workflow_id", wi.ID)
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
			slog.Error("GetExecution downstream failed", "component", "tracker", "execution_id", downstreamExecID, "err", err)
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
			slog.Error("PromoteExecution failed", "component", "tracker", "execution_id", downstreamExecID, "err", err)
			continue
		}

		slog.Info("execution promoted to PENDING", "component", "tracker",
			"execution_id", downstreamExecID, "slot_id", slot.ID, "workflow_id", wi.ID, "priority", wi.Priority)

		select {
		case t.readyExec <- downstreamExecID:
		case <-done:
			return
		}
	}

	if allDone {
		if err := t.store.MarkWorkflowDone(wi.ID); err != nil {
			slog.Error("MarkWorkflowDone failed", "component", "tracker", "workflow_id", wi.ID, "err", err)
		} else {
			slog.Info("workflow completed", "component", "tracker",
				"workflow_id", wi.ID, "template", wi.TemplateName, "priority", wi.Priority)
		}
	}
}

func (t *Tracker) allDepsDone(depExecIDs []string) bool {
	done, err := t.store.AllExecutionsDone(depExecIDs)
	if err != nil {
		slog.Error("AllExecutionsDone failed", "component", "tracker", "err", err)
		return false
	}
	return done
}

func containsStr(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
