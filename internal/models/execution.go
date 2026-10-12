package models

import "time"

// Execution is one runnable instance of a stored task definition.
// Status lifecycle: BLOCKED → PENDING → QUEUED → RUNNING → DONE
// BLOCKED executions belong to a workflow and wait for upstream dependencies.
// The workflow tracker promotes them to PENDING once all DependsOn are DONE.
type Execution struct {
	ID             string     `json:"id"`
	TaskID         string     `json:"task_id"`
	Status         string     `json:"status"` // BLOCKED | PENDING | QUEUED | RUNNING | DONE
	AssignedServer string     `json:"assigned_server,omitempty"`
	WorkflowID     string     `json:"workflow_id,omitempty"`
	SlotID         string     `json:"slot_id,omitempty"`
	Priority       int        `json:"priority,omitempty"`
	DependsOn      []string   `json:"depends_on,omitempty"` // execution IDs that must be DONE first
	CreatedAt      time.Time  `json:"created_at"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	// ReadyAt is when the execution became PENDING: at creation for entry
	// slots, on promotion from BLOCKED otherwise.
	ReadyAt *time.Time `json:"ready_at,omitempty"`
	// DispatchedAt is when the scheduler assigned a server and queued the
	// execution; cleared if the assignment is reverted.
	DispatchedAt *time.Time `json:"dispatched_at,omitempty"`
}
