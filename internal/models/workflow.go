package models

import "time"

// WorkflowSlot is one task node inside a workflow template.
type WorkflowSlot struct {
	ID        string   // logical name within the template, e.g. "A", "B"
	DependsOn []string // slot IDs that must complete before this slot can start
}

// WorkflowTemplate is a static DAG definition. Templates are hardcoded in the
// workflow package and are never stored in Redis.
type WorkflowTemplate struct {
	Name  string
	Slots []WorkflowSlot
}

// WorkflowInstance is a concrete run of a WorkflowTemplate. It maps each slot
// to the execution ID that was created for it and is persisted under
// "workflow:<id>" in Redis.
type WorkflowInstance struct {
	ID           string            `json:"id"`
	TemplateName string            `json:"template_name"`
	Priority     int               `json:"priority"`
	Status       string            `json:"status"`        // RUNNING | DONE
	SlotExecIDs  map[string]string `json:"slot_exec_ids"` // slot ID → execution ID
	CreatedAt    time.Time         `json:"created_at"`
	MakespanMS   int64             `json:"makespan_ms,omitempty"` // wall time first→last task
}
