package models

import "time"

// TaskResult stores the execution trace for a single task.
type TaskResult struct {
	ExecutionID         string    `json:"execution_id"`
	TaskID              string    `json:"task_id"`
	TaskName            string    `json:"task_name"`
	TargetTier          string    `json:"target_tier"`
	ServerID            string    `json:"server_id"`
	ServerType          string    `json:"server_type"`
	WorkflowID          string    `json:"workflow_id,omitempty"`
	Algorithm           string    `json:"algorithm"`
	StartTime           time.Time `json:"start_time"`
	EndTime             time.Time `json:"end_time"`
	DurationMS          int64     `json:"duration_ms"`
	EffectiveDurationMS int64     `json:"effective_duration_ms"`
	CostUSD             float64   `json:"cost_usd"`
	EnergyWh            float64   `json:"energy_wh"`
	DataflowPath        []string  `json:"dataflow_path"`
}
