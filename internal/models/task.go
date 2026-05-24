// Package models defines the shared data types exchanged across the
// scheduler pipeline (Task, Server, Execution, TaskMessage, ResultMessage,
// TaskResult and the Workflow* types). All values are JSON-encoded when
// stored in Redis or carried over RabbitMQ.
package models

import "time"

// Task is a reusable task definition stored once in Redis as task:<id>.
// A single Task may back many Executions over the life of a simulation.
type Task struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	CPU  int    `json:"cpu"` // CPU cores required to run the task.
	// Memory is the working-set requirement, in megabytes (MB).
	Memory int `json:"memory"`
	// Disk is the scratch-space requirement, in gigabytes (GB).
	Disk int `json:"disk"`
	// Duration is the base (server-agnostic) execution time, in milliseconds.
	// Workers add the chosen server's Latency on top when simulating execution.
	Duration int `json:"duration"`
	// TargetTier is the dataset-derived hint (EDGE | FOG | CLOUD) describing
	// which tier the task is "best suited" for. The scheduler may or may not
	// honor it depending on the active algorithm.
	TargetTier string    `json:"target_tier"`
	CreatedAt  time.Time `json:"created_at"`
}
