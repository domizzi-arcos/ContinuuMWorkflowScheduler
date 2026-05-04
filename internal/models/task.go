package models

import "time"

// Task is a reusable task definition stored in Redis.
type Task struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	CPU        int       `json:"cpu"`      // cores required
	Memory     int       `json:"memory"`   // MB required
	Disk       int       `json:"disk"`     // GB required
	Duration   int       `json:"duration"` // base execution time in ms
	TargetTier string    `json:"target_tier"`
	CreatedAt  time.Time `json:"created_at"`
}
