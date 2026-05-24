package models

// Server represents an Edge, Fog, or Cloud node with its static capacity,
// live resource usage counters, and per-tier cost/energy parameters.
// Servers are stored in Redis under server:<id> and mutated under
// optimistic-concurrency WATCH/MULTI/EXEC by store.RedisStore.
type Server struct {
	ID string `json:"id"`
	// Type is the tier classification used to route the server's queue:
	// EDGE | FOG | CLOUD.
	Type      string `json:"type"`
	MaxCPU    int    `json:"max_cpu"`
	MaxMemory int    `json:"max_memory"` // total memory, in megabytes (MB).
	MaxDisk   int    `json:"max_disk"`   // total disk, in gigabytes (GB).
	// UsedCPU / UsedMemory / UsedDisk are the live reservations held by tasks
	// currently QUEUED or RUNNING on this server. They are released when the
	// worker reports completion.
	UsedCPU    int `json:"used_cpu"`
	UsedMemory int `json:"used_memory"`
	UsedDisk   int `json:"used_disk"`
	// Latency is the simulated WAN round-trip from the client/generator to
	// this server, in milliseconds. Workers add it to task.Duration to
	// produce the EffectiveDuration of each execution.
	Latency int `json:"latency"`
	// CostPerHour is the billed price per server-hour, in USD. Used by the
	// worker to derive per-task CostUSD from effective wall time.
	CostPerHour float64 `json:"cost_per_hour"`
	// PowerWatts is the average draw used to estimate per-task EnergyWh
	// (idle + active blended into a single representative number).
	PowerWatts     int `json:"power_watts"`
	ActiveTasks    int `json:"active_tasks"`
	CompletedTasks int `json:"completed_tasks"`
}

// HasCapacity reports whether the server can host an additional task that
// requires cpu cores, memory MB, and disk GB given current reservations.
func (s *Server) HasCapacity(cpu, memory, disk int) bool {
	return s.UsedCPU+cpu <= s.MaxCPU &&
		s.UsedMemory+memory <= s.MaxMemory &&
		s.UsedDisk+disk <= s.MaxDisk
}

// FreeCapacity returns a coarse "how empty is this server" score that
// scheduling algorithms can use to bias placement. Memory is scaled by 1/100
// so it does not dominate the sum; higher means more free, lower means more
// packed.
func (s *Server) FreeCapacity() int {
	return (s.MaxCPU - s.UsedCPU) + (s.MaxMemory-s.UsedMemory)/100 + (s.MaxDisk - s.UsedDisk)
}
