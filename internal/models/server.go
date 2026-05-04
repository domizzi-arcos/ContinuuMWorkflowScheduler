package models

// Server represents an Edge, Fog, or Cloud node.
type Server struct {
	ID             string  `json:"id"`
	Type           string  `json:"type"` // EDGE | FOG | CLOUD
	MaxCPU         int     `json:"max_cpu"`
	MaxMemory      int     `json:"max_memory"`  // MB
	MaxDisk        int     `json:"max_disk"`    // GB
	UsedCPU        int     `json:"used_cpu"`
	UsedMemory     int     `json:"used_memory"`
	UsedDisk       int     `json:"used_disk"`
	Latency        int     `json:"latency"`      // WAN latency in ms
	CostPerHour    float64 `json:"cost_per_hour"` // USD per server-hour
	PowerWatts     int     `json:"power_watts"`   // idle+active power draw in W
	ActiveTasks    int     `json:"active_tasks"`
	CompletedTasks int     `json:"completed_tasks"`
}

// HasCapacity returns true if the server can accommodate the given resource requirements.
func (s *Server) HasCapacity(cpu, memory, disk int) bool {
	return s.UsedCPU+cpu <= s.MaxCPU &&
		s.UsedMemory+memory <= s.MaxMemory &&
		s.UsedDisk+disk <= s.MaxDisk
}

// FreeCapacity returns the sum of free resources as a rough score (lower = more packed).
func (s *Server) FreeCapacity() int {
	return (s.MaxCPU - s.UsedCPU) + (s.MaxMemory-s.UsedMemory)/100 + (s.MaxDisk - s.UsedDisk)
}
