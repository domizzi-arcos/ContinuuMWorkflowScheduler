package main

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

type tierSnapshot struct {
	Nodes          int
	ActiveTasks    int
	CompletedTasks int
	UsedCPU        int
	MaxCPU         int
	UsedMemory     int
	MaxMemory      int
	UsedDisk       int
	MaxDisk        int
}

type serverSnapshot struct {
	ID             string
	Type           string
	CompletedTasks int
	ActiveTasks    int
}

type experimentSnapshot struct {
	TaskDefinitions    int
	Executions         int
	BlockedExecutions  int
	PendingExecutions  int
	QueuedExecutions   int
	RunningExecutions  int
	DoneExecutions     int
	Results            int
	Tiers              map[string]tierSnapshot
	TopServers         []serverSnapshot
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		slog.Warn("invalid env var, using fallback", "component", "experiment", "key", key, "value", raw, "fallback", fallback)
		return fallback
	}
	return value
}

func collectExperimentSnapshot(s *store.RedisStore) (*experimentSnapshot, error) {
	tasks, err := s.GetAllTasks()
	if err != nil {
		return nil, fmt.Errorf("get tasks: %w", err)
	}
	executions, err := s.GetAllExecutions()
	if err != nil {
		return nil, fmt.Errorf("get executions: %w", err)
	}
	servers, err := s.GetAllServers()
	if err != nil {
		return nil, fmt.Errorf("get servers: %w", err)
	}
	results, err := s.GetAllTaskResults()
	if err != nil {
		return nil, fmt.Errorf("get results: %w", err)
	}

	snapshot := &experimentSnapshot{
		TaskDefinitions: len(tasks),
		Executions:      len(executions),
		Results:         len(results),
		Tiers: map[string]tierSnapshot{
			"EDGE":  {},
			"FOG":   {},
			"CLOUD": {},
		},
	}

	for _, execution := range executions {
		switch execution.Status {
		case "BLOCKED":
			snapshot.BlockedExecutions++
		case "PENDING":
			snapshot.PendingExecutions++
		case "QUEUED":
			snapshot.QueuedExecutions++
		case "RUNNING":
			snapshot.RunningExecutions++
		case "DONE":
			snapshot.DoneExecutions++
		}
	}

	serverRank := make([]serverSnapshot, 0, len(servers))
	for _, srv := range servers {
		tier := snapshot.Tiers[srv.Type]
		tier.Nodes++
		tier.ActiveTasks += srv.ActiveTasks
		tier.CompletedTasks += srv.CompletedTasks
		tier.UsedCPU += srv.UsedCPU
		tier.MaxCPU += srv.MaxCPU
		tier.UsedMemory += srv.UsedMemory
		tier.MaxMemory += srv.MaxMemory
		tier.UsedDisk += srv.UsedDisk
		tier.MaxDisk += srv.MaxDisk
		snapshot.Tiers[srv.Type] = tier

		serverRank = append(serverRank, serverSnapshot{
			ID:             srv.ID,
			Type:           srv.Type,
			CompletedTasks: srv.CompletedTasks,
			ActiveTasks:    srv.ActiveTasks,
		})
	}

	sort.Slice(serverRank, func(i, j int) bool {
		if serverRank[i].CompletedTasks != serverRank[j].CompletedTasks {
			return serverRank[i].CompletedTasks > serverRank[j].CompletedTasks
		}
		if serverRank[i].ActiveTasks != serverRank[j].ActiveTasks {
			return serverRank[i].ActiveTasks > serverRank[j].ActiveTasks
		}
		return serverRank[i].ID < serverRank[j].ID
	})
	if len(serverRank) > 5 {
		serverRank = serverRank[:5]
	}
	snapshot.TopServers = serverRank

	return snapshot, nil
}

func experimentDrained(snapshot *experimentSnapshot) bool {
	return snapshot.BlockedExecutions == 0 &&
		snapshot.PendingExecutions == 0 &&
		snapshot.QueuedExecutions == 0 &&
		snapshot.RunningExecutions == 0 &&
		snapshot.Results >= snapshot.DoneExecutions
}

func logExperimentProgress(elapsed time.Duration, phase string, snapshot *experimentSnapshot) {
	elapsed = elapsed.Round(time.Second)
	edge := snapshot.Tiers["EDGE"]
	fog := snapshot.Tiers["FOG"]
	cloud := snapshot.Tiers["CLOUD"]

	slog.Info("experiment progress", "component", "experiment",
		"phase", phase, "elapsed", elapsed,
		"task_defs", snapshot.TaskDefinitions,
		"executions", snapshot.Executions,
		"blocked", snapshot.BlockedExecutions,
		"pending", snapshot.PendingExecutions,
		"queued", snapshot.QueuedExecutions,
		"running", snapshot.RunningExecutions,
		"done", snapshot.DoneExecutions,
		"results", snapshot.Results,
		"edge_completed", edge.CompletedTasks,
		"fog_completed", fog.CompletedTasks,
		"cloud_completed", cloud.CompletedTasks,
	)
}

func logExperimentSummary(s *store.RedisStore, activeWall time.Duration, reason string) {
	snapshot, err := collectExperimentSnapshot(s)
	if err != nil {
		slog.Error("summary unavailable", "component", "experiment", "err", err)
		return
	}

	results, err := s.GetAllTaskResults()
	if err != nil {
		slog.Error("could not reload task results for summary", "component", "experiment", "err", err)
		results = nil
	}

	wall := activeWall
	wallSeconds := wall.Seconds()
	if wallSeconds <= 0 {
		wallSeconds = 1
	}

	slog.Info("experiment summary", "component", "experiment",
		"reason", reason,
		"wall", wall.Round(time.Second),
		"task_defs", snapshot.TaskDefinitions,
		"executions", snapshot.Executions,
		"completed", snapshot.DoneExecutions,
		"in_flight", snapshot.PendingExecutions+snapshot.QueuedExecutions+snapshot.RunningExecutions,
		"results", snapshot.Results,
		"throughput_per_s", fmt.Sprintf("%.2f", float64(snapshot.Results)/wallSeconds),
	)

	slog.Info("execution breakdown", "component", "experiment",
		"pending", snapshot.PendingExecutions,
		"queued", snapshot.QueuedExecutions,
		"running", snapshot.RunningExecutions,
		"done", snapshot.DoneExecutions,
		"completion_pct", fmt.Sprintf("%.1f", completionRate(snapshot)),
	)

	for _, tierName := range []string{"EDGE", "FOG", "CLOUD"} {
		tier := snapshot.Tiers[tierName]
		slog.Info("tier stats", "component", "experiment",
			"tier", tierName,
			"nodes", tier.Nodes,
			"completed", tier.CompletedTasks,
			"active", tier.ActiveTasks,
			"cpu", fmt.Sprintf("%d/%d", tier.UsedCPU, tier.MaxCPU),
			"mem_mb", fmt.Sprintf("%d/%d", tier.UsedMemory, tier.MaxMemory),
			"disk_gb", fmt.Sprintf("%d/%d", tier.UsedDisk, tier.MaxDisk),
		)
	}

	durations := computeTaskDurations(results)
	if len(durations) > 0 {
		slog.Info("latency stats", "component", "experiment",
			"count", len(durations),
			"min", formatDuration(durations[0]),
			"p50", formatDuration(percentileDuration(durations, 0.50)),
			"p95", formatDuration(percentileDuration(durations, 0.95)),
			"avg", formatDuration(averageDuration(durations)),
			"max", formatDuration(durations[len(durations)-1]),
		)
	}

	if len(snapshot.TopServers) > 0 {
		parts := make([]string, 0, len(snapshot.TopServers))
		for _, server := range snapshot.TopServers {
			parts = append(parts, fmt.Sprintf("%s/%s(%d done)", server.Type, server.ID, server.CompletedTasks))
		}
		slog.Info("top servers", "component", "experiment", "servers", strings.Join(parts, ", "))
	}
}

func completionRate(snapshot *experimentSnapshot) float64 {
	if snapshot.Executions == 0 {
		return 0
	}
	return float64(snapshot.DoneExecutions) * 100 / float64(snapshot.Executions)
}

func computeTaskDurations(results []*models.TaskResult) []time.Duration {
	if len(results) == 0 {
		return nil
	}
	durations := make([]time.Duration, 0, len(results))
	for _, result := range results {
		if result == nil {
			continue
		}
		duration := time.Duration(result.EffectiveDurationMS) * time.Millisecond
		if duration > 0 {
			durations = append(durations, duration)
		}
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	return durations
}

func percentileDuration(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	if len(values) == 1 {
		return values[0]
	}
	index := int(float64(len(values)-1) * p)
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func averageDuration(values []time.Duration) time.Duration {
	if len(values) == 0 {
		return 0
	}
	var total time.Duration
	for _, value := range values {
		total += value
	}
	return total / time.Duration(len(values))
}

func formatDuration(value time.Duration) string {
	if value <= 0 {
		return "0s"
	}
	return value.Round(time.Millisecond).String()
}
