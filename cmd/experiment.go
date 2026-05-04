package main

import (
	"fmt"
	"log"
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
		log.Printf("[Experiment] Invalid %s=%q, using %d", key, raw, fallback)
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

	log.Printf(
		"[Experiment] phase=%s elapsed=%s task_defs=%d executions=%d blocked=%d pending=%d queued=%d running=%d done=%d results=%d tier_completed={edge=%d fog=%d cloud=%d}",
		phase,
		elapsed,
		snapshot.TaskDefinitions,
		snapshot.Executions,
		snapshot.BlockedExecutions,
		snapshot.PendingExecutions,
		snapshot.QueuedExecutions,
		snapshot.RunningExecutions,
		snapshot.DoneExecutions,
		snapshot.Results,
		edge.CompletedTasks,
		fog.CompletedTasks,
		cloud.CompletedTasks,
	)
}

func logExperimentSummary(s *store.RedisStore, activeWall time.Duration, reason string) {
	snapshot, err := collectExperimentSnapshot(s)
	if err != nil {
		log.Printf("[Experiment] Summary unavailable: %v", err)
		return
	}

	results, err := s.GetAllTaskResults()
	if err != nil {
		log.Printf("[Experiment] Could not reload task results for summary: %v", err)
		results = nil
	}

	wall := activeWall
	wallSeconds := wall.Seconds()
	if wallSeconds <= 0 {
		wallSeconds = 1
	}

	log.Printf(
		"[Experiment] SUMMARY reason=%s wall=%s task_defs=%d executions=%d completed=%d in_flight=%d results=%d throughput={executions=%.2f/s}",
		reason,
		wall.Round(time.Second),
		snapshot.TaskDefinitions,
		snapshot.Executions,
		snapshot.DoneExecutions,
		snapshot.PendingExecutions+snapshot.QueuedExecutions+snapshot.RunningExecutions,
		snapshot.Results,
		float64(snapshot.Results)/wallSeconds,
	)

	log.Printf(
		"[Experiment] EXECUTIONS pending=%d queued=%d running=%d done=%d completion=%.1f%%",
		snapshot.PendingExecutions,
		snapshot.QueuedExecutions,
		snapshot.RunningExecutions,
		snapshot.DoneExecutions,
		completionRate(snapshot),
	)

	for _, tierName := range []string{"EDGE", "FOG", "CLOUD"} {
		tier := snapshot.Tiers[tierName]
		log.Printf(
			"[Experiment] TIER %s nodes=%d completed=%d active=%d cpu=%d/%d mem=%d/%dMB disk=%d/%dGB",
			tierName,
			tier.Nodes,
			tier.CompletedTasks,
			tier.ActiveTasks,
			tier.UsedCPU,
			tier.MaxCPU,
			tier.UsedMemory,
			tier.MaxMemory,
			tier.UsedDisk,
			tier.MaxDisk,
		)
	}

	durations := computeTaskDurations(results)
	if len(durations) > 0 {
		log.Printf(
			"[Experiment] LATENCY count=%d min=%s p50=%s p95=%s avg=%s max=%s",
			len(durations),
			formatDuration(durations[0]),
			formatDuration(percentileDuration(durations, 0.50)),
			formatDuration(percentileDuration(durations, 0.95)),
			formatDuration(averageDuration(durations)),
			formatDuration(durations[len(durations)-1]),
		)
	}

	if len(snapshot.TopServers) > 0 {
		parts := make([]string, 0, len(snapshot.TopServers))
		for _, server := range snapshot.TopServers {
			parts = append(parts, fmt.Sprintf("%s/%s(%d done)", server.Type, server.ID, server.CompletedTasks))
		}
		log.Printf("[Experiment] TOP_SERVERS %s", strings.Join(parts, ", "))
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
