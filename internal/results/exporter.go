package results

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

// ExperimentResult is the top-level structure written to the JSON output file.
// The schema is stable — downstream notebooks or scripts loading
// `results/*.json` rely on the exact field names below.
type ExperimentResult struct {
	Meta      ExperimentMeta       `json:"meta"`
	Summary   ExperimentSummary    `json:"summary"`
	Tasks     []*models.TaskResult `json:"task_results"`
	Workflows []*WorkflowSummary   `json:"workflow_results"`
	Servers   []*ServerSnapshot    `json:"servers"`
}

// ExperimentMeta records the configuration that produced a result file:
// which algorithm ran, when, for how long, and over how many servers per
// tier. TOPSISWeights is only populated when the algorithm is "topsis".
type ExperimentMeta struct {
	Algorithm     string               `json:"algorithm"`
	Timestamp     time.Time            `json:"timestamp"`
	DurationS     int                  `json:"duration_seconds"`
	TaskCount     int                  `json:"task_count"`
	NumServers    map[string]int       `json:"num_servers"`
	TOPSISWeights config.TOPSISWeights `json:"topsis_weights,omitempty"`
}

// ExperimentSummary holds the aggregate metrics computed at end-of-run by
// buildSummary. Throughput is tasks-per-minute over wall time (not active
// time); makespan stats only cover workflows whose Status is DONE.
type ExperimentSummary struct {
	TotalTasksCompleted     int                    `json:"total_tasks_completed"`
	TotalWorkflowsCompleted int                    `json:"total_workflows_completed"`
	AvgMakespanMS           float64                `json:"avg_workflow_makespan_ms"`
	P50MakespanMS           int64                  `json:"p50_workflow_makespan_ms"`
	P95MakespanMS           int64                  `json:"p95_workflow_makespan_ms"`
	TotalCostUSD            float64                `json:"total_cost_usd"`
	TotalEnergyWh           float64                `json:"total_energy_wh"`
	ThroughputPerMin        float64                `json:"throughput_tasks_per_min"`
	AvgEffectiveDurationMS  float64                `json:"avg_effective_duration_ms"`
	ByTier                  map[string]TierSummary `json:"by_tier"`
}

// TierSummary aggregates per-tier (EDGE, FOG, CLOUD) totals. AvgDurationMS
// uses tasks placed on this tier, not all tasks system-wide.
type TierSummary struct {
	TasksCompleted int     `json:"tasks_completed"`
	AvgDurationMS  float64 `json:"avg_effective_duration_ms"`
	TotalCostUSD   float64 `json:"total_cost_usd"`
	TotalEnergyWh  float64 `json:"total_energy_wh"`
}

// WorkflowSummary is the trimmed view of a WorkflowInstance kept in the
// exported JSON — the full instance with its slot→exec map stays in Redis.
type WorkflowSummary struct {
	ID           string    `json:"id"`
	TemplateName string    `json:"template_name"`
	Priority     int       `json:"priority"`
	Status       string    `json:"status"`
	MakespanMS   int64     `json:"makespan_ms"`
	CreatedAt    time.Time `json:"created_at"`
}

// ServerSnapshot captures end-of-run counters per server so the analysis
// scripts can compute load-balance metrics (e.g. tasks-per-server stdev).
// ActiveTasks should be 0 after a clean drain but is reported anyway.
type ServerSnapshot struct {
	ID             string `json:"id"`
	ServerType     string `json:"server_type"`
	CompletedTasks int    `json:"completed_tasks"`
	ActiveTasks    int    `json:"active_tasks"`
}

// Export reads all task results, workflow instances, and server state from
// Redis, builds the aggregate summary, and writes the combined JSON to a
// timestamped file under outputDir. The filename encodes the algorithm and
// per-tier server counts so the grid runner can scan a results directory
// without re-opening every file.
func Export(s *store.RedisStore, cfg *config.Config, wallDuration time.Duration, outputDir string) {
	tasks, err := s.GetAllTaskResults()
	if err != nil {
		slog.Error("failed to load task results", "component", "exporter", "err", err)
		return
	}
	workflows, err := s.GetAllWorkflowInstances()
	if err != nil {
		slog.Error("failed to load workflow instances", "component", "exporter", "err", err)
		return
	}
	servers, err := s.GetAllServers()
	if err != nil {
		slog.Error("failed to load servers", "component", "exporter", "err", err)
		return
	}

	numServers := map[string]int{"EDGE": 0, "FOG": 0, "CLOUD": 0}
	serverSnapshots := make([]*ServerSnapshot, 0, len(servers))
	for _, srv := range servers {
		numServers[srv.Type]++
		serverSnapshots = append(serverSnapshots, &ServerSnapshot{
			ID:             srv.ID,
			ServerType:     srv.Type,
			CompletedTasks: srv.CompletedTasks,
			ActiveTasks:    srv.ActiveTasks,
		})
	}

	meta := ExperimentMeta{
		Algorithm:     cfg.Scheduler.Algorithm,
		Timestamp:     time.Now().UTC(),
		DurationS:     cfg.Experiment.DurationSeconds,
		TaskCount:     cfg.Generator.TaskCount,
		NumServers:    numServers,
		TOPSISWeights: cfg.Scheduler.TOPSISWeights,
	}

	summary := buildSummary(tasks, workflows, wallDuration)

	wfSummaries := make([]*WorkflowSummary, 0, len(workflows))
	for _, wi := range workflows {
		wfSummaries = append(wfSummaries, &WorkflowSummary{
			ID:           wi.ID,
			TemplateName: wi.TemplateName,
			Priority:     wi.Priority,
			Status:       wi.Status,
			MakespanMS:   wi.MakespanMS,
			CreatedAt:    wi.CreatedAt,
		})
	}

	result := ExperimentResult{
		Meta:      meta,
		Summary:   summary,
		Tasks:     tasks,
		Workflows: wfSummaries,
		Servers:   serverSnapshots,
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		slog.Error("cannot create output directory", "component", "exporter", "err", err)
		return
	}

	ts := time.Now().UTC().Format("20060102_150405")
	filename := fmt.Sprintf("%s/%s_edge%d_fog%d_cloud%d_%s.json",
		outputDir,
		cfg.Scheduler.Algorithm,
		numServers["EDGE"], numServers["FOG"], numServers["CLOUD"],
		ts,
	)

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		slog.Error("JSON marshal failed", "component", "exporter", "err", err)
		return
	}
	if err := os.WriteFile(filename, data, 0644); err != nil {
		slog.Error("write failed", "component", "exporter", "err", err)
		return
	}
	slog.Info("results exported", "component", "exporter", "tasks", len(tasks), "workflows", len(wfSummaries), "file", filename)
}

// buildSummary aggregates per-tier and overall metrics from the task list,
// plus makespan percentiles over the completed workflows. Throughput uses
// wallMin = max(wall.Minutes(), 1) so a very short run does not divide by
// near-zero. p50/p95 use the nearest-rank method on the sorted makespans.
func buildSummary(tasks []*models.TaskResult, workflows []*models.WorkflowInstance, wall time.Duration) ExperimentSummary {
	byTier := map[string]*TierSummary{
		"EDGE":  {},
		"FOG":   {},
		"CLOUD": {},
	}
	var totalCost, totalEnergy, totalDuration float64
	for _, t := range tasks {
		totalCost += t.CostUSD
		totalEnergy += t.EnergyWh
		totalDuration += float64(t.EffectiveDurationMS)
		tier := byTier[t.ServerType]
		if tier == nil {
			tier = &TierSummary{}
			byTier[t.ServerType] = tier
		}
		tier.TasksCompleted++
		tier.TotalCostUSD += t.CostUSD
		tier.TotalEnergyWh += t.EnergyWh
		tier.AvgDurationMS += float64(t.EffectiveDurationMS)
	}
	for _, ts := range byTier {
		if ts.TasksCompleted > 0 {
			ts.AvgDurationMS /= float64(ts.TasksCompleted)
		}
	}

	// Workflow makespan percentiles
	var makespans []int64
	completedWF := 0
	for _, wf := range workflows {
		if wf.Status == "DONE" {
			completedWF++
			if wf.MakespanMS > 0 {
				makespans = append(makespans, wf.MakespanMS)
			}
		}
	}
	sort.Slice(makespans, func(i, j int) bool { return makespans[i] < makespans[j] })

	var avgMakespan float64
	var p50, p95 int64
	if len(makespans) > 0 {
		var sum int64
		for _, m := range makespans {
			sum += m
		}
		avgMakespan = float64(sum) / float64(len(makespans))
		p50 = makespans[int(float64(len(makespans)-1)*0.50)]
		p95 = makespans[int(float64(len(makespans)-1)*0.95)]
	}

	wallMin := wall.Minutes()
	if wallMin <= 0 {
		wallMin = 1
	}

	tierOut := map[string]TierSummary{}
	for k, v := range byTier {
		tierOut[k] = *v
	}

	avg := 0.0
	if len(tasks) > 0 {
		avg = totalDuration / float64(len(tasks))
	}

	return ExperimentSummary{
		TotalTasksCompleted:     len(tasks),
		TotalWorkflowsCompleted: completedWF,
		AvgMakespanMS:           avgMakespan,
		P50MakespanMS:           p50,
		P95MakespanMS:           p95,
		TotalCostUSD:            totalCost,
		TotalEnergyWh:           totalEnergy,
		ThroughputPerMin:        float64(len(tasks)) / wallMin,
		AvgEffectiveDurationMS:  avg,
		ByTier:                  tierOut,
	}
}
