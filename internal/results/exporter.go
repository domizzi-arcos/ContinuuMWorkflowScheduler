package results

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

// ExperimentResult is the top-level structure written to the JSON output file.
type ExperimentResult struct {
	Meta      ExperimentMeta       `json:"meta"`
	Summary   ExperimentSummary    `json:"summary"`
	Tasks     []*models.TaskResult `json:"task_results"`
	Workflows []*WorkflowSummary   `json:"workflow_results"`
}

type ExperimentMeta struct {
	Algorithm     string            `json:"algorithm"`
	Timestamp     time.Time         `json:"timestamp"`
	DurationS     int               `json:"duration_seconds"`
	TaskCount     int               `json:"task_count"`
	NumServers    map[string]int    `json:"num_servers"`
	TOPSISWeights config.TOPSISWeights `json:"topsis_weights,omitempty"`
}

type ExperimentSummary struct {
	TotalTasksCompleted     int            `json:"total_tasks_completed"`
	TotalWorkflowsCompleted int            `json:"total_workflows_completed"`
	AvgMakespanMS           float64        `json:"avg_workflow_makespan_ms"`
	P50MakespanMS           int64          `json:"p50_workflow_makespan_ms"`
	P95MakespanMS           int64          `json:"p95_workflow_makespan_ms"`
	TotalCostUSD            float64        `json:"total_cost_usd"`
	TotalEnergyWh           float64        `json:"total_energy_wh"`
	ThroughputPerMin        float64        `json:"throughput_tasks_per_min"`
	AvgEffectiveDurationMS  float64        `json:"avg_effective_duration_ms"`
	ByTier                  map[string]TierSummary `json:"by_tier"`
}

type TierSummary struct {
	TasksCompleted int     `json:"tasks_completed"`
	AvgDurationMS  float64 `json:"avg_effective_duration_ms"`
	TotalCostUSD   float64 `json:"total_cost_usd"`
	TotalEnergyWh  float64 `json:"total_energy_wh"`
}

type WorkflowSummary struct {
	ID           string    `json:"id"`
	TemplateName string    `json:"template_name"`
	Priority     int       `json:"priority"`
	Status       string    `json:"status"`
	MakespanMS   int64     `json:"makespan_ms"`
	CreatedAt    time.Time `json:"created_at"`
}

// Export reads all results from Redis and writes a JSON file to the results/ directory.
func Export(s *store.RedisStore, cfg *config.Config, wallDuration time.Duration) {
	tasks, err := s.GetAllTaskResults()
	if err != nil {
		log.Printf("[Exporter] Failed to load task results: %v", err)
		return
	}
	workflows, err := s.GetAllWorkflowInstances()
	if err != nil {
		log.Printf("[Exporter] Failed to load workflow instances: %v", err)
		return
	}
	servers, err := s.GetAllServers()
	if err != nil {
		log.Printf("[Exporter] Failed to load servers: %v", err)
		return
	}

	numServers := map[string]int{"EDGE": 0, "FOG": 0, "CLOUD": 0}
	for _, srv := range servers {
		numServers[srv.Type]++
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
	}

	if err := os.MkdirAll("results", 0755); err != nil {
		log.Printf("[Exporter] Cannot create results/ directory: %v", err)
		return
	}

	ts := time.Now().UTC().Format("20060102_150405")
	filename := fmt.Sprintf("results/%s_edge%d_fog%d_cloud%d_%s.json",
		cfg.Scheduler.Algorithm,
		numServers["EDGE"], numServers["FOG"], numServers["CLOUD"],
		ts,
	)

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Printf("[Exporter] JSON marshal failed: %v", err)
		return
	}
	if err := os.WriteFile(filename, data, 0644); err != nil {
		log.Printf("[Exporter] Write failed: %v", err)
		return
	}
	log.Printf("[Exporter] Wrote %d task results and %d workflow results to %s", len(tasks), len(wfSummaries), filename)
}

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
