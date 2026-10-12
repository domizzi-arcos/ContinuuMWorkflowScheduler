package results

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// legacySchema lists every JSON key written by the v1 exporter (commit
// 9ac5f9c), per object. The schema is additive only: each key must still be
// present, with the same name, in a fully populated v2 result.
var legacySchema = map[string][]string{
	"":                   {"meta", "summary", "task_results", "workflow_results", "servers"},
	"meta":               {"algorithm", "timestamp", "duration_seconds", "task_count", "num_servers", "topsis_weights"},
	"summary":            {"total_tasks_completed", "total_workflows_completed", "avg_workflow_makespan_ms", "p50_workflow_makespan_ms", "p95_workflow_makespan_ms", "total_cost_usd", "total_energy_wh", "throughput_tasks_per_min", "avg_effective_duration_ms", "by_tier"},
	"summary.by_tier.":   {"tasks_completed", "avg_effective_duration_ms", "total_cost_usd", "total_energy_wh"},
	"task_results[]":     {"execution_id", "task_id", "task_name", "target_tier", "server_id", "server_type", "workflow_id", "algorithm", "start_time", "end_time", "duration_ms", "effective_duration_ms", "cost_usd", "energy_wh", "dataflow_path"},
	"workflow_results[]": {"id", "template_name", "priority", "status", "makespan_ms", "created_at"},
	"servers[]":          {"id", "server_type", "completed_tasks", "active_tasks"},
}

func objKeys(t *testing.T, raw json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal object: %v", err)
	}
	return m
}

func firstElem(t *testing.T, raw json.RawMessage) json.RawMessage {
	t.Helper()
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil || len(arr) == 0 {
		t.Fatalf("want non-empty array, got %s", raw)
	}
	return arr[0]
}

// fullResult builds a result with every field set, so omitempty fields are
// present in the JSON.
func fullResult() ExperimentResult {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	tasks := []*models.TaskResult{{
		ExecutionID: "e", TaskID: "t", TaskName: "t", TargetTier: "EDGE", ServerID: "s", ServerType: "EDGE",
		WorkflowID: "w", Algorithm: "topsis", StartTime: now, EndTime: now, DurationMS: 1, EffectiveDurationMS: 2,
		CostUSD: 1, EnergyWh: 1, DataflowPath: []string{"x"},
	}}
	wfs := []*models.WorkflowInstance{{ID: "w", TemplateName: "pipeline", Priority: 1, Status: "DONE", MakespanMS: 5, CreatedAt: now}}
	return ExperimentResult{
		Meta:      ExperimentMeta{Algorithm: "topsis", Timestamp: now, NumServers: map[string]int{"EDGE": 1}},
		Summary:   buildSummary(tasks, wfs, time.Minute),
		Tasks:     tasks,
		Workflows: []*WorkflowSummary{{ID: "w", TemplateName: "pipeline", Priority: 1, Status: "DONE", MakespanMS: 5, CreatedAt: now}},
		Servers:   []*ServerSnapshot{{ID: "s", ServerType: "EDGE"}},
	}
}

func TestLegacyKeysStillPresent(t *testing.T) {
	data, err := json.Marshal(fullResult())
	if err != nil {
		t.Fatal(err)
	}
	top := json.RawMessage(data)
	root := objKeys(t, top)
	summary := objKeys(t, root["summary"])
	byTier := objKeys(t, summary["by_tier"])
	objects := map[string]map[string]json.RawMessage{
		"":                   root,
		"meta":               objKeys(t, root["meta"]),
		"summary":            summary,
		"summary.by_tier.":   objKeys(t, byTier["EDGE"]),
		"task_results[]":     objKeys(t, firstElem(t, root["task_results"])),
		"workflow_results[]": objKeys(t, firstElem(t, root["workflow_results"])),
		"servers[]":          objKeys(t, firstElem(t, root["servers"])),
	}
	for path, want := range legacySchema {
		got := objects[path]
		for _, k := range want {
			if _, ok := got[k]; !ok {
				t.Errorf("%s: legacy key %q missing", path, k)
			}
		}
	}
}
