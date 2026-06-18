package models_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// These benchmarks exercise the JSON encode/decode path that every Redis
// write/read and every RabbitMQ message goes through. Run them with the
// stock library and again with Go's experimental v2 encoder to quantify the
// gain on the scheduler's actual payloads:
//
//	go test -run '^$' -bench BenchmarkJSON -benchmem ./internal/models/
//	GOEXPERIMENT=jsonv2 go test -run '^$' -bench BenchmarkJSON -benchmem ./internal/models/
//
// Server and Execution are the high-churn objects (rewritten on every reserve
// / release / state transition), so their marshal cost is the one that shows
// up under load. ResultMessage is the largest payload (carries the dataflow
// trace) and is emitted once per completed task.

var benchServer = models.Server{
	ID: "fog-007", Type: "FOG", MaxCPU: 8, MaxMemory: 16384, MaxDisk: 500,
	UsedCPU: 3, UsedMemory: 6144, UsedDisk: 120, Latency: 22,
	CostPerHour: 0.45, PowerWatts: 130, ActiveTasks: 3, CompletedTasks: 91,
}

var benchResult = models.ResultMessage{
	ExecutionID: "exec-abc123", TaskID: "task-42", TaskName: "transform",
	TargetTier: "FOG", ServerID: "fog-007", ServerType: "FOG",
	WorkflowID: "wf-9", Algorithm: "topsis",
	StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(1500 * time.Millisecond),
	DurationMS: 1500, EffectiveDurationMS: 1522, CostUSD: 0.00019, EnergyWh: 0.055,
	DataflowPath: []string{
		"generator", "redis", "scheduler", "task_exchange",
		"fog_queue", "fog-007", "result_exchange", "result_queue", "result_store",
	},
}

func BenchmarkJSONMarshalServer(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(&benchServer); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONUnmarshalServer(b *testing.B) {
	data, err := json.Marshal(&benchServer)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s models.Server
		if err := json.Unmarshal(data, &s); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONMarshalResult(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(&benchResult); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONUnmarshalResult(b *testing.B) {
	data, err := json.Marshal(&benchResult)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var r models.ResultMessage
		if err := json.Unmarshal(data, &r); err != nil {
			b.Fatal(err)
		}
	}
}
