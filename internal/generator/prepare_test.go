package generator

import (
	"reflect"
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

func prepTasks() []*models.Task {
	return []*models.Task{
		{ID: "e", TargetTier: "EDGE", CPU: 2, Memory: 768, Disk: 20, Duration: 30},
		{ID: "f", TargetTier: "FOG", CPU: 6, Memory: 6144, Disk: 110, Duration: 400000},
		{ID: "c", TargetTier: "CLOUD", CPU: 32, Memory: 65536, Disk: 1600, Duration: 5000},
	}
}

func clone(ts []*models.Task) []models.Task {
	out := make([]models.Task, len(ts))
	for i, t := range ts {
		out[i] = *t
	}
	return out
}

func TestPrepareTasksDefaultsAreIdentity(t *testing.T) {
	in := prepTasks()
	out := PrepareTasks(in, Options{}, nil)
	if !reflect.DeepEqual(clone(out), clone(in)) {
		t.Fatalf("defaults changed tasks: %+v", clone(out))
	}
	if out[0] == in[0] {
		t.Fatal("PrepareTasks must return new Task values")
	}
}

func TestPrepareTasksScaleAndCap(t *testing.T) {
	in := prepTasks()
	before := clone(in)
	cfg := Options{
		// The small profile's factors: profile max / medium max.
		DemandScale: map[string]config.DemandScale{
			"edge":  {CPU: 2.0 / 2, Memory: 512.0 / 768, Disk: 10.0 / 20},
			"cloud": {CPU: 8.0 / 32, Memory: 8192.0 / 65536, Disk: 400.0 / 1600},
		},
		MaxDurationMS: 30000,
	}
	out := PrepareTasks(in, cfg, nil)
	want := []models.Task{
		{ID: "e", TargetTier: "EDGE", CPU: 2, Memory: 512, Disk: 10, Duration: 30},
		{ID: "f", TargetTier: "FOG", CPU: 6, Memory: 6144, Disk: 110, Duration: 30000}, // no fog factors
		{ID: "c", TargetTier: "CLOUD", CPU: 8, Memory: 8192, Disk: 400, Duration: 5000},
	}
	if got := clone(out); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
	if !reflect.DeepEqual(clone(in), before) {
		t.Fatal("input tasks were modified")
	}
}

func TestPrepareTasksRoundsUpWithMinOneCore(t *testing.T) {
	in := []*models.Task{{ID: "x", TargetTier: "EDGE", CPU: 1, Memory: 100, Disk: 0}}
	out := PrepareTasks(in, Options{DemandScale: map[string]config.DemandScale{"edge": {CPU: 0.1, Memory: 0.333, Disk: 0.5}}}, nil)
	if got := *out[0]; got.CPU != 1 || got.Memory != 34 || got.Disk != 0 {
		t.Fatalf("got cpu %d mem %d disk %d, want 1 34 0", got.CPU, got.Memory, got.Disk)
	}
}

func TestPrepareTasksDropUnschedulable(t *testing.T) {
	servers := []*models.Server{
		{ID: "a", MaxCPU: 32, MaxMemory: 49152, MaxDisk: 500},
		{ID: "b", MaxCPU: 8, MaxMemory: 8192, MaxDisk: 1600},
	}
	out := PrepareTasks(prepTasks(), Options{DropUnschedulable: true}, servers)
	var ids []string
	for _, t := range out {
		ids = append(ids, t.ID)
	}
	// "c" needs 32 cores, 64 GiB and 1600 GB: each fits somewhere, never together.
	if !reflect.DeepEqual(ids, []string{"e", "f"}) {
		t.Fatalf("kept %v, want [e f]", ids)
	}
}
