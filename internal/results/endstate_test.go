package results

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

func TestExportRunV2Fields(t *testing.T) {
	mr := miniredis.RunT(t)
	s, err := store.NewRedisStore(mr.Addr(), 0)
	if err != nil {
		t.Fatal(err)
	}
	srv := &models.Server{ID: "c0", Type: "CLOUD", MaxCPU: 16, MaxMemory: 32768, MaxDisk: 400, Latency: 80, CostPerHour: 1.5, PowerWatts: 300}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(s.StoreServer(srv))
	must(s.StoreTask(&models.Task{ID: "fits", CPU: 1, Memory: 128, Disk: 1}))
	must(s.StoreTask(&models.Task{ID: "huge", CPU: 32, Memory: 128, Disk: 1}))
	now := time.Now().UTC()
	for id, st := range map[string][2]string{
		"e1": {"PENDING", "fits"}, "e2": {"PENDING", "huge"}, "e3": {"BLOCKED", "fits"},
		"e4": {"QUEUED", "fits"}, "e5": {"RUNNING", "fits"}, "e6": {"DONE", "fits"},
	} {
		must(s.StoreExecution(&models.Execution{ID: id, Status: st[0], TaskID: st[1], CreatedAt: now}))
	}
	done := now.Add(3 * time.Second)
	must(s.StoreWorkflowInstance(&models.WorkflowInstance{ID: "w", TemplateName: "pipeline", Status: "DONE", Seq: 4, CreatedAt: now, CompletedAt: &done, ResponseMS: 3000}))

	cfg := &config.Config{}
	cfg.Scheduler.Algorithm = "topsis"
	cfg.Experiment.Labels = map[string]string{"topology": "uniform_m", "seed": "1"}
	drained, drainS := false, 180.5
	path, err := ExportRun(s, cfg, time.Minute, t.TempDir(), RunInfo{Drained: &drained, DrainSeconds: &drainS})
	must(err)
	data, err := os.ReadFile(path)
	must(err)
	var r ExperimentResult
	must(json.Unmarshal(data, &r))

	es := r.Summary.EndState
	if es == nil {
		t.Fatal("summary.end_state missing")
	}
	if es.Pending != 2 || es.PendingUnschedulable != 1 || es.Blocked != 1 || es.Queued != 1 || es.Running != 1 {
		t.Errorf("end_state = %+v", *es)
	}
	if es.Drained == nil || *es.Drained || es.DrainSeconds == nil || *es.DrainSeconds != 180.5 {
		t.Errorf("drained/drain_seconds = %v/%v", es.Drained, es.DrainSeconds)
	}
	if r.Meta.Labels["topology"] != "uniform_m" || r.Meta.Labels["seed"] != "1" {
		t.Errorf("meta.labels = %v", r.Meta.Labels)
	}
	sv := r.Servers[0]
	if sv.MaxCPU != 16 || sv.MaxMemory != 32768 || sv.MaxDisk != 400 || sv.Latency != 80 || sv.CostPerHour != 1.5 || sv.PowerWatts != 300 {
		t.Errorf("server snapshot = %+v", *sv)
	}
	wf := r.Workflows[0]
	if wf.Seq != 4 || wf.CompletedAt == nil || !wf.CompletedAt.Equal(done) || wf.ResponseMS != 3000 {
		t.Errorf("workflow summary = %+v", *wf)
	}
}

func TestExportWithoutDrainInfoOmitsDrainFields(t *testing.T) {
	top := exportOnce(t, RunInfo{})
	var summary struct {
		EndState map[string]json.RawMessage `json:"end_state"`
	}
	if err := json.Unmarshal(top["summary"], &summary); err != nil {
		t.Fatal(err)
	}
	if _, ok := summary.EndState["drained"]; ok {
		t.Error("drained present without RunInfo.Drained")
	}
	if _, ok := summary.EndState["pending"]; !ok {
		t.Error("end_state.pending missing")
	}
}
