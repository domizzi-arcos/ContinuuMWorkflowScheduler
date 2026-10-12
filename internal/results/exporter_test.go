package results

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/provenance"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

// legacyMetaKeys is the meta key set written before eval-v2. The schema is
// additive only: these keys must keep appearing, unchanged.
var legacyMetaKeys = []string{"algorithm", "duration_seconds", "num_servers", "task_count", "timestamp", "topsis_weights"}

var legacyTopKeys = []string{"meta", "servers", "summary", "task_results", "workflow_results"}

func exportOnce(t *testing.T, info RunInfo) map[string]json.RawMessage {
	t.Helper()
	mr := miniredis.RunT(t)
	s, err := store.NewRedisStore(mr.Addr(), 0)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	if err := s.StoreServer(&models.Server{ID: "e1", Type: "EDGE", MaxCPU: 2, MaxMemory: 512, MaxDisk: 10}); err != nil {
		t.Fatalf("StoreServer: %v", err)
	}
	cfg := &config.Config{}
	cfg.Scheduler.Algorithm = "topsis"
	dir := t.TempDir()
	path, err := ExportRun(s, cfg, time.Minute, dir, info)
	if err != nil {
		t.Fatalf("ExportRun: %v", err)
	}
	if filepath.Dir(path) != dir {
		t.Fatalf("written to %q, want dir %q", path, dir)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return top
}

func keys(t *testing.T, raw json.RawMessage) map[string]bool {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	out := map[string]bool{}
	for k := range m {
		out[k] = true
	}
	return out
}

func TestExportZeroRunInfoKeepsLegacySchema(t *testing.T) {
	top := exportOnce(t, RunInfo{})
	var got []string
	for k := range top {
		got = append(got, k)
	}
	sort.Strings(got)
	if len(got) != len(legacyTopKeys) {
		t.Fatalf("top-level keys = %v, want %v", got, legacyTopKeys)
	}
	meta := keys(t, top["meta"])
	if meta["provenance"] {
		t.Error("meta.provenance must be omitted when RunInfo is zero")
	}
	if len(meta) != len(legacyMetaKeys) {
		t.Errorf("meta keys = %v, want exactly %v", meta, legacyMetaKeys)
	}
}

func TestExportProvenanceIsAdditive(t *testing.T) {
	prov := &provenance.Info{GitCommit: "abc", GitDirty: true, GoVersion: "go1.x", HostHash: "0123456789abcdef", Args: []string{"-config", "c.yaml"}}
	top := exportOnce(t, RunInfo{Provenance: prov})
	for _, k := range legacyTopKeys {
		if _, ok := top[k]; !ok {
			t.Errorf("missing top-level key %q", k)
		}
	}
	meta := keys(t, top["meta"])
	for _, k := range legacyMetaKeys {
		if !meta[k] {
			t.Errorf("missing legacy meta key %q", k)
		}
	}
	var m struct {
		Provenance provenance.Info `json:"provenance"`
	}
	if err := json.Unmarshal(top["meta"], &m); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}
	if m.Provenance.GitCommit != "abc" || !m.Provenance.GitDirty || m.Provenance.HostHash != prov.HostHash || len(m.Provenance.Args) != 2 {
		t.Errorf("provenance round-trip = %+v", m.Provenance)
	}
}
