package store

import (
	"testing"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

func TestExecutionTimestamps(t *testing.T) {
	s := newTestStore(t)
	created := time.Now().UTC().Add(-time.Second)
	if err := s.StoreExecution(&models.Execution{ID: "e1", Status: "BLOCKED", CreatedAt: created}); err != nil {
		t.Fatal(err)
	}

	before := time.Now().UTC()
	if err := s.PromoteExecution("e1"); err != nil {
		t.Fatal(err)
	}
	ex, _ := s.GetExecution("e1")
	if ex.Status != "PENDING" || ex.ReadyAt == nil || ex.ReadyAt.Before(before) {
		t.Fatalf("after promote: status %s ready_at %v", ex.Status, ex.ReadyAt)
	}
	ready := *ex.ReadyAt

	// Promoting again must not move ReadyAt.
	if err := s.PromoteExecution("e1"); err != nil {
		t.Fatal(err)
	}
	if ex, _ = s.GetExecution("e1"); !ex.ReadyAt.Equal(ready) {
		t.Fatalf("second promote moved ready_at: %v -> %v", ready, ex.ReadyAt)
	}

	if err := s.AssignExecution("e1", "srv", "QUEUED"); err != nil {
		t.Fatal(err)
	}
	if ex, _ = s.GetExecution("e1"); ex.DispatchedAt == nil || ex.DispatchedAt.Before(ready) {
		t.Fatalf("after assign: dispatched_at %v", ex.DispatchedAt)
	}

	if err := s.RevertExecutionAssignment("e1"); err != nil {
		t.Fatal(err)
	}
	ex, _ = s.GetExecution("e1")
	if ex.DispatchedAt != nil || ex.Status != "PENDING" || !ex.ReadyAt.Equal(ready) {
		t.Fatalf("after revert: status %s dispatched_at %v ready_at %v", ex.Status, ex.DispatchedAt, ex.ReadyAt)
	}

	// A non-QUEUED assignment state does not stamp DispatchedAt.
	if err := s.AssignExecution("e1", "srv", "RUNNING"); err != nil {
		t.Fatal(err)
	}
	if ex, _ = s.GetExecution("e1"); ex.DispatchedAt != nil {
		t.Fatalf("RUNNING assignment stamped dispatched_at %v", ex.DispatchedAt)
	}
}

func TestCreateExecutionSetsReadyAt(t *testing.T) {
	s := newTestStore(t)
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	ex, err := s.CreateExecution("task", at)
	if err != nil {
		t.Fatal(err)
	}
	if ex.ReadyAt == nil || !ex.ReadyAt.Equal(at) {
		t.Fatalf("ready_at = %v, want %v", ex.ReadyAt, at)
	}
}

func TestMarkWorkflowDoneSetsResponse(t *testing.T) {
	s := newTestStore(t)
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	start1, end1 := created.Add(2*time.Second), created.Add(5*time.Second)
	start2, end2 := created.Add(6*time.Second), created.Add(9*time.Second)
	for id, se := range map[string][2]time.Time{"a": {start1, end1}, "b": {start2, end2}} {
		st, en := se[0], se[1]
		if err := s.StoreExecution(&models.Execution{ID: id, Status: "DONE", StartedAt: &st, CompletedAt: &en}); err != nil {
			t.Fatal(err)
		}
	}
	wi := &models.WorkflowInstance{ID: "w", Status: "RUNNING", CreatedAt: created, Seq: 7, SlotExecIDs: map[string]string{"A": "a", "B": "b"}}
	if err := s.StoreWorkflowInstance(wi); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkWorkflowDone("w"); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetWorkflowInstance("w")
	// Makespan keeps its v1 definition (first start -> last completion).
	if got.MakespanMS != 7000 {
		t.Errorf("makespan_ms = %d, want 7000", got.MakespanMS)
	}
	if got.ResponseMS != 9000 || got.CompletedAt == nil || !got.CompletedAt.Equal(end2) || got.Seq != 7 {
		t.Errorf("response_ms %d completed_at %v seq %d", got.ResponseMS, got.CompletedAt, got.Seq)
	}
}
