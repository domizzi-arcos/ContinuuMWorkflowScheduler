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
