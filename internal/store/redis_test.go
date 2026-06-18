package store

import (
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// newTestStore spins up an in-process miniredis and returns a RedisStore
// bound to it. No external services are required to run these tests.
func newTestStore(t *testing.T) *RedisStore {
	t.Helper()
	mr := miniredis.RunT(t)
	s, err := NewRedisStore(mr.Addr(), 0)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	return s
}

func seedServer(t *testing.T, s *RedisStore, id string, cpu, mem, disk int) *models.Server {
	t.Helper()
	srv := &models.Server{
		ID: id, Type: "EDGE",
		MaxCPU: cpu, MaxMemory: mem, MaxDisk: disk,
		Latency: 5, CostPerHour: 0.02, PowerWatts: 15,
	}
	if err := s.StoreServer(srv); err != nil {
		t.Fatalf("StoreServer: %v", err)
	}
	return srv
}

func TestReserveServerResources_ReservesAndRefuses(t *testing.T) {
	s := newTestStore(t)
	seedServer(t, s, "srv1", 2, 1024, 10)

	// First reservation fits.
	srv, ok, err := s.ReserveServerResources("srv1", 2, 512, 5)
	if err != nil || !ok {
		t.Fatalf("expected successful reservation, ok=%v err=%v", ok, err)
	}
	if srv.UsedCPU != 2 || srv.UsedMemory != 512 || srv.UsedDisk != 5 || srv.ActiveTasks != 1 {
		t.Fatalf("counters after reserve = %+v", srv)
	}

	// Second reservation must be refused: no CPU left.
	srv2, ok, err := s.ReserveServerResources("srv1", 1, 1, 1)
	if err != nil {
		t.Fatalf("refusal should not error: %v", err)
	}
	if ok {
		t.Fatalf("reservation should have been refused, got server %+v", srv2)
	}
	// Refusal must not mutate counters.
	cur, err := s.GetServer("srv1")
	if err != nil {
		t.Fatalf("GetServer: %v", err)
	}
	if cur.UsedCPU != 2 || cur.ActiveTasks != 1 {
		t.Fatalf("refused reservation mutated state: %+v", cur)
	}
}

func TestReleaseServerResources_ReleasesAndClamps(t *testing.T) {
	s := newTestStore(t)
	seedServer(t, s, "srv1", 4, 2048, 20)

	if _, ok, _ := s.ReserveServerResources("srv1", 2, 1024, 10); !ok {
		t.Fatal("setup reservation failed")
	}
	srv, err := s.ReleaseServerResources("srv1", 2, 1024, 10, true)
	if err != nil {
		t.Fatalf("ReleaseServerResources: %v", err)
	}
	if srv.UsedCPU != 0 || srv.UsedMemory != 0 || srv.UsedDisk != 0 || srv.ActiveTasks != 0 {
		t.Fatalf("counters after release = %+v", srv)
	}
	if srv.CompletedTasks != 1 {
		t.Fatalf("completed=true must increment CompletedTasks, got %d", srv.CompletedTasks)
	}

	// A duplicate (retried) release must clamp at zero, never go negative.
	srv, err = s.ReleaseServerResources("srv1", 2, 1024, 10, false)
	if err != nil {
		t.Fatalf("duplicate release: %v", err)
	}
	if srv.UsedCPU != 0 || srv.UsedMemory != 0 || srv.UsedDisk != 0 || srv.ActiveTasks != 0 {
		t.Fatalf("duplicate release went negative: %+v", srv)
	}
}

// TestReserveServerResources_NeverOvercommits is the core safety property of
// the scheduler: under concurrent reservation attempts, the number of granted
// reservations must never exceed the server's capacity.
func TestReserveServerResources_NeverOvercommits(t *testing.T) {
	s := newTestStore(t)
	const capacity = 8
	seedServer(t, s, "srv1", capacity, 1<<20, 1<<10) // CPU is the scarce dimension

	const attempts = 64
	var granted int32
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := s.ReserveServerResources("srv1", 1, 1, 1)
			if err != nil {
				t.Errorf("reserve error: %v", err)
				return
			}
			if ok {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if granted != capacity {
		t.Fatalf("granted %d reservations for capacity %d", granted, capacity)
	}
	cur, err := s.GetServer("srv1")
	if err != nil {
		t.Fatalf("GetServer: %v", err)
	}
	if cur.UsedCPU != capacity || cur.UsedCPU > cur.MaxCPU {
		t.Fatalf("overcommitted: UsedCPU=%d MaxCPU=%d", cur.UsedCPU, cur.MaxCPU)
	}
}

func TestExecutionAssignmentRoundTrip(t *testing.T) {
	s := newTestStore(t)

	if err := s.StoreTask(&models.Task{ID: "task_1", Name: "t", CPU: 1, Memory: 1, Disk: 1, Duration: 10, TargetTier: "EDGE"}); err != nil {
		t.Fatalf("StoreTask: %v", err)
	}
	exec, err := s.CreateExecution("task_1", time.Now())
	if err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if exec.Status != "PENDING" {
		t.Fatalf("new execution should be PENDING, got %s", exec.Status)
	}

	if err := s.AssignExecution(exec.ID, "srv1", "QUEUED"); err != nil {
		t.Fatalf("AssignExecution: %v", err)
	}
	got, _ := s.GetExecution(exec.ID)
	if got.Status != "QUEUED" || got.AssignedServer != "srv1" {
		t.Fatalf("after assign: %+v", got)
	}

	// Revert (broker publish failed): back to PENDING with no server.
	if err := s.RevertExecutionAssignment(exec.ID); err != nil {
		t.Fatalf("RevertExecutionAssignment: %v", err)
	}
	got, _ = s.GetExecution(exec.ID)
	if got.Status != "PENDING" || got.AssignedServer != "" {
		t.Fatalf("after revert: %+v", got)
	}

	// Happy path to DONE.
	if err := s.AssignExecution(exec.ID, "srv1", "QUEUED"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkExecutionRunning(exec.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkExecutionDone(exec.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetExecution(exec.ID)
	if got.Status != "DONE" {
		t.Fatalf("after done: %+v", got)
	}
}
