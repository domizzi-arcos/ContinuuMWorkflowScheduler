package scheduler

import (
	"encoding/json"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/broker"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/control"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

// Algorithm is the interface every scheduling policy must implement.
type Algorithm interface {
	Name() string
	// Select picks one server from candidates (all of which already have capacity).
	Select(task *models.Task, candidates []*models.Server) *models.Server
}

// Scheduler tracks execution IDs signaled by the Generator and dispatches
// pending executions by loading both the execution and the task definition
// from Redis.
type Scheduler struct {
	store        *store.RedisStore
	broker       *broker.Broker
	algorithm    Algorithm
	poll         time.Duration
	executionIDs <-chan string
	control      *control.Controller
	mu           sync.Mutex
	active       map[string]int // value = retry count
}

func New(s *store.RedisStore, b *broker.Broker, algorithm Algorithm, executionIDs <-chan string, ctrl *control.Controller) *Scheduler {
	return &Scheduler{
		store:        s,
		broker:       b,
		algorithm:    algorithm,
		poll:         500 * time.Millisecond,
		executionIDs: executionIDs,
		control:      ctrl,
		active:       make(map[string]int),
	}
}

// NewAlgorithm creates an Algorithm from the config string.
// wTime, wCost, wEnergy are the TOPSIS criterion weights (only used when name == "topsis").
func NewAlgorithm(name string, seed int64, wTime, wCost, wEnergy float64) Algorithm {
	switch strings.ToLower(name) {
	case "random":
		return NewRandom(seed)
	case "best_fit":
		return NewBestFit()
	case "worst_fit":
		return NewWorstFit()
	case "topsis":
		return NewTOPSIS(wTime, wCost, wEnergy)
	default:
		return NewRoundRobin()
	}
}

// Start runs the dispatch loop until done is closed.
func (s *Scheduler) Start(done <-chan struct{}) {
	ticker := time.NewTicker(s.poll)
	defer ticker.Stop()
	s.bootstrapActiveExecutions()
	slog.Info("scheduler started", "component", "scheduler", "algorithm", s.algorithm.Name(), "poll", s.poll)

	for {
		select {
		case executionID, ok := <-s.executionIDs:
			if !ok {
				s.executionIDs = nil
				continue
			}
			s.trackExecution(executionID)
		case <-ticker.C:
			if s.control != nil && !s.control.Wait(done) {
				slog.Info("scheduler stopped", "component", "scheduler")
				return
			}
			s.dispatch(done)
		case <-done:
			slog.Info("scheduler stopped", "component", "scheduler")
			return
		}
	}
}

func (s *Scheduler) dispatch(done <-chan struct{}) {
	executionIDs := s.snapshotActiveExecutions()
	if len(executionIDs) == 0 {
		return
	}

	servers, err := s.store.GetAllServers()
	if err != nil {
		slog.Error("error fetching servers", "component", "scheduler", "err", err)
		return
	}

	// Fetch all tracked executions, retire stale ones, collect PENDING.
	type candidate struct {
		exec *models.Execution
	}
	pending := make([]candidate, 0, len(executionIDs))
	for _, id := range executionIDs {
		exec, err := s.store.GetExecution(id)
		if err != nil {
			slog.Error("error fetching execution", "component", "scheduler", "execution_id", id, "err", err)
			s.untrackExecution(id)
			continue
		}
		switch exec.Status {
		case "DONE":
			s.untrackExecution(id)
		case "PENDING":
			pending = append(pending, candidate{exec})
		}
		// BLOCKED, QUEUED, RUNNING: leave in active map, skip this cycle.
	}

	// Dispatch highest-priority executions first.
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].exec.Priority > pending[j].exec.Priority
	})

	for _, c := range pending {
		if s.control != nil && !s.control.Wait(done) {
			return
		}
		s.dispatchOne(c.exec, servers, done)
	}
}

func (s *Scheduler) dispatchOne(execution *models.Execution, servers []*models.Server, done <-chan struct{}) {
	task, err := s.store.GetTask(execution.TaskID)
	if err != nil {
		slog.Error("error fetching task", "component", "scheduler", "task_id", execution.TaskID, "execution_id", execution.ID, "err", err)
		s.untrackExecution(execution.ID)
		return
	}

	candidates := filterByCapacity(task, servers)
	if len(candidates) == 0 {
		retries := s.incrementRetry(execution.ID)
		if retries == 1 {
			slog.Debug("no capacity, will retry", "component", "scheduler", "execution_id", execution.ID, "task_id", task.ID, "target_tier", task.TargetTier)
		}
		return
	}

	srv := s.algorithm.Select(task, candidates)
	if srv == nil {
		return
	}

	reservedServer, reserved, err := s.store.ReserveServerResources(srv.ID, task.CPU, task.Memory, task.Disk)
	if err != nil {
		slog.Error("error reserving server", "component", "scheduler", "server_id", srv.ID, "err", err)
		return
	}
	if reservedServer != nil {
		updateLocal(servers, reservedServer)
	}
	if !reserved {
		retries := s.incrementRetry(execution.ID)
		if retries == 1 {
			slog.Debug("no capacity after refresh, will retry", "component", "scheduler", "execution_id", execution.ID, "task_id", task.ID, "server_id", srv.ID)
		}
		return
	}

	if err := s.store.AssignExecution(execution.ID, srv.ID, "QUEUED"); err != nil {
		slog.Error("error assigning execution", "component", "scheduler", "execution_id", execution.ID, "err", err)
		rolledBack, releaseErr := s.store.ReleaseServerResources(srv.ID, task.CPU, task.Memory, task.Disk, false)
		if releaseErr != nil {
			slog.Error("error rolling back server after assign failure", "component", "scheduler", "server_id", srv.ID, "err", releaseErr)
		} else if rolledBack != nil {
			updateLocal(servers, rolledBack)
		}
		return
	}

	msg := models.TaskMessage{
		ExecutionID: execution.ID,
		TaskID:      task.ID,
		ServerID:    srv.ID,
	}
	body, _ := json.Marshal(msg)
	routingKey := strings.ToLower(srv.Type)
	if err := s.broker.PublishTask(routingKey, body); err != nil {
		slog.Error("publish error", "component", "scheduler", "execution_id", execution.ID, "err", err)
		if revertErr := s.store.RevertExecutionAssignment(execution.ID); revertErr != nil {
			slog.Error("error reverting execution assignment", "component", "scheduler", "execution_id", execution.ID, "err", revertErr)
		}
		rolledBack, releaseErr := s.store.ReleaseServerResources(srv.ID, task.CPU, task.Memory, task.Disk, false)
		if releaseErr != nil {
			slog.Error("error rolling back server after publish failure", "component", "scheduler", "server_id", srv.ID, "err", releaseErr)
		} else if rolledBack != nil {
			updateLocal(servers, rolledBack)
		}
		return
	}

	retries := s.resetRetry(execution.ID)
	attrs := []any{
		"component", "scheduler",
		"algorithm", s.algorithm.Name(),
		"execution_id", execution.ID,
		"task_id", task.ID,
		"task_name", task.Name,
		"priority", execution.Priority,
		"target_tier", task.TargetTier,
		"server_id", srv.ID,
	}
	if retries > 0 {
		attrs = append(attrs, "retries", retries)
	}
	slog.Debug("execution dispatched", attrs...)
}

func filterByCapacity(task *models.Task, servers []*models.Server) []*models.Server {
	out := make([]*models.Server, 0, len(servers))
	for _, srv := range servers {
		if srv.HasCapacity(task.CPU, task.Memory, task.Disk) {
			out = append(out, srv)
		}
	}
	return out
}

func updateLocal(servers []*models.Server, updated *models.Server) {
	for i, srv := range servers {
		if srv.ID == updated.ID {
			servers[i] = updated
			return
		}
	}
}

func (s *Scheduler) bootstrapActiveExecutions() {
	executions, err := s.store.GetAllExecutions()
	if err != nil {
		slog.Error("error bootstrapping executions", "component", "scheduler", "err", err)
		return
	}
	for _, execution := range executions {
		// BLOCKED executions are managed by the workflow tracker; the scheduler
		// only tracks executions that are actively dispatchable.
		if execution.Status != "DONE" && execution.Status != "BLOCKED" {
			s.trackExecution(execution.ID)
		}
	}
}

func (s *Scheduler) trackExecution(executionID string) {
	if executionID == "" {
		return
	}
	s.mu.Lock()
	if _, exists := s.active[executionID]; !exists {
		s.active[executionID] = 0
	}
	s.mu.Unlock()
}

func (s *Scheduler) untrackExecution(executionID string) {
	s.mu.Lock()
	delete(s.active, executionID)
	s.mu.Unlock()
}

func (s *Scheduler) incrementRetry(executionID string) int {
	s.mu.Lock()
	s.active[executionID]++
	n := s.active[executionID]
	s.mu.Unlock()
	return n
}

func (s *Scheduler) resetRetry(executionID string) int {
	s.mu.Lock()
	n := s.active[executionID]
	s.active[executionID] = 0
	s.mu.Unlock()
	return n
}

func (s *Scheduler) snapshotActiveExecutions() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.active))
	for id := range s.active {
		ids = append(ids, id)
	}
	return ids
}
