package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
}

const optimisticWriteRetries = 16

var errInsufficientCapacity = errors.New("insufficient server capacity")

type RawEntry struct {
	Key       string      `json:"key"`
	Category  string      `json:"category"`
	Value     interface{} `json:"value"`
	ValueType string      `json:"value_type"`
	SizeBytes int         `json:"size_bytes"`
}

func NewRedisStore(addr string, db int) (*RedisStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   db,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}
	return &RedisStore{client: client}, nil
}

// ── Task Definitions ────────────────────────────────────────────────────────

func (s *RedisStore) StoreTask(task *models.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.client.Set(context.Background(), "task:"+task.ID, data, 0).Err()
}

func (s *RedisStore) GetTask(id string) (*models.Task, error) {
	data, err := s.client.Get(context.Background(), "task:"+id).Result()
	if err != nil {
		return nil, err
	}
	var task models.Task
	return &task, json.Unmarshal([]byte(data), &task)
}

func (s *RedisStore) GetAllTasks() ([]*models.Task, error) {
	ctx := context.Background()
	keys, err := s.client.Keys(ctx, "task:*").Result()
	if err != nil {
		return nil, err
	}
	tasks := make([]*models.Task, 0, len(keys))
	for _, key := range keys {
		data, err := s.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		var task models.Task
		if err := json.Unmarshal([]byte(data), &task); err != nil {
			continue
		}
		tasks = append(tasks, &task)
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].CreatedAt.Equal(tasks[j].CreatedAt) {
			return tasks[i].ID < tasks[j].ID
		}
		return tasks[i].CreatedAt.Before(tasks[j].CreatedAt)
	})
	return tasks, nil
}

// ── Executions ──────────────────────────────────────────────────────────────

func (s *RedisStore) CreateExecution(taskID string, createdAt time.Time) (*models.Execution, error) {
	ctx := context.Background()
	seq, err := s.client.Incr(ctx, "counter:execution").Result()
	if err != nil {
		return nil, err
	}

	execution := &models.Execution{
		ID:        fmt.Sprintf("exec_%06d", seq),
		TaskID:    taskID,
		Status:    "PENDING",
		CreatedAt: createdAt,
	}

	data, err := json.Marshal(execution)
	if err != nil {
		return nil, err
	}
	if err := s.client.Set(ctx, "execution:"+execution.ID, data, 0).Err(); err != nil {
		return nil, err
	}
	return execution, nil
}

// AllocateExecutionIDs atomically allocates n sequential execution IDs using a
// Redis pipeline. Callers can then build full Execution structs and store them
// with StoreExecution without a second round-trip to generate IDs.
func (s *RedisStore) AllocateExecutionIDs(n int) ([]string, error) {
	if n <= 0 {
		return nil, nil
	}
	ctx := context.Background()
	pipe := s.client.Pipeline()
	cmds := make([]*redis.IntCmd, n)
	for i := range cmds {
		cmds[i] = pipe.Incr(ctx, "counter:execution")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	ids := make([]string, n)
	for i, cmd := range cmds {
		ids[i] = fmt.Sprintf("exec_%06d", cmd.Val())
	}
	return ids, nil
}

// StoreExecution persists a pre-built Execution struct. Use after AllocateExecutionIDs.
func (s *RedisStore) StoreExecution(exec *models.Execution) error {
	data, err := json.Marshal(exec)
	if err != nil {
		return err
	}
	return s.client.Set(context.Background(), "execution:"+exec.ID, data, 0).Err()
}

// PromoteExecution transitions a BLOCKED execution to PENDING.
// Idempotent: if the execution is already PENDING or beyond, it is left unchanged.
func (s *RedisStore) PromoteExecution(executionID string) error {
	_, err := s.updateExecution(executionID, func(e *models.Execution) error {
		if e.Status == "BLOCKED" {
			e.Status = "PENDING"
		}
		return nil
	})
	return err
}

func (s *RedisStore) GetExecution(id string) (*models.Execution, error) {
	data, err := s.client.Get(context.Background(), "execution:"+id).Result()
	if err != nil {
		return nil, err
	}
	var execution models.Execution
	return &execution, json.Unmarshal([]byte(data), &execution)
}

func (s *RedisStore) GetAllExecutions() ([]*models.Execution, error) {
	ctx := context.Background()
	keys, err := s.client.Keys(ctx, "execution:*").Result()
	if err != nil {
		return nil, err
	}
	executions := make([]*models.Execution, 0, len(keys))
	for _, key := range keys {
		data, err := s.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		var execution models.Execution
		if err := json.Unmarshal([]byte(data), &execution); err != nil {
			continue
		}
		executions = append(executions, &execution)
	}
	sort.Slice(executions, func(i, j int) bool {
		if executions[i].CreatedAt.Equal(executions[j].CreatedAt) {
			return executions[i].ID < executions[j].ID
		}
		return executions[i].CreatedAt.Before(executions[j].CreatedAt)
	})
	return executions, nil
}

func (s *RedisStore) AssignExecution(executionID, serverID, state string) error {
	_, err := s.updateExecution(executionID, func(execution *models.Execution) error {
		execution.AssignedServer = serverID
		execution.Status = state
		return nil
	})
	return err
}

func (s *RedisStore) RevertExecutionAssignment(executionID string) error {
	_, err := s.updateExecution(executionID, func(execution *models.Execution) error {
		execution.AssignedServer = ""
		execution.Status = "PENDING"
		execution.StartedAt = nil
		execution.CompletedAt = nil
		return nil
	})
	return err
}

func (s *RedisStore) MarkExecutionRunning(executionID string, startedAt time.Time) error {
	_, err := s.updateExecution(executionID, func(execution *models.Execution) error {
		execution.Status = "RUNNING"
		execution.StartedAt = &startedAt
		return nil
	})
	return err
}

func (s *RedisStore) MarkExecutionDone(executionID string, completedAt time.Time) error {
	_, err := s.updateExecution(executionID, func(execution *models.Execution) error {
		execution.Status = "DONE"
		execution.CompletedAt = &completedAt
		return nil
	})
	return err
}

// ── Server ──────────────────────────────────────────────────────────────────

func (s *RedisStore) StoreServer(srv *models.Server) error {
	data, err := json.Marshal(srv)
	if err != nil {
		return err
	}
	return s.client.Set(context.Background(), "server:"+srv.ID, data, 0).Err()
}

func (s *RedisStore) ReserveServerResources(serverID string, cpu, memory, disk int) (*models.Server, bool, error) {
	srv, err := s.updateServer(serverID, func(srv *models.Server) error {
		if !srv.HasCapacity(cpu, memory, disk) {
			return errInsufficientCapacity
		}
		srv.UsedCPU += cpu
		srv.UsedMemory += memory
		srv.UsedDisk += disk
		srv.ActiveTasks++
		return nil
	})
	if err == nil {
		return srv, true, nil
	}
	if !errors.Is(err, errInsufficientCapacity) {
		return nil, false, err
	}
	current, getErr := s.GetServer(serverID)
	if getErr != nil {
		return nil, false, getErr
	}
	return current, false, nil
}

func (s *RedisStore) ReleaseServerResources(serverID string, cpu, memory, disk int, completed bool) (*models.Server, error) {
	return s.updateServer(serverID, func(srv *models.Server) error {
		srv.UsedCPU -= cpu
		srv.UsedMemory -= memory
		srv.UsedDisk -= disk
		srv.ActiveTasks--
		if completed {
			srv.CompletedTasks++
		}
		if srv.UsedCPU < 0 {
			srv.UsedCPU = 0
		}
		if srv.UsedMemory < 0 {
			srv.UsedMemory = 0
		}
		if srv.UsedDisk < 0 {
			srv.UsedDisk = 0
		}
		if srv.ActiveTasks < 0 {
			srv.ActiveTasks = 0
		}
		return nil
	})
}

func (s *RedisStore) GetServer(id string) (*models.Server, error) {
	data, err := s.client.Get(context.Background(), "server:"+id).Result()
	if err != nil {
		return nil, err
	}
	var srv models.Server
	return &srv, json.Unmarshal([]byte(data), &srv)
}

func (s *RedisStore) GetAllServers() ([]*models.Server, error) {
	ctx := context.Background()
	keys, err := s.client.Keys(ctx, "server:*").Result()
	if err != nil {
		return nil, err
	}
	servers := make([]*models.Server, 0, len(keys))
	for _, key := range keys {
		data, err := s.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		var srv models.Server
		if err := json.Unmarshal([]byte(data), &srv); err != nil {
			continue
		}
		servers = append(servers, &srv)
	}
	sort.Slice(servers, func(i, j int) bool { return servers[i].ID < servers[j].ID })
	return servers, nil
}

// ── TaskResult ──────────────────────────────────────────────────────────────

func (s *RedisStore) StoreTaskResult(r *models.TaskResult) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return s.client.Set(context.Background(), "result:"+r.ExecutionID, data, 0).Err()
}

func (s *RedisStore) GetTaskResult(executionID string) (*models.TaskResult, error) {
	data, err := s.client.Get(context.Background(), "result:"+executionID).Result()
	if err != nil {
		return nil, err
	}
	var result models.TaskResult
	return &result, json.Unmarshal([]byte(data), &result)
}

func (s *RedisStore) GetAllTaskResults() ([]*models.TaskResult, error) {
	ctx := context.Background()
	keys, err := s.client.Keys(ctx, "result:*").Result()
	if err != nil {
		return nil, err
	}
	results := make([]*models.TaskResult, 0, len(keys))
	for _, key := range keys {
		data, err := s.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		var result models.TaskResult
		if err := json.Unmarshal([]byte(data), &result); err != nil {
			continue
		}
		results = append(results, &result)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].EndTime.Equal(results[j].EndTime) {
			return results[i].ExecutionID < results[j].ExecutionID
		}
		return results[i].EndTime.Before(results[j].EndTime)
	})
	return results, nil
}

// ── Workflow Instances ───────────────────────────────────────────────────────

func (s *RedisStore) StoreWorkflowInstance(wi *models.WorkflowInstance) error {
	data, err := json.Marshal(wi)
	if err != nil {
		return err
	}
	return s.client.Set(context.Background(), "workflow:"+wi.ID, data, 0).Err()
}

func (s *RedisStore) GetWorkflowInstance(id string) (*models.WorkflowInstance, error) {
	data, err := s.client.Get(context.Background(), "workflow:"+id).Result()
	if err != nil {
		return nil, err
	}
	var wi models.WorkflowInstance
	return &wi, json.Unmarshal([]byte(data), &wi)
}

func (s *RedisStore) MarkWorkflowDone(id string) error {
	wi, err := s.GetWorkflowInstance(id)
	if err != nil {
		return err
	}
	wi.Status = "DONE"
	wi.MakespanMS = s.computeWorkflowMakespan(wi)
	return s.StoreWorkflowInstance(wi)
}

// computeWorkflowMakespan scans all slot executions and returns
// max(CompletedAt) - min(StartedAt) in milliseconds.
func (s *RedisStore) computeWorkflowMakespan(wi *models.WorkflowInstance) int64 {
	var earliest, latest *time.Time
	for _, execID := range wi.SlotExecIDs {
		exec, err := s.GetExecution(execID)
		if err != nil || exec.StartedAt == nil || exec.CompletedAt == nil {
			continue
		}
		if earliest == nil || exec.StartedAt.Before(*earliest) {
			t := *exec.StartedAt
			earliest = &t
		}
		if latest == nil || exec.CompletedAt.After(*latest) {
			t := *exec.CompletedAt
			latest = &t
		}
	}
	if earliest == nil || latest == nil {
		return 0
	}
	return latest.Sub(*earliest).Milliseconds()
}

func (s *RedisStore) GetAllWorkflowInstances() ([]*models.WorkflowInstance, error) {
	ctx := context.Background()
	keys, err := s.client.Keys(ctx, "workflow:*").Result()
	if err != nil {
		return nil, err
	}
	instances := make([]*models.WorkflowInstance, 0, len(keys))
	for _, key := range keys {
		data, err := s.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		var wi models.WorkflowInstance
		if err := json.Unmarshal([]byte(data), &wi); err != nil {
			continue
		}
		instances = append(instances, &wi)
	}
	sort.Slice(instances, func(i, j int) bool {
		return instances[i].CreatedAt.Before(instances[j].CreatedAt)
	})
	return instances, nil
}

// ── Raw Redis inspection ────────────────────────────────────────────────────

func (s *RedisStore) GetAllRawEntries() ([]RawEntry, error) {
	ctx := context.Background()
	keys, err := s.client.Keys(ctx, "*").Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)

	entries := make([]RawEntry, 0, len(keys))
	for _, key := range keys {
		raw, err := s.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		entry := RawEntry{
			Key:       key,
			Category:  entryCategory(key),
			Value:     raw,
			ValueType: "string",
			SizeBytes: len(raw),
		}

		var parsed interface{}
		if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
			entry.Value = parsed
			entry.ValueType = "json"
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func (s *RedisStore) ResetSimulationData() error {
	ctx := context.Background()
	patterns := []string{
		"task:*",
		"execution:*",
		"server:*",
		"result:*",
		"workflow:*",
		"instance:*",
		"counter:execution",
	}

	for _, pattern := range patterns {
		keys, err := s.client.Keys(ctx, pattern).Result()
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			continue
		}
		if err := s.client.Del(ctx, keys...).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (s *RedisStore) updateExecution(executionID string, mutate func(*models.Execution) error) (*models.Execution, error) {
	key := "execution:" + executionID
	ctx := context.Background()
	for attempt := 0; attempt < optimisticWriteRetries; attempt++ {
		var updated models.Execution
		err := s.client.Watch(ctx, func(tx *redis.Tx) error {
			data, err := tx.Get(ctx, key).Bytes()
			if err != nil {
				return err
			}
			if err := json.Unmarshal(data, &updated); err != nil {
				return err
			}
			if err := mutate(&updated); err != nil {
				return err
			}
			payload, err := json.Marshal(&updated)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, key, payload, 0)
				return nil
			})
			return err
		}, key)
		if err == nil {
			return &updated, nil
		}
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("execution %s update exceeded retry budget", executionID)
}

func (s *RedisStore) updateServer(serverID string, mutate func(*models.Server) error) (*models.Server, error) {
	key := "server:" + serverID
	ctx := context.Background()
	for attempt := 0; attempt < optimisticWriteRetries; attempt++ {
		var updated models.Server
		err := s.client.Watch(ctx, func(tx *redis.Tx) error {
			data, err := tx.Get(ctx, key).Bytes()
			if err != nil {
				return err
			}
			if err := json.Unmarshal(data, &updated); err != nil {
				return err
			}
			if err := mutate(&updated); err != nil {
				return err
			}
			payload, err := json.Marshal(&updated)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, key, payload, 0)
				return nil
			})
			return err
		}, key)
		if err == nil {
			return &updated, nil
		}
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("server %s update exceeded retry budget", serverID)
}

func entryCategory(key string) string {
	prefix, _, found := strings.Cut(key, ":")
	if !found || prefix == "" {
		return "misc"
	}
	return prefix
}
