// Package worker simulates task execution. Three consumer goroutines — one
// per tier queue (edge, fog, cloud) — pull TaskMessages from RabbitMQ, mark
// each execution RUNNING in Redis, sleep for `task.Duration + server.Latency`
// to mimic real work, then mark it DONE and emit a ResultMessage on
// result_exchange. A semaphore caps concurrent in-flight tasks per worker so
// the broker's prefetch limit and the worker pool stay aligned.
package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/broker"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/control"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

// errBadMessage signals a TaskMessage that could not be decoded. The worker
// nacks the delivery without requeue so it does not get re-tried forever.
var errBadMessage = errors.New("bad message")

// errAlreadyDone signals a duplicate delivery for an execution that has
// already reached the DONE state. The worker acks (without re-running) so
// the broker stops redelivering it.
var errAlreadyDone = errors.New("execution already done")

// concurrencyLimit caps simultaneous task goroutines; must match
// broker.workerConcurrencyLimit so RabbitMQ does not prefetch more messages
// than the worker can actually process at once.
const concurrencyLimit = 50

// Worker consumes task messages from the three tier queues and simulates execution.
type Worker struct {
	store     *store.RedisStore
	broker    *broker.Broker
	algorithm string
	control   *control.Controller
	consumers sync.WaitGroup
	tasks     sync.WaitGroup
	sem       chan struct{} // bounded task concurrency
}

// New constructs a Worker bound to the given Redis store, RabbitMQ broker,
// and pause/resume controller. The algorithm string is recorded verbatim in
// every emitted ResultMessage so the results dataset can be partitioned by
// algorithm name during analysis.
func New(s *store.RedisStore, b *broker.Broker, algorithm string, ctrl *control.Controller) *Worker {
	return &Worker{
		store:     s,
		broker:    b,
		algorithm: algorithm,
		control:   ctrl,
		sem:       make(chan struct{}, concurrencyLimit),
	}
}

// StartAll launches one consumer goroutine per tier (edge, fog, cloud).
func (w *Worker) StartAll(done <-chan struct{}) {
	tiers := []struct{ queue, tag string }{
		{broker.EdgeQueue, "edge-consumer"},
		{broker.FogQueue, "fog-consumer"},
		{broker.CloudQueue, "cloud-consumer"},
	}
	for _, t := range tiers {
		w.consumers.Add(1)
		go func(queue, tag string) {
			defer w.consumers.Done()
			w.consumeTier(queue, tag, done)
		}(t.queue, t.tag)
	}
	slog.Info("started consumers for edge/fog/cloud queues", "component", "worker")
}

// Wait blocks until every consumer goroutine and every spawned task goroutine
// has exited. Used by simulationSession.StopAll to drain in-flight work before
// the simulation finishes.
func (w *Worker) Wait() {
	w.consumers.Wait()
	w.tasks.Wait()
}

// WaitWithTimeout is Wait with a deadline: returns true when draining
// completed cleanly, false if the timeout expired first. Used by the drain
// loop in cmd/main.go so an experiment cannot hang indefinitely on stuck tasks.
func (w *Worker) WaitWithTimeout(timeout time.Duration) bool {
	drained := make(chan struct{})
	go func() {
		w.consumers.Wait()
		w.tasks.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		return true
	case <-time.After(timeout):
		return false
	}
}

// consumeTier is the per-tier consumer loop. It acquires a slot on w.sem
// before launching the execution goroutine — the semaphore is what bounds
// in-flight work to concurrencyLimit across the entire process, even though
// each tier has its own goroutine. On unrecoverable decode errors the
// delivery is nacked without requeue; on transient failures it is requeued.
func (w *Worker) consumeTier(queueName, consumerTag string, done <-chan struct{}) {
	deliveries, err := w.broker.ConsumeQueue(queueName, consumerTag)
	if err != nil {
		slog.Error("failed to start consumer", "component", "worker", "consumer", consumerTag, "err", err)
		return
	}
	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			if w.control != nil && !w.control.Wait(done) {
				return
			}
			w.sem <- struct{}{} // acquire slot; blocks when at concurrencyLimit
			w.tasks.Add(1)
			go func(d amqp.Delivery) {
				defer func() {
					<-w.sem // release slot
					w.tasks.Done()
				}()
				if err := w.execute(d.Body, done); err != nil {
					switch {
					case errors.Is(err, errBadMessage):
						// Undecodable — drop without requeue so it doesn't
						// bounce forever in a poison-message loop.
						d.Nack(false, false)
					case errors.Is(err, errAlreadyDone):
						// Duplicate delivery for an already-completed
						// execution. Ack so the broker stops resending.
						d.Ack(false)
					default:
						// Transient failure — requeue and let another attempt
						// reconcile. The idempotency check above prevents
						// double-execution if the original side effects partly
						// landed.
						d.Nack(false, true)
					}
					return
				}
				d.Ack(false)
			}(d)
		case <-done:
			return
		}
	}
}

// execute simulates one task end-to-end:
//  1. decode the TaskMessage and fetch Task/Server/Execution from Redis,
//  2. mark the execution RUNNING,
//  3. sleep for task.Duration + server.Latency (pause-aware),
//  4. mark the execution DONE and release the reserved server resources,
//  5. publish a ResultMessage carrying cost, energy, and dataflow trace.
//
// Cost = CostPerHour × elapsed seconds / 3600; energy = PowerWatts × elapsed
// seconds / 3600. Both are computed from the actual elapsed wall time so a
// paused or slow execution shows up in the results dataset accurately.
func (w *Worker) execute(body []byte, done <-chan struct{}) error {
	var msg models.TaskMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		slog.Warn("bad message", "component", "worker", "err", err)
		return errBadMessage
	}

	execution, err := w.store.GetExecution(msg.ExecutionID)
	if err != nil {
		slog.Error("execution not found", "component", "worker", "execution_id", msg.ExecutionID, "err", err)
		return fmt.Errorf("get execution: %w", err)
	}
	// Idempotency: if the broker redelivers a message for an execution that
	// already finished (worker crashed between MarkExecutionDone and Ack,
	// say), don't re-run it. Re-running would re-release server resources
	// (clamped to zero, but already corrupted), re-emit a result row, and
	// double-count CompletedTasks on the server.
	if execution.Status == "DONE" {
		slog.Info("skipping duplicate delivery", "component", "worker", "execution_id", msg.ExecutionID)
		return errAlreadyDone
	}
	task, err := w.store.GetTask(msg.TaskID)
	if err != nil {
		slog.Error("task not found", "component", "worker", "task_id", msg.TaskID, "err", err)
		return fmt.Errorf("get task: %w", err)
	}
	srv, err := w.store.GetServer(msg.ServerID)
	if err != nil {
		slog.Error("server not found", "component", "worker", "server_id", msg.ServerID, "err", err)
		return fmt.Errorf("get server: %w", err)
	}
	if w.control != nil && !w.control.Wait(done) {
		return nil
	}

	startTime := time.Now().UTC()
	if err := w.store.MarkExecutionRunning(msg.ExecutionID, startTime); err != nil {
		// Cannot leave the reservation hanging — Nack so the message comes
		// back; the idempotency check at the top handles the retry, and on
		// fresh failure the resources are released below.
		slog.Error("MarkExecutionRunning failed", "component", "worker", "execution_id", msg.ExecutionID, "err", err)
		if _, relErr := w.store.ReleaseServerResources(srv.ID, task.CPU, task.Memory, task.Disk, false); relErr != nil {
			slog.Error("ReleaseServerResources (on mark-running failure) failed", "component", "worker", "execution_id", msg.ExecutionID, "err", relErr)
		}
		return fmt.Errorf("mark running: %w", err)
	}

	effectiveDuration := time.Duration(task.Duration+srv.Latency) * time.Millisecond
	slog.Debug("task running", "component", "worker",
		"tier", srv.Type, "execution_id", execution.ID, "task_id", task.ID,
		"task_name", task.Name, "server_id", srv.ID, "duration", effectiveDuration,
	)

	if w.control != nil {
		if !w.control.Sleep(done, effectiveDuration) {
			return nil
		}
	} else {
		timer := time.NewTimer(effectiveDuration)
		select {
		case <-timer.C:
		case <-done:
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		}
	}

	endTime := time.Now().UTC()
	select {
	case <-done:
		return nil
	default:
	}

	if err := w.store.MarkExecutionDone(msg.ExecutionID, endTime); err != nil {
		// If the DONE write fails the result row would dangle — better to
		// requeue and let the redelivery path retry it.
		slog.Error("MarkExecutionDone failed", "component", "worker", "execution_id", msg.ExecutionID, "err", err)
		return fmt.Errorf("mark done: %w", err)
	}
	if _, err := w.store.ReleaseServerResources(srv.ID, task.CPU, task.Memory, task.Disk, true); err != nil {
		slog.Error("ReleaseServerResources failed", "component", "worker", "execution_id", msg.ExecutionID, "err", err)
	}

	slog.Debug("task done", "component", "worker",
		"tier", srv.Type, "execution_id", execution.ID, "task_id", task.ID,
		"effective_ms", endTime.Sub(startTime).Milliseconds(),
	)

	effectiveSec := endTime.Sub(startTime).Seconds()
	costUSD := srv.CostPerHour * effectiveSec / 3600.0
	energyWh := float64(srv.PowerWatts) * effectiveSec / 3600.0

	tierLower := strings.ToLower(srv.Type)
	result := models.ResultMessage{
		ExecutionID:         execution.ID,
		TaskID:              task.ID,
		TaskName:            task.Name,
		TargetTier:          task.TargetTier,
		ServerID:            srv.ID,
		ServerType:          srv.Type,
		WorkflowID:          execution.WorkflowID,
		Algorithm:           w.algorithm,
		StartTime:           startTime,
		EndTime:             endTime,
		DurationMS:          int64(task.Duration),
		EffectiveDurationMS: endTime.Sub(startTime).Milliseconds(),
		CostUSD:             costUSD,
		EnergyWh:            energyWh,
		DataflowPath: []string{
			"generator", "redis", "scheduler", "task_exchange",
			fmt.Sprintf("%s_queue", tierLower), srv.ID,
			"result_exchange", "result_queue", "result_store",
		},
	}
	resultBody, _ := json.Marshal(result)
	if err := w.broker.PublishResult(resultBody); err != nil {
		// Without the result the collector never sees this execution and the
		// drain-complete predicate (results >= done) stalls. Returning an
		// error requeues the original task message; on redelivery the
		// idempotency check skips re-running and we'll retry the publish
		// here. The collector and Redis state both tolerate that.
		slog.Error("PublishResult failed; requesting requeue", "component", "worker", "execution_id", msg.ExecutionID, "err", err)
		return fmt.Errorf("publish result: %w", err)
	}
	return nil
}
