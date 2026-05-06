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

var errBadMessage = errors.New("bad message")

// concurrencyLimit caps simultaneous task goroutines; must match broker.workerConcurrencyLimit.
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

func (w *Worker) Wait() {
	w.consumers.Wait()
	w.tasks.Wait()
}

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
					if errors.Is(err, errBadMessage) {
						d.Nack(false, false)
					} else {
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
		slog.Error("MarkExecutionRunning failed", "component", "worker", "execution_id", msg.ExecutionID, "err", err)
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
		slog.Error("MarkExecutionDone failed", "component", "worker", "execution_id", msg.ExecutionID, "err", err)
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
		slog.Error("PublishResult failed", "component", "worker", "execution_id", msg.ExecutionID, "err", err)
	}
	return nil
}
