package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/broker"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/control"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

// Worker consumes task messages from the three tier queues and simulates execution.
type Worker struct {
	store     *store.RedisStore
	broker    *broker.Broker
	algorithm string
	control   *control.Controller
	consumers sync.WaitGroup
	tasks     sync.WaitGroup
}

func New(s *store.RedisStore, b *broker.Broker, algorithm string, ctrl *control.Controller) *Worker {
	return &Worker{store: s, broker: b, algorithm: algorithm, control: ctrl}
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
	log.Println("[Worker] Started consumers for edge / fog / cloud queues")
}

func (w *Worker) Wait() {
	w.consumers.Wait()
	w.tasks.Wait()
}

func (w *Worker) consumeTier(queueName, consumerTag string, done <-chan struct{}) {
	deliveries, err := w.broker.ConsumeQueue(queueName, consumerTag)
	if err != nil {
		log.Printf("[Worker/%s] Failed to start consumer: %v", consumerTag, err)
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
			d.Ack(false)
			w.tasks.Add(1)
			go func(body []byte) {
				defer w.tasks.Done()
				w.execute(body, done)
			}(d.Body)
		case <-done:
			return
		}
	}
}

func (w *Worker) execute(body []byte, done <-chan struct{}) {
	var msg models.TaskMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		log.Printf("[Worker] Bad message: %v", err)
		return
	}

	execution, err := w.store.GetExecution(msg.ExecutionID)
	if err != nil {
		log.Printf("[Worker] Execution %s not found: %v", msg.ExecutionID, err)
		return
	}
	task, err := w.store.GetTask(msg.TaskID)
	if err != nil {
		log.Printf("[Worker] Task %s not found: %v", msg.TaskID, err)
		return
	}
	srv, err := w.store.GetServer(msg.ServerID)
	if err != nil {
		log.Printf("[Worker] Server %s not found: %v", msg.ServerID, err)
		return
	}
	if w.control != nil && !w.control.Wait(done) {
		return
	}

	startTime := time.Now().UTC()
	if err := w.store.MarkExecutionRunning(msg.ExecutionID, startTime); err != nil {
		log.Printf("[Worker] MarkExecutionRunning %s: %v", msg.ExecutionID, err)
	}

	effectiveDuration := time.Duration(task.Duration+srv.Latency) * time.Millisecond
	log.Printf(
		"[Worker/%-5s] RUNNING  exec=%-12s task=%-10s %-24s server=%-10s duration=%v",
		srv.Type,
		execution.ID,
		task.ID,
		task.Name,
		srv.ID,
		effectiveDuration,
	)

	if w.control != nil {
		if !w.control.Sleep(done, effectiveDuration) {
			return
		}
	} else {
		timer := time.NewTimer(effectiveDuration)
		select {
		case <-timer.C:
		case <-done:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}

	endTime := time.Now().UTC()
	select {
	case <-done:
		return
	default:
	}

	if err := w.store.MarkExecutionDone(msg.ExecutionID, endTime); err != nil {
		log.Printf("[Worker] MarkExecutionDone %s: %v", msg.ExecutionID, err)
	}
	if _, err := w.store.ReleaseServerResources(srv.ID, task.CPU, task.Memory, task.Disk, true); err != nil {
		log.Printf("[Worker] ReleaseServerResources %s: %v", msg.ExecutionID, err)
	}

	log.Printf(
		"[Worker/%-5s] DONE     exec=%-12s task=%-10s effective=%dms",
		srv.Type,
		execution.ID,
		task.ID,
		endTime.Sub(startTime).Milliseconds(),
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
		log.Printf("[Worker] PublishResult %s: %v", msg.ExecutionID, err)
	}
}
