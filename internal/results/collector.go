// Package results owns the result-side half of the simulation pipeline.
//
// Collector subscribes to RabbitMQ's result_queue, decodes each
// ResultMessage produced by the workers into a TaskResult, persists it to
// Redis, and (optionally) fans the completed execution ID out to the
// workflow tracker so DAG dependencies can be resolved.
//
// Exporter walks Redis at the end of a run and serialises all collected
// TaskResults plus aggregate statistics into a JSON file under results/.
package results

import (
	"encoding/json"
	"log/slog"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/broker"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/control"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

// Collector consumes ResultMessages from result_queue and persists TaskResults to Redis.
// If doneExecIDs is non-nil, every completed execution ID is forwarded to it so
// the workflow tracker can resolve downstream dependencies.
type Collector struct {
	store       *store.RedisStore
	broker      *broker.Broker
	control     *control.Controller
	doneExecIDs chan<- string
}

// New builds a Collector. doneExecIDs may be nil when the simulation has no
// workflow tracker (e.g. flat-task experiments); otherwise every completed
// execution ID is forwarded to it so downstream BLOCKED executions can be
// promoted to PENDING.
func New(s *store.RedisStore, b *broker.Broker, ctrl *control.Controller, doneExecIDs chan<- string) *Collector {
	return &Collector{store: s, broker: b, control: ctrl, doneExecIDs: doneExecIDs}
}

// Start runs the collector until done is closed.
func (c *Collector) Start(done <-chan struct{}) {
	deliveries, err := c.broker.ConsumeQueue(broker.ResultQueue, "result-collector")
	if err != nil {
		slog.Error("failed to start collector", "component", "collector", "err", err)
		return
	}
	slog.Info("consuming from result_queue", "component", "collector")

	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			if c.control != nil && !c.control.Wait(done) {
				return
			}
			c.persist(done, d.Body)
			d.Ack(false)
		case <-done:
			slog.Info("collector stopped", "component", "collector")
			return
		}
	}
}

// persist decodes one result delivery, stores the resulting TaskResult in
// Redis, and (if a tracker channel is wired) signals the completed execution
// ID downstream. Persistence errors are logged but never fatal: the execution
// is already DONE in Redis from the worker side, and the tracker still needs
// to be notified so the DAG can progress.
func (c *Collector) persist(done <-chan struct{}, body []byte) {
	select {
	case <-done:
		return
	default:
	}

	var msg models.ResultMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		slog.Warn("bad message", "component", "collector", "err", err)
		return
	}

	result := &models.TaskResult{
		ExecutionID:         msg.ExecutionID,
		TaskID:              msg.TaskID,
		TaskName:            msg.TaskName,
		TargetTier:          msg.TargetTier,
		ServerID:            msg.ServerID,
		ServerType:          msg.ServerType,
		WorkflowID:          msg.WorkflowID,
		Algorithm:           msg.Algorithm,
		StartTime:           msg.StartTime,
		EndTime:             msg.EndTime,
		DurationMS:          msg.DurationMS,
		EffectiveDurationMS: msg.EffectiveDurationMS,
		CostUSD:             msg.CostUSD,
		EnergyWh:            msg.EnergyWh,
		DataflowPath:        msg.DataflowPath,
	}

	select {
	case <-done:
		return
	default:
	}

	if err := c.store.StoreTaskResult(result); err != nil {
		slog.Error("StoreTaskResult failed", "component", "collector", "task_id", msg.TaskID, "err", err)
		// Fall through: execution is already DONE in Redis; tracker must still be notified
		// so downstream BLOCKED executions can be promoted to PENDING.
	}

	if c.doneExecIDs != nil {
		select {
		case c.doneExecIDs <- msg.ExecutionID:
		case <-done:
			return
		}
	}

	slog.Debug("result stored", "component", "collector",
		"execution_id", msg.ExecutionID, "task_id", msg.TaskID,
		"target_tier", msg.TargetTier, "server_id", msg.ServerID,
		"effective_ms", msg.EffectiveDurationMS,
	)
}
