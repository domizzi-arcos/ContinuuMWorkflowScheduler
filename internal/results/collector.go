package results

import (
	"encoding/json"
	"log"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/broker"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/control"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

// Collector consumes ResultMessages from result_queue and persists TaskResults to Redis.
// If doneExecIDs is non-nil, every completed execution ID is forwarded to it so
// the workflow tracker can resolve downstream dependencies.
type Collector struct {
	store        *store.RedisStore
	broker       *broker.Broker
	control      *control.Controller
	doneExecIDs  chan<- string
}

func New(s *store.RedisStore, b *broker.Broker, ctrl *control.Controller, doneExecIDs chan<- string) *Collector {
	return &Collector{store: s, broker: b, control: ctrl, doneExecIDs: doneExecIDs}
}

// Start runs the collector until done is closed.
func (c *Collector) Start(done <-chan struct{}) {
	deliveries, err := c.broker.ConsumeQueue(broker.ResultQueue, "result-collector")
	if err != nil {
		log.Printf("[Collector] Failed to start: %v", err)
		return
	}
	log.Println("[Collector] Consuming from result_queue")

	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			if c.control != nil && !c.control.Wait(done) {
				return
			}
			d.Ack(false)
			c.persist(done, d.Body)
		case <-done:
			log.Println("[Collector] Stopped")
			return
		}
	}
}

func (c *Collector) persist(done <-chan struct{}, body []byte) {
	select {
	case <-done:
		return
	default:
	}

	var msg models.ResultMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		log.Printf("[Collector] Bad message: %v", err)
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
		log.Printf("[Collector] StoreTaskResult %s: %v", msg.TaskID, err)
		return
	}

	if c.doneExecIDs != nil {
		select {
		case c.doneExecIDs <- msg.ExecutionID:
		case <-done:
			return
		}
	}

	log.Printf(
		"[Collector] Stored result exec=%-12s task=%-10s target=%-5s server=%-10s effective=%dms",
		msg.ExecutionID,
		msg.TaskID,
		msg.TargetTier,
		msg.ServerID,
		msg.EffectiveDurationMS,
	)
}
