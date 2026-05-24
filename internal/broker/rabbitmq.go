// Package broker wraps the RabbitMQ topology used by the scheduler.
//
// Two exchanges drive the pipeline:
//
//   - task_exchange (direct): routes a TaskMessage to one of three tier
//     queues — edge_queue, fog_queue, cloud_queue — using a lowercase
//     tier name ("edge", "fog", "cloud") as the routing key.
//   - result_exchange (fanout): broadcasts every ResultMessage to all
//     bound queues; the simulation uses a single result_queue.
//
// The broker keeps one connection and one channel; PublishTask and
// PublishResult serialize publishes through a mutex so the AMQP channel is
// only ever touched by one goroutine at a time.
package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Exchange and queue names declared on broker startup. They are exported so
// the rest of the codebase can refer to them without re-declaring strings.
const (
	TaskExchange   = "task_exchange"
	ResultExchange = "result_exchange"
	EdgeQueue      = "edge_queue"
	FogQueue       = "fog_queue"
	CloudQueue     = "cloud_queue"
	ResultQueue    = "result_queue"
	// workerConcurrencyLimit caps unacknowledged deliveries per channel via
	// AMQP Basic.Qos. It matches the worker-side semaphore so RabbitMQ does
	// not flood a consumer that already has 50 messages in-flight.
	workerConcurrencyLimit = 50
	// publishTimeout bounds a single PublishTask / PublishResult call so a
	// stalled broker cannot block the scheduler or worker goroutine forever.
	publishTimeout = 5 * time.Second
)

// Broker wraps a single RabbitMQ connection and channel.
// All exchanges and queues are declared on construction.
type Broker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	mu      sync.Mutex
}

// NewBroker dials the AMQP URL, opens one channel, declares the exchanges
// and queues used by the simulation, and returns a ready-to-use Broker.
// On any setup failure the underlying connection is closed before returning.
func NewBroker(url string) (*Broker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("rabbitmq channel: %w", err)
	}
	b := &Broker{conn: conn, channel: ch}
	if err := b.setup(); err != nil {
		b.Close()
		return nil, err
	}
	return b, nil
}

// setup declares the two exchanges, the three tier queues, and the result
// queue, and binds them in the topology described in the package doc.
func (b *Broker) setup() error {
	// task_exchange: direct — one queue per tier
	if err := b.channel.ExchangeDeclare(TaskExchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", TaskExchange, err)
	}
	// result_exchange: fanout — all consumers get every result
	if err := b.channel.ExchangeDeclare(ResultExchange, "fanout", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", ResultExchange, err)
	}

	// Tier queues bound to task_exchange by routing key
	tierQueues := []struct{ queue, key string }{
		{EdgeQueue, "edge"},
		{FogQueue, "fog"},
		{CloudQueue, "cloud"},
	}
	for _, t := range tierQueues {
		if _, err := b.channel.QueueDeclare(t.queue, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare queue %s: %w", t.queue, err)
		}
		if err := b.channel.QueueBind(t.queue, t.key, TaskExchange, false, nil); err != nil {
			return fmt.Errorf("bind %s: %w", t.queue, err)
		}
	}

	// Result queue bound to result_exchange (fanout — no routing key needed)
	if _, err := b.channel.QueueDeclare(ResultQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", ResultQueue, err)
	}
	if err := b.channel.QueueBind(ResultQueue, "", ResultExchange, false, nil); err != nil {
		return fmt.Errorf("bind %s: %w", ResultQueue, err)
	}
	// Limit unacknowledged deliveries per channel so the broker doesn't flood consumers.
	if err := b.channel.Qos(workerConcurrencyLimit, 0, false); err != nil {
		return fmt.Errorf("set QoS: %w", err)
	}
	return nil
}

// PublishTask routes a task message to the correct tier queue.
// serverType must be one of "edge", "fog", "cloud".
func (b *Broker) PublishTask(serverType string, body []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()
	return b.channel.PublishWithContext(
		ctx,
		TaskExchange, serverType,
		false, false,
		amqp.Publishing{ContentType: "application/json", Body: body},
	)
}

// PublishResult broadcasts a result message to all result consumers.
func (b *Broker) PublishResult(body []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()
	return b.channel.PublishWithContext(
		ctx,
		ResultExchange, "",
		false, false,
		amqp.Publishing{ContentType: "application/json", Body: body},
	)
}

// ConsumeQueue returns a delivery channel for the given queue name.
func (b *Broker) ConsumeQueue(queueName, consumerTag string) (<-chan amqp.Delivery, error) {
	return b.channel.Consume(queueName, consumerTag, false, false, false, false, nil)
}

// PurgeQueues clears every task and result queue. Called once per session
// start so a fresh simulation never inherits leftover messages from a
// previous run that was interrupted before draining.
func (b *Broker) PurgeQueues() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	queues := []string{EdgeQueue, FogQueue, CloudQueue, ResultQueue}
	for _, queue := range queues {
		if _, err := b.channel.QueuePurge(queue, false); err != nil {
			return fmt.Errorf("purge %s: %w", queue, err)
		}
	}
	return nil
}

// Close releases the channel and connection. Safe to call even if either
// is nil (e.g., if NewBroker failed midway through setup).
func (b *Broker) Close() {
	if b.channel != nil {
		b.channel.Close()
	}
	if b.conn != nil {
		b.conn.Close()
	}
}
