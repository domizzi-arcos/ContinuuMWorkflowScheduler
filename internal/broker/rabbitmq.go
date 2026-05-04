package broker

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	TaskExchange   = "task_exchange"
	ResultExchange = "result_exchange"
	EdgeQueue      = "edge_queue"
	FogQueue       = "fog_queue"
	CloudQueue     = "cloud_queue"
	ResultQueue    = "result_queue"
)

// Broker wraps a single RabbitMQ connection and channel.
// All exchanges and queues are declared on construction.
type Broker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

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
	return nil
}

// PublishTask routes a task message to the correct tier queue.
// serverType must be one of "edge", "fog", "cloud".
func (b *Broker) PublishTask(serverType string, body []byte) error {
	return b.channel.PublishWithContext(
		context.Background(),
		TaskExchange, serverType,
		false, false,
		amqp.Publishing{ContentType: "application/json", Body: body},
	)
}

// PublishResult broadcasts a result message to all result consumers.
func (b *Broker) PublishResult(body []byte) error {
	return b.channel.PublishWithContext(
		context.Background(),
		ResultExchange, "",
		false, false,
		amqp.Publishing{ContentType: "application/json", Body: body},
	)
}

// ConsumeQueue returns a delivery channel for the given queue name.
func (b *Broker) ConsumeQueue(queueName, consumerTag string) (<-chan amqp.Delivery, error) {
	return b.channel.Consume(queueName, consumerTag, false, false, false, false, nil)
}

func (b *Broker) PurgeQueues() error {
	queues := []string{EdgeQueue, FogQueue, CloudQueue, ResultQueue}
	for _, queue := range queues {
		if _, err := b.channel.QueuePurge(queue, false); err != nil {
			return fmt.Errorf("purge %s: %w", queue, err)
		}
	}
	return nil
}

func (b *Broker) Close() {
	if b.channel != nil {
		b.channel.Close()
	}
	if b.conn != nil {
		b.conn.Close()
	}
}
