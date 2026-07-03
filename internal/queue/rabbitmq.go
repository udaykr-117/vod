package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeVideoUploaded = "video.uploaded"

	QueueHLS       = "video.uploaded.hls"
	QueueThumbnail = "video.uploaded.thumbnail"
	QueueCaption   = "video.uploaded.caption"

	ExchangeDLX = "video.uploaded.dlx"
	QueueDLQ    = "video.uploaded.dlq"
)

// queueNames lists every worker queue bound to ExchangeVideoUploaded so
// topology setup and tests don't have to repeat the list.
var queueNames = []string{QueueHLS, QueueThumbnail, QueueCaption}

// Delivery re-exports amqp091-go's Delivery so callers (workers) depend on
// this package's API surface, not the underlying client library directly.
type Delivery = amqp.Delivery

type Conn struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

// Dial connects and declares the full topology: a durable fanout exchange
// (every worker independently consumes every upload event), one durable
// queue per worker type, and a dead-letter exchange/queue that messages land
// in after exhausting retries via per-queue x-dead-letter-exchange.
func Dial(url string) (*Conn, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}

	c := &Conn{conn: conn, ch: ch}
	if err := c.declareTopology(); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Conn) declareTopology() error {
	if err := c.ch.ExchangeDeclare(ExchangeVideoUploaded, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", ExchangeVideoUploaded, err)
	}
	if err := c.ch.ExchangeDeclare(ExchangeDLX, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", ExchangeDLX, err)
	}
	if _, err := c.ch.QueueDeclare(QueueDLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %s: %w", QueueDLQ, err)
	}
	if err := c.ch.QueueBind(QueueDLQ, "", ExchangeDLX, false, nil); err != nil {
		return fmt.Errorf("bind queue %s: %w", QueueDLQ, err)
	}

	for _, q := range queueNames {
		_, err := c.ch.QueueDeclare(q, true, false, false, false, amqp.Table{
			"x-dead-letter-exchange": ExchangeDLX,
		})
		if err != nil {
			return fmt.Errorf("declare queue %s: %w", q, err)
		}
		if err := c.ch.QueueBind(q, "", ExchangeVideoUploaded, false, nil); err != nil {
			return fmt.Errorf("bind queue %s: %w", q, err)
		}
	}
	return nil
}

func (c *Conn) Close() {
	if c.ch != nil {
		c.ch.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

// Publish marshals v as JSON and publishes it to ExchangeVideoUploaded as a
// persistent message, so it survives a RabbitMQ restart while undelivered.
func (c *Conn) Publish(ctx context.Context, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	return c.ch.PublishWithContext(ctx, ExchangeVideoUploaded, "", false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         body,
	})
}

// Consume returns a delivery channel for queueName. Workers must call
// d.Ack(false) only after their own DB commit succeeds, and d.Nack(false,
// true) to requeue on transient failure — never Ack before the commit, so a
// crash mid-job results in safe redelivery rather than silent data loss.
func (c *Conn) Consume(queueName, consumerTag string) (<-chan amqp.Delivery, error) {
	if err := c.ch.Qos(1, 0, false); err != nil {
		return nil, fmt.Errorf("set qos: %w", err)
	}
	deliveries, err := c.ch.Consume(queueName, consumerTag, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("consume %s: %w", queueName, err)
	}
	return deliveries, nil
}
