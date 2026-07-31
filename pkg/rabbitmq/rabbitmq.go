// Copyright (C) 2026 Yukthi Systems Private Limited
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 3
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// version 3 along with this program. If not, see
// <https://www.gnu.org/licenses/>.

// Package rabbitmq is a thin wrapper around [github.com/streadway/amqp]
// that exposes only the queue-declare, consume, and publish operations the
// archive consumer needs, with a single channel per connection.
package rabbitmq

import (
	"context"
	"fmt"

	"github.com/streadway/amqp"
)

// Connection wraps a single AMQP connection and its one channel. It is not
// safe for concurrent publishing/consuming from multiple goroutines beyond
// what the underlying amqp.Channel itself guarantees.
type Connection struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewConnection dials the RabbitMQ broker at url, opens a channel, and
// applies the given prefetch (QoS) limit. url is a standard AMQP URI, e.g.
// "amqp://user:pass@host:port/vhost".
func NewConnection(url string, prefetch int) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	if err := ch.Qos(prefetch, 0, false); err != nil {
		return nil, err
	}

	return &Connection{conn: conn, ch: ch}, nil
}

// DeclareQueues ensures each named queue exists, declaring it as durable
// if it isn't already present. Existing queues are left untouched (checked
// via a passive declare first) so this is safe to call on every startup.
func (c *Connection) DeclareQueues(queues ...string) error {
	for _, q := range queues {
		_, err := c.ch.QueueDeclarePassive(q, true, false, false, false, nil)
		if err == nil {
			continue
		}

		if _, err := c.ch.QueueDeclare(
			q, true, false, false, false, nil,
		); err != nil {
			return fmt.Errorf("declare queue %s: %w", q, err)
		}
	}
	return nil
}

// Consume starts delivering messages from queue, identifying this
// consumer with the given tag. Deliveries are not auto-acknowledged;
// callers must Ack/Nack each amqp.Delivery themselves.
func (c *Connection) Consume(queue, consumer string) (<-chan amqp.Delivery, error) {
	return c.ch.Consume(queue, consumer, false, false, false, false, nil)
}

// Publish sends body to queue as a persistent, JSON-content-typed message,
// aborting early if ctx is already done.
func (c *Connection) Publish(ctx context.Context, queue string, body []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return c.ch.Publish(
		"",
		queue,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}

// PublishHeaders behaves like Publish but attaches the given headers,
// e.g. dead-letter diagnostic fields such as "x-dead-letter-reason".
func (c *Connection) PublishHeaders(ctx context.Context, queue string, body []byte, headers amqp.Table) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return c.ch.Publish(
		"",
		queue,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
			Headers:      headers,
		},
	)
}

// Close closes the channel and connection, ignoring any errors from an
// already-closed underlying connection.
func (c *Connection) Close() {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}
