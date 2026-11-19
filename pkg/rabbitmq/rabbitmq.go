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

// Package rabbitmq is a thin wrapper around [github.com/wagslane/go-rabbitmq]
// for connecting to RabbitMQ with automatic reconnect, declaring queues,
// consuming messages, and publishing to the dead-letter queue.
package rabbitmq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	gorabbitmq "github.com/wagslane/go-rabbitmq"
)

// Connection wraps a reconnecting [gorabbitmq.Conn], shared between the
// consumer created via [Connection.NewConsumer] and a [gorabbitmq.Publisher]
// used to dead-letter failed messages. Create one with [NewConnection]; call
// [Connection.Close] when done.
type Connection struct {
	URL       string
	conn      *gorabbitmq.Conn
	publisher *gorabbitmq.Publisher
}

// NewConnection dials the given AMQP URL through go-rabbitmq. The returned
// connection reconnects automatically on connection loss, waiting
// reconnectDelay between attempts; consumers and publishers created from it
// recover their channels and subscriptions transparently. A
// [gorabbitmq.Publisher] used by [Connection.PublishHeaders] is opened on
// the same connection.
//
// The initial dial is not retried here; callers that want to wait for the
// broker to come up should retry NewConnection themselves.
func NewConnection(amqpURL string, reconnectDelay time.Duration) (*Connection, error) {
	conn, err := gorabbitmq.NewConn(
		amqpURL,
		gorabbitmq.WithConnectionOptionsLogger(rmqLogger{}),
		gorabbitmq.WithConnectionOptionsBaseReconnectInterval(reconnectDelay),
	)
	if err != nil {
		return nil, err
	}

	publisher, err := gorabbitmq.NewPublisher(
		conn,
		gorabbitmq.WithPublisherOptionsLogger(rmqLogger{}),
	)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	return &Connection{URL: amqpURL, conn: conn, publisher: publisher}, nil
}

// DeclareQueues ensures each named queue exists, declaring it as durable
// if it isn't already present. Existing queues are left untouched (checked
// via a passive declare first) so this is safe to call on every startup.
//
// Declaration happens over a short-lived, non-reconnecting connection:
// the consumer is created with [gorabbitmq.WithConsumerOptionsQueueNoDeclare]
// and nothing consumes from the dead-letter queue, so there is no other
// place for these queues to be declared.
func (c *Connection) DeclareQueues(queues ...string) error {
	rawConn, err := amqp.Dial(c.URL)
	if err != nil {
		return fmt.Errorf("dial for queue declaration: %w", err)
	}
	defer rawConn.Close()

	for _, q := range queues {
		// A failed passive declare closes the channel it was issued on, so
		// each probe runs on its own channel.
		probeCh, err := rawConn.Channel()
		if err != nil {
			return fmt.Errorf("open channel for queue %s check: %w", q, err)
		}
		_, err = probeCh.QueueDeclarePassive(q, true, false, false, false, nil)
		_ = probeCh.Close()
		if err == nil {
			continue
		}

		ch, err := rawConn.Channel()
		if err != nil {
			return fmt.Errorf("open channel for queue %s declaration: %w", q, err)
		}
		_, err = ch.QueueDeclare(q, true, false, false, false, nil)
		_ = ch.Close()
		if err != nil {
			return fmt.Errorf("declare queue %s: %w", q, err)
		}
	}
	return nil
}

// NewConsumer creates a [gorabbitmq.Consumer] on queue, using consumerTag
// as the AMQP consumer identifier, concurrency goroutines to run the
// handler in parallel, and prefetch as the channel's QoS prefetch count.
// The queue is assumed to already exist (see [Connection.DeclareQueues]) and
// is not redeclared. Deliveries are not auto-acknowledged; the handler's
// returned [gorabbitmq.Action] decides Ack/Nack.
func (c *Connection) NewConsumer(queue, consumerTag string, concurrency, prefetch int) (*gorabbitmq.Consumer, error) {
	return gorabbitmq.NewConsumer(
		c.conn,
		queue,
		gorabbitmq.WithConsumerOptionsConsumerName(consumerTag),
		gorabbitmq.WithConsumerOptionsConcurrency(concurrency),
		gorabbitmq.WithConsumerOptionsQOSPrefetch(prefetch),
		gorabbitmq.WithConsumerOptionsQueueNoDeclare,
		gorabbitmq.WithConsumerOptionsLogger(rmqLogger{}),
	)
}

// PublishHeaders sends body to queue on the default exchange as a
// persistent, JSON-content-typed message with the given headers, e.g.
// dead-letter diagnostic fields such as "x-dead-letter-reason".
func (c *Connection) PublishHeaders(ctx context.Context, queue string, body []byte, headers amqp.Table) error {
	return c.publisher.PublishWithContext(
		ctx,
		body,
		[]string{queue},
		gorabbitmq.WithPublishOptionsContentType("application/json"),
		gorabbitmq.WithPublishOptionsPersistentDelivery,
		gorabbitmq.WithPublishOptionsHeaders(gorabbitmq.Table(headers)),
	)
}

// Close gracefully shuts down the publisher and the underlying connection.
func (c *Connection) Close() {
	if c.publisher != nil {
		c.publisher.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}
