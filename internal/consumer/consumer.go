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

// Package consumer implements the batching RabbitMQ consumer that drives
// the archive pipeline: reading processed-mail messages, resolving domain
// metadata, uploading mail content to the storage server, and bulk-writing
// records to ClickHouse.
package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/cache"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/client"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/config"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/db"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/model"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/pkg/logger"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	gorabbitmq "github.com/wagslane/go-rabbitmq"
)

// batchItem is a single delivery handed from a go-rabbitmq handler
// goroutine to a batch worker. The worker closes done once the batch
// containing the delivery has been fully processed, which lets the
// handler return and go-rabbitmq acknowledge the message.
type batchItem struct {
	delivery amqp.Delivery
	done     chan struct{}
}

// Consumer coordinates one or more batch-processing workers that consume
// messages from RabbitMQ and archive them via apiClient and chDB. A single
// Consumer owns one RabbitMQ consumer and one domain-metadata cache shared
// across all its workers.
//
// Consumption is driven by go-rabbitmq, which reconnects transparently on
// connection loss. Its handler goroutines forward each delivery to the
// batch workers and block until the batch is flushed, then Ack it.
type Consumer struct {
	conn      *rabbitmq.Connection
	consumer  *gorabbitmq.Consumer
	items     chan batchItem
	workers   sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	chDB      db.ClickHouseDB
	apiClient *client.APIClient
	cache     *cache.TTLCache[model.Domain]
}

// NewConsumer builds a Consumer wired to the given RabbitMQ connection,
// ClickHouse client, and API client. It does not start consuming until
// [Consumer.Start] is called.
func NewConsumer(conn *rabbitmq.Connection, chDB db.ClickHouseDB, apiClient *client.APIClient) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{
		conn:      conn,
		items:     make(chan batchItem),
		ctx:       ctx,
		cancel:    cancel,
		chDB:      chDB,
		apiClient: apiClient,
		cache:     cache.NewTTLCache[model.Domain](config.Cfg.CacheTTL, config.Cfg.CacheCleanupInterval),
	}
}

// Start declares the configured consume and dead-letter queues, registers
// a reconnecting go-rabbitmq consumer on the consume queue, and launches
// config.Cfg.WorkerCount background workers to process deliveries in
// batches. It returns once workers have been launched; use [Consumer.Stop]
// to shut them down.
func (c *Consumer) Start() error {
	if err := c.conn.DeclareQueues(
		config.Cfg.RabbitConsumeQueue,
		config.Cfg.RabbitFailedQueue,
	); err != nil {
		return fmt.Errorf("declare queues: %w", err)
	}

	// Each handler goroutine holds one unacknowledged delivery while its
	// batch is pending, so concurrency must cover the prefetch window for
	// batches to fill up to BatchSize.
	concurrency := config.Cfg.RabbitPrefetch
	if concurrency < config.Cfg.WorkerCount {
		concurrency = config.Cfg.WorkerCount
	}

	rc, err := c.conn.NewConsumer(
		config.Cfg.RabbitConsumeQueue,
		config.Cfg.ConsumerTag,
		concurrency,
		config.Cfg.RabbitPrefetch,
	)
	if err != nil {
		return err
	}
	c.consumer = rc

	for i := 0; i < config.Cfg.WorkerCount; i++ {
		c.workers.Add(1)
		go c.worker(i)
	}

	go func() {
		// Run blocks until the consumer is closed, restarting consumption
		// after every reconnect; it only returns an error if the initial
		// subscription fails.
		if err := rc.Run(c.handle); err != nil {
			logger.Fatal().Err(err).Msg("Failed to start consuming from the email queue")
		}
	}()

	return nil
}

// Stop stops consuming, waits for in-flight batches to be processed and
// acknowledged, then stops all workers and the domain-metadata cache's
// cleanup goroutine.
func (c *Consumer) Stop() {
	if c.consumer != nil {
		// Close waits for every in-flight handler to return, i.e. for the
		// workers to flush the batches those deliveries belong to.
		c.consumer.Close()
	}
	c.cancel()
	c.workers.Wait()
	if c.cache != nil {
		c.cache.Close()
	}
}

// handle is the go-rabbitmq handler. It passes the delivery to a batch
// worker and blocks until that worker has processed the batch containing
// it. Failed messages have already been routed to the dead-letter queue
// by then, so the original is always acknowledged.
func (c *Consumer) handle(d gorabbitmq.Delivery) gorabbitmq.Action {
	item := batchItem{delivery: d.Delivery, done: make(chan struct{})}

	select {
	case c.items <- item:
	case <-c.ctx.Done():
		return gorabbitmq.NackRequeue
	}

	<-item.done
	return gorabbitmq.Ack
}

// worker receives deliveries from the handlers, accumulating them into a
// batch of up to config.Cfg.BatchSize messages and flushing early if
// config.Cfg.BatchFlushTimeout elapses first. It runs until the consumer's
// context is canceled, flushing any partial batch before returning.
func (c *Consumer) worker(id int) {
	defer c.workers.Done()

	logger.Debug().Int("worker_id", id).Msg("Background worker is now active and waiting for new emails")

	batchSize := config.Cfg.BatchSize
	flushTimeout := config.Cfg.BatchFlushTimeout

	batch := make([]amqp.Delivery, 0, batchSize)
	dones := make([]chan struct{}, 0, batchSize)

	timer := time.NewTimer(flushTimeout)
	defer timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		logger.Info().
			Int("worker_id", id).
			Int("batch_size", len(batch)).
			Msg("Triggered archiving process for a batch of emails")

		ctx, cancel := context.WithTimeout(c.ctx, 60*time.Second)

		// Implement this similar to the Rust process_batch_message()
		c.BatchprocessMessage(ctx, batch)
		cancel()

		// Release the handlers so go-rabbitmq acknowledges each message.
		for _, done := range dones {
			close(done)
		}

		batch = batch[:0]
		dones = dones[:0]
	}

	for {
		select {

		case <-c.ctx.Done():
			flush()
			return

		case <-timer.C:
			flush()
			timer.Reset(flushTimeout)

		case item := <-c.items:
			batch = append(batch, item.delivery)
			dones = append(dones, item.done)

			if len(batch) >= batchSize {
				flush()

				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}

				timer.Reset(flushTimeout)
			}
		}
	}
}
