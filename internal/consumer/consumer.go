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

	"time"

	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/cache"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/client"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/config"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/db"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/model"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/pkg/logger"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/pkg/rabbitmq"
	amqp "github.com/streadway/amqp"
)

// Consumer coordinates one or more batch-processing workers that consume
// messages from RabbitMQ and archive them via apiClient and chDB. A single
// Consumer owns one RabbitMQ connection and one domain-metadata cache
// shared across all its workers.
type Consumer struct {
	conn      *rabbitmq.Connection
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
		ctx:       ctx,
		cancel:    cancel,
		chDB:      chDB,
		apiClient: apiClient,
		cache:     cache.NewTTLCache[model.Domain](config.Cfg.CacheTTL, config.Cfg.CacheCleanupInterval),
	}
}

// Start declares the configured consume and dead-letter queues, begins
// consuming from RabbitMQ, and launches config.Cfg.WorkerCount background
// workers to process deliveries in batches. It returns once workers have
// been launched; use [Consumer.Stop] to shut them down.
func (c *Consumer) Start() error {
	if err := c.conn.DeclareQueues(
		config.Cfg.RabbitConsumeQueue,
		config.Cfg.RabbitFailedQueue,
	); err != nil {
		return fmt.Errorf("declare queues: %w", err)
	}

	msgs, err := c.conn.Consume(
		config.Cfg.RabbitConsumeQueue,
		config.Cfg.ConsumerTag,
	)
	if err != nil {
		return err
	}

	for i := 0; i < config.Cfg.WorkerCount; i++ {
		go c.worker(i, msgs)
	}

	return nil
}

// Stop signals all workers to finish their current batch and return, and
// stops the domain-metadata cache's cleanup goroutine. It does not block
// until workers have exited; callers that need that (e.g. main) should
// wait separately.
func (c *Consumer) Stop() {
	c.cancel()
	if c.cache != nil {
		c.cache.Close()
	}
}

// worker consumes deliveries from msgs, accumulating them into a batch of
// up to config.Cfg.BatchSize messages and flushing early if
// config.Cfg.BatchFlushTimeout elapses first. It runs until msgs is closed
// or the consumer's context is canceled, flushing any partial batch before
// returning.
func (c *Consumer) worker(id int, msgs <-chan amqp.Delivery) {
	logger.Debug().Int("worker_id", id).Msg("Background worker is now active and waiting for new emails")

	batchSize := config.Cfg.BatchSize
	flushTimeout := config.Cfg.BatchFlushTimeout

	batch := make([]amqp.Delivery, 0, batchSize)

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

		for _, msg := range batch {
			if err := msg.Ack(false); err != nil {
				logger.Error().
					Int("worker_id", id).
					Err(err).
					Msg("Failed to acknowledge email processing to queue manager")
			}
		}

		batch = batch[:0]
	}

	for {
		select {

		case <-c.ctx.Done():
			flush()
			return

		case <-timer.C:
			flush()
			timer.Reset(flushTimeout)

		case msg, ok := <-msgs:
			if !ok {
				flush()
				return
			}

			batch = append(batch, msg)

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
