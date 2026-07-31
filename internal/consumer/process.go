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

package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/config"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/model"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/pkg/logger"
	"github.com/google/uuid"
	amqp "github.com/streadway/amqp"
)

// processResult carries the outcome of processing a single delivery
// through unmarshal -> resolve domain -> upload-to-storage. err is nil
// only if all three steps succeeded.
type processResult struct {
	msg      model.Message
	domain   model.Domain
	delivery amqp.Delivery
	err      error
}

// BatchprocessMessage concurrently unmarshals, resolves domain metadata
// for, and uploads to the storage server every delivery in batch, then
// bulk-inserts all successfully processed messages into ClickHouse in a
// single call. Deliveries that fail any step — including the ClickHouse
// insert itself — are individually routed to the dead-letter queue via
// [Consumer.deadLetterMessage]; it does not return an error because
// per-message failures are handled internally rather than propagated.
func (c *Consumer) BatchprocessMessage(ctx context.Context, batch []amqp.Delivery) {
	batchID := uuid.New().String()

	logger.Info().
		Str("batch_id", batchID).
		Int("batch_size", len(batch)).
		Msg("Beginning to archive a new batch of emails")

	results := make(chan processResult, len(batch))
	var wg sync.WaitGroup

	for _, delivery := range batch {
		wg.Add(1)
		go func(delivery amqp.Delivery) {
			defer wg.Done()

			msg, err := unmarshalPayload(delivery.Body)
			if err != nil {
				logger.Warn().
					Str("batch_id", batchID).
					Err(err).
					Msg("Failed to read email data; sending to failure queue")
				results <- processResult{delivery: delivery, err: err}
				return
			}

			logger.Info().
				Str("batch_id", batchID).
				Str("message_id", msg.MessageID).
				Str("domain_id", msg.DomainID).
				Msg("Successfully read email metadata")

			domain, err := c.getDomainDetails(ctx, msg.DomainID, msg.MessageID, batchID)
			if err != nil {
				logger.Error().
					Str("batch_id", batchID).
					Str("message_id", msg.MessageID).
					Str("domain_id", msg.DomainID).
					Err(err).
					Msg("Failed to retrieve domain configuration; sending to failure queue")
				results <- processResult{delivery: delivery, err: err}
				return
			}

			logger.Debug().
				Str("batch_id", batchID).
				Str("message_id", msg.MessageID).
				Str("domain_id", msg.DomainID).
				Msg("Uploading email content to the storage server")

			if err := c.apiClient.StoreMail(ctx, msg, domain); err != nil {
				logger.Error().
					Str("batch_id", batchID).
					Str("message_id", msg.MessageID).
					Str("domain_id", msg.DomainID).
					Err(err).
					Msg("Failed to upload email to storage server; sending to failure queue")
				results <- processResult{delivery: delivery, err: err}
				return
			}

			logger.Info().
				Str("batch_id", batchID).
				Str("message_id", msg.MessageID).
				Str("domain_id", msg.DomainID).
				Msg("Successfully uploaded email to the storage server")

			results <- processResult{msg: msg, domain: domain, delivery: delivery}
		}(delivery)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var successMessages []model.Message
	domains := make(map[string]model.Domain)
	failedCount := 0

	for res := range results {
		if res.err != nil {
			failedCount++
			msgID := ""
			if res.msg.MessageID != "" {
				msgID = res.msg.MessageID
			}
			c.deadLetterMessage(ctx, res.delivery, res.err, "x-storage-success", msgID, batchID)
			continue
		}
		successMessages = append(successMessages, res.msg)
		domains[res.msg.DomainID] = res.domain
	}

	logger.Info().
		Str("batch_id", batchID).
		Int("success_count", len(successMessages)).
		Int("failed_count", failedCount).
		Msg("Finished processing batch, preparing to save to database")

	if len(successMessages) > 0 {
		logger.Info().
			Str("batch_id", batchID).
			Int("count", len(successMessages)).
			Msg("Saving batch of emails to database")

		if err := c.chDB.BulkInsertMails(ctx, successMessages, domains); err != nil {
			logger.Error().
				Str("batch_id", batchID).
				Err(err).
				Msg("Failed to save emails to database; sending entire batch to failure queue")

			for _, delivery := range batch {
				msgID := ""
				if msg, err := unmarshalPayload(delivery.Body); err == nil {
					msgID = msg.MessageID
				}
				c.deadLetterMessage(ctx, delivery, fmt.Errorf("clickhouse insert failed: %w", err), "x-clickdb-success", msgID, batchID)
			}
		} else {
			logger.Info().
				Str("batch_id", batchID).
				Int("inserted_count", len(successMessages)).
				Msg("Successfully saved emails to database")
		}
	}

	logger.Info().
		Str("batch_id", batchID).
		Int("inserted", len(successMessages)).
		Int("failed", failedCount).
		Msg("Archiving process for the email batch has completed")
}

// getDomainDetails returns metadata for domainID, preferring the
// in-memory cache and falling back to the domain API on a cache miss. A
// successful API lookup is cached for subsequent calls. messageID and
// batchID are used only for log correlation.
func (c *Consumer) getDomainDetails(ctx context.Context, domainID string, messageID string, batchID string) (model.Domain, error) {
	if cached, found := c.cache.Get(domainID); found {
		logger.Debug().
			Str("batch_id", batchID).
			Str("message_id", messageID).
			Str("domain_id", domainID).
			Msg("Retrieved domain configuration from memory cache")
		return cached, nil
	}

	logger.Debug().
		Str("batch_id", batchID).
		Str("message_id", messageID).
		Str("domain_id", domainID).
		Msg("Checking domain configuration from the server")

	fetched, err := c.apiClient.GetDomainDetails(ctx, domainID)
	if err != nil {
		return model.Domain{}, fmt.Errorf("get domain details: %w", err)
	}
	if fetched.Id == "" {
		return model.Domain{}, fmt.Errorf("domain %s not found", domainID)
	}

	c.cache.Set(domainID, fetched)
	return fetched, nil
}

// deadLetterMessage republishes msg's original body to the failed queue,
// annotated with an "x-dead-letter-reason" header describing err and a
// status header (e.g. "x-storage-success") set to false to indicate which
// processing step failed.
func (c *Consumer) deadLetterMessage(ctx context.Context, msg amqp.Delivery, err error, status string, messageID string, batchID string) {
	headers := msg.Headers
	if headers == nil {
		headers = amqp.Table{}
	}
	headers["x-dead-letter-reason"] = err.Error()
	headers[status] = false

	fCtx, fCancel := context.WithTimeout(ctx, 5*time.Second)
	defer fCancel()

	logger.Warn().
		Str("batch_id", batchID).
		Str("message_id", messageID).
		Str("status", status).
		Err(err).
		Msg("Sending failed email to failure queue")

	if e := c.publishFailed(fCtx, msg.Body, headers); e != nil {
		logger.Error().
			Str("batch_id", batchID).
			Str("message_id", messageID).
			Err(e).
			Msg("Could not send failed email to failure queue")
	}
}

// unmarshalPayload decodes a raw delivery body into a [model.Message].
func unmarshalPayload(body []byte) (model.Message, error) {
	var payload model.Message
	if err := json.Unmarshal(body, &payload); err != nil {
		return payload, fmt.Errorf("unmarshal message payload: %w", err)
	}
	return payload, nil
}

// publishFailed publishes body and headers to the configured dead-letter
// queue. It is a no-op returning nil if the consumer has no RabbitMQ
// connection.
func (c *Consumer) publishFailed(ctx context.Context, body []byte, headers amqp.Table) error {
	if c.conn == nil {
		logger.Warn().Msg("rabbitmq connection is nil, skipping publishing to failed queue")
		return nil
	}
	err := c.conn.PublishHeaders(
		ctx,
		config.Cfg.RabbitFailedQueue,
		body,
		headers,
	)
	if err != nil {
		return fmt.Errorf("publish to failed queue failed: %w", err)
	}
	return nil
}
