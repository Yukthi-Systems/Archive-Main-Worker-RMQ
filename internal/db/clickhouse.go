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

// Package db provides the ClickHouse client used to persist archived mail
// records in bulk.
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/config"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/model"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/pkg/logger"
)

// newEmailArchiveTable constructs an EmailArchiveTable from a Message and its Domain,
// replicating the Rust EmailArchiveTable::new() logic.
func newEmailArchiveTable(msg model.Message, domain model.Domain) model.ClickHouseData {
	archiveLocation := fmt.Sprintf(
		"%s:%s:%s:%s:%s",
		domain.StorageServerIPv4,
		domain.OrganizationId,
		domain.Name,
		formatArchiveTime(msg.TimeStamp),
		msg.MessageID,
	)

	return model.ClickHouseData{
		ArchiveID:       msg.MessageID,
		Domain:          domain.Name,
		ArchiveTime:     msg.TimeStamp,
		FromAddress:     msg.FromAddress,
		ToAddresses:     msg.ToAddresses,
		EmailSubject:    msg.EmailSubject,
		HasAttachments:  msg.HasAttachments,
		Attachments:     msg.Attachments,
		RawEMLSize:      msg.RawEMLSize,
		ArchiveLocation: archiveLocation,
		RetentionDays:   uint16(domain.DataRetentionDays),
	}
}

// formatArchiveTime renders ts in the DD-MM-YYYY layout used throughout
// storage paths and archive locations.
func formatArchiveTime(ts time.Time) string {
	return ts.Format("02-01-2006")
}

// ClickHouseDB defines operations performed on ClickHouse database.
type ClickHouseDB interface {
	BulkInsertMails(ctx context.Context, messages []model.Message, domains map[string]model.Domain) error
	Close() error
}

// RealClickHouseClient connects to a real ClickHouse server.
type RealClickHouseClient struct {
	conn driver.Conn
}

// NewRealClickHouseClient initializes a real ClickHouse connection.
func NewRealClickHouseClient() (*RealClickHouseClient, error) {
	ctx := context.Background()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{config.Cfg.ClickHouseAddr},
		Protocol: clickhouse.HTTP,
		Auth: clickhouse.Auth{
			Database: config.Cfg.ClickHouseDB,
			Username: config.Cfg.ClickHouseUser,
			Password: config.Cfg.ClickHousePassword,
		},
		DialTimeout: 5 * time.Second,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open connection: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}

	return &RealClickHouseClient{conn: conn}, nil
}

// BulkInsertMails builds EmailArchiveTable rows and inserts them as a batch,
// matching the Rust insert_batch_data() approach of writing row-by-row then calling end().
func (c *RealClickHouseClient) BulkInsertMails(ctx context.Context, messages []model.Message, domains map[string]model.Domain) error {
	insert, err := c.conn.PrepareBatch(ctx, "INSERT INTO email_archive")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, msg := range messages {
		domain, ok := domains[msg.DomainID]
		if !ok {
			logger.Warn().
				Str("domain_id", msg.DomainID).
				Str("message_id", msg.MessageID).
				Msg("Skipped saving email to database because domain details could not be found")
			continue
		}

		row := newEmailArchiveTable(msg, domain)

		if err := insert.Append(
			row.ArchiveID,
			row.Domain,
			row.ArchiveTime,
			row.FromAddress,
			row.ToAddresses,
			row.EmailSubject,
			row.HasAttachments,
			row.Attachments,
			row.RawEMLSize,
			row.ArchiveLocation,
			row.RetentionDays,
		); err != nil {
			return fmt.Errorf("batch append message_id=%s: %w", msg.MessageID, err)
		}
	}

	if err := insert.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	return nil
}

// Close closes the ClickHouse database connection.
func (c *RealClickHouseClient) Close() error {
	return c.conn.Close()
}
