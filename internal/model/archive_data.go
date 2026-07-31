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

// Package model defines the data structures shared across the consumer:
// the RabbitMQ message payload, resolved domain metadata, and the row
// shape written to ClickHouse.
package model

import "time"

// Message is the JSON payload consumed from the RabbitMQ processed-mail
// queue. It represents a single email awaiting archival.
type Message struct {
	// DomainID identifies the domain this message belongs to and is used
	// to resolve storage/quota settings via the domain API.
	DomainID     string   `json:"domain_id"`
	FromAddress  string   `json:"from_address"`
	ToAddresses  []string `json:"to_addresses"`
	EmailSubject string   `json:"email_subject"`
	// HasAttachments reports whether Attachments is non-empty.
	HasAttachments bool     `json:"has_attachments"`
	Attachments    []string `json:"attachments"`
	// RawEMLSize is the size in bytes of the original EML content.
	RawEMLSize uint32 `json:"raw_eml_size"`
	// MessageID uniquely identifies this email and is used as the
	// ClickHouse archive ID and as part of the storage server object path.
	MessageID        string    `json:"message_id"`
	TimeStamp        time.Time `json:"timestamp"`
	ClientAddress    string    `json:"client_address"`
	RecipientAddress []string  `json:"recipients"`
	// EmailContent is the raw EML body uploaded verbatim to the storage
	// server by [internal/client.APIClient.StoreMail].
	EmailContent  string `json:"email_content"`
	SenderAddress string `json:"sender"`
	EMLMessageID  string `json:"eml_message_id"`

	BodyText           string   `json:"body_text"`
	BodyHTML           string   `json:"body_html"`
	AttachmentContents []string `json:"attachment_contents"`
}

// Domain holds the metadata resolved from the domain API for a given
// domain ID, describing where and how long a message's mail should be
// retained. Domain values are cached in-memory (see [internal/cache])
// since they change infrequently relative to message volume.
type Domain struct {
	Id             string `json:"domain_id"`
	Name           string `json:"domain_name"`
	OrganizationId string `json:"organization_id"`
	// DataRetentionDays is the number of days archived mail for this
	// domain should be retained before it may be purged.
	DataRetentionDays int `json:"data_retention_days"`
	// StorageServerIPv4 is the address of the storage server this
	// domain's mail content is uploaded to.
	StorageServerIPv4 string `json:"storage_server_ipv4"`
	QuotaAllocated    int    `json:"quota_allocated"`
	QuotaUtilized     int    `json:"quota_utilized"`
}

// ClickHouseData is the row shape inserted into the `email_archive`
// ClickHouse table, built from a [Message] and its resolved [Domain].
type ClickHouseData struct {
	ArchiveID      string
	Domain         string
	ArchiveTime    time.Time
	FromAddress    string
	ToAddresses    []string
	EmailSubject   string
	HasAttachments bool
	Attachments    []string
	RawEMLSize     uint32
	// ArchiveLocation is the composite storage-server/organization/domain
	// path this message's mail content was uploaded to.
	ArchiveLocation string
	// RetentionDays mirrors Domain.DataRetentionDays at the time this row
	// was written.
	RetentionDays uint16
}
