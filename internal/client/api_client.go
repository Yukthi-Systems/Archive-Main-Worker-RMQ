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

// Package client provides an HTTP client for talking to the external
// services the consumer depends on: the domain-metadata API and the
// distributed mail storage server.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/config"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/model"
)

// APIClient interacts with external REST endpoints, namely the domain
// metadata API and the mail storage server. It is safe for concurrent use
// by multiple goroutines.
type APIClient struct {
	httpClient *http.Client
}

// NewAPIClient returns a new APIClient configured with a bounded request
// timeout suitable for the batch-processing worker pool.
func NewAPIClient() *APIClient {
	return &APIClient{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// GetDomainDetails fetches metadata for domainID from the domain API
// configured via config.Cfg.APIUrl, including its storage server address
// and data-retention policy. Callers typically cache the result, since this
// performs a network round trip on every call.
func (c *APIClient) GetDomainDetails(ctx context.Context, domainID string) (model.Domain, error) {

	url := fmt.Sprintf("%s/internal/domain/%s/info", config.Cfg.APIUrl, domainID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return model.Domain{}, fmt.Errorf("create get domain request: %w", err)
	}

	if config.Cfg.APIKey != "" {
		req.Header.Set("X-API-Key", config.Cfg.APIKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return model.Domain{}, fmt.Errorf("execute get domain request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return model.Domain{}, fmt.Errorf("domain API returned status code %d", resp.StatusCode)
	}

	var details model.Domain
	if err := json.NewDecoder(resp.Body).Decode(&details); err != nil {
		return model.Domain{}, fmt.Errorf("decode domain details: %w", err)
	}

	return details, nil
}

// StoreMail uploads the raw EML body of msg to domain's storage server,
// authenticating with config.Cfg.ArchiveAPIKey. The remote path is derived
// from the domain's organization, name, and the message timestamp/ID.
func (c *APIClient) StoreMail(ctx context.Context, msg model.Message, domain model.Domain) error {
	url := fmt.Sprintf(
		"http://%s:8686/archive/write/%s/%s/%s/%s",
		domain.StorageServerIPv4,
		domain.OrganizationId,
		domain.Name,
		msg.TimeStamp.Format("02-01-2006"),
		msg.MessageID,
	)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBufferString(msg.EmailContent))
	if err != nil {
		return fmt.Errorf("create store mail request: %w", err)
	}
	req.Header.Set("X-API-KEY", config.Cfg.ArchiveAPIKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute store mail request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("store mail API returned status code %d", resp.StatusCode)
	}
	return nil
}
