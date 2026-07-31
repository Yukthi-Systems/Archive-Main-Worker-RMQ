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

// Package config loads and holds the application's runtime configuration.
//
// Values are read from OS environment variables, with an optional local
// .env file (see [github.com/joho/godotenv]) loaded first as a convenience
// for local development. Environment variables always take precedence over
// the .env file. See the project README for the full list of supported
// variables and their defaults.
package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all runtime configuration for the consumer service. It is
// populated once by [Load] and is treated as read-only thereafter.
type Config struct {
	// WorkerCount is the number of concurrent batch-processing goroutines.
	WorkerCount int

	// AMQPReconnectDelay is the wait time between RabbitMQ reconnect attempts.
	AMQPReconnectDelay time.Duration
	RabbitHost         string
	RabbitUsername     string
	RabbitPassword     string
	RabbitPort         int
	RabbitVHost        string
	// RabbitConsumeQueue is the queue messages are consumed from.
	RabbitConsumeQueue string
	// RabbitFailedQueue is the dead-letter queue failed messages are routed to.
	RabbitFailedQueue string
	// RabbitPrefetch is the maximum number of unacknowledged deliveries
	// the broker will send to this consumer at once.
	RabbitPrefetch int
	// BatchSize is the maximum number of messages collected before a
	// worker flushes a batch to the storage server and ClickHouse.
	BatchSize int
	// BatchFlushTimeout is the maximum time a worker waits for a batch to
	// fill up before flushing it anyway.
	BatchFlushTimeout time.Duration
	// ConsumerTag identifies this consumer instance to RabbitMQ.
	ConsumerTag string

	// ClickHouse connection settings.
	ClickHouseAddr     string
	ClickHouseUser     string
	ClickHousePassword string
	ClickHouseDB       string

	// APIUrl is the base URL of the domain-metadata API.
	APIUrl string
	// APIKey authenticates requests to the domain-metadata API.
	APIKey string

	// ArchiveAPIKey authenticates requests made to the storage server's
	// archive-write endpoint (see internal/client.APIClient.StoreMail).
	ArchiveAPIKey string

	// CacheTTL is how long resolved domain details are cached in memory.
	CacheTTL time.Duration
	// CacheCleanupInterval is how often expired cache entries are evicted.
	CacheCleanupInterval time.Duration

	LogFile        string
	LogMaxSizeMB   int
	LogMaxBackups  int
	LogMaxAgeDays  int
	LogCompress    bool
	LogLevel       string
	LogShowConsole bool
}

// Cfg is the process-wide configuration instance. It is nil until [Load]
// has been called, which happens once at process startup.
var Cfg *Config

// getenv returns the environment variable named key, or def if it is unset
// or empty.
func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// getint parses the environment variable named key as an int, falling back
// to def if it is unset or fails to parse.
func getint(key string, def int) int {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

// getbool parses the environment variable named key as a bool, falling
// back to def if it is unset or fails to parse.
func getbool(key string, def bool) bool {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

// getdur parses the environment variable named key as a [time.Duration]
// (e.g. "5s", "1h"), falling back to def if it is unset or fails to parse.
func getdur(key string, def time.Duration) time.Duration {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// Load populates the global [Cfg] from environment variables, applying the
// defaults documented in the README for anything unset. It must be called
// once before any other package reads Cfg, typically as the first step in
// main.
func Load() {
	// Load environment variables from a local .env file if one exists.
	// This does not overwrite variables already set in the environment
	// (e.g. by Docker via env_file), so .env is purely a local-dev fallback.
	_ = godotenv.Load()

	Cfg = &Config{
		RabbitConsumeQueue: getenv("RB_CONSUME_QUEUE", "archive_processed_mails"),
		RabbitPrefetch:     getint("RB_PREFETCH", 100),
		RabbitFailedQueue:  getenv("RB_FAILED_QUEUE", "dead_parser"),
		WorkerCount:        getint("RB_WORKER_COUNT", 1),

		RabbitHost:        getenv("RB_HOST", ""),
		RabbitUsername:    getenv("RB_USERNAME", ""),
		RabbitPassword:    getenv("RB_PASSWD", ""),
		RabbitPort:        getint("RB_PORT", 0),
		RabbitVHost:       getenv("RB_VHOST", ""),
		BatchSize:         getint("RB_BATCH_SIZE", 100),
		BatchFlushTimeout: getdur("RB_BATCH_FLUSH_TIMEOUT", 1*time.Second),

		ConsumerTag:        getenv("RB_CONSUMER_TAG", "main-consumer-1"),
		AMQPReconnectDelay: getdur("AMQPR_CON_DELAY", 5*time.Second),

		ClickHouseAddr:     getenv("CLICKHOUSE_ADDR", ""),
		ClickHouseUser:     getenv("CLICKHOUSE_USER", ""),
		ClickHousePassword: getenv("CLICKHOUSE_PASSWORD", ""),
		ClickHouseDB:       getenv("CLICKHOUSE_DB", "default"),

		// API URL
		APIUrl:        getenv("API_URL", ""),
		APIKey:        getenv("API_KEY", ""),
		ArchiveAPIKey: getenv("ARCHIVE_API_KEY", ""),

		// Cache config defaults
		CacheTTL:             getdur("CACHE_TTL", 3*time.Hour),
		CacheCleanupInterval: getdur("CACHE_CLEANUP_INTERVAL", 5*time.Minute),

		LogFile:        getenv("LOG_FILE", "./logs/app.log"),
		LogMaxSizeMB:   getint("LOG_MAX_SIZE_MB", 50),
		LogMaxBackups:  getint("LOG_MAX_BACKUPS", 5),
		LogMaxAgeDays:  getint("LOG_MAX_AGE_DAYS", 14),
		LogCompress:    getbool("LOG_COMPRESS", true),
		LogLevel:       getenv("LOG_LEVEL", "debug"),
		LogShowConsole: getbool("LOG_CONSOLE", false),
	}
}
