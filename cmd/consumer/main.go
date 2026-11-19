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

// Command consumer is the entry point for the Archive Main Worker: it
// loads configuration, connects to ClickHouse and RabbitMQ, and runs the
// batch-processing email archive consumer until it receives SIGINT or
// SIGTERM.
package main

import (
	"fmt"
	"os"
	"os/signal"

	"syscall"
	"time"

	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/client"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/config"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/consumer"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/internal/db"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/pkg/logger"
	"github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/pkg/rabbitmq"
)

func main() {

	config.Load()
	// Intiate logger
	logger.Init(logger.Options{
		File:       config.Cfg.LogFile,
		MaxSizeMB:  config.Cfg.LogMaxSizeMB,
		MaxBackups: config.Cfg.LogMaxBackups,
		MaxAgeDays: config.Cfg.LogMaxAgeDays,
		Compress:   config.Cfg.LogCompress,
		Level:      config.Cfg.LogLevel,
		Console:    config.Cfg.LogShowConsole,
	})

	// Initialize ClickHouse Client (Real or Mock)
	var chDB db.ClickHouseDB
	var err error
	chDB, err = db.NewRealClickHouseClient()
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to ClickHouse database")
	}
	logger.Info().Msg("Connected to ClickHouse database successfully")

	defer chDB.Close()

	// Initialize API Client
	apiClient := client.NewAPIClient()

	// RBMQ Connection
	rabbitURL := fmt.Sprintf(
		"amqp://%s:%s@%s:%d/%s",
		config.Cfg.RabbitUsername,
		config.Cfg.RabbitPassword,
		config.Cfg.RabbitHost,
		config.Cfg.RabbitPort,
		config.Cfg.RabbitVHost,
	)
	// logger.Debug().Msg("Connection to :" + rabbitURL)

	// Listen for termination signals before connecting so the retry loop
	// below can be interrupted.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	// Retry the initial connection until the broker is reachable. Once
	// connected, go-rabbitmq reconnects automatically on connection loss
	// using the same delay.
	var conn *rabbitmq.Connection
	for {
		conn, err = rabbitmq.NewConnection(rabbitURL, config.Cfg.AMQPReconnectDelay)
		if err == nil {
			break
		}
		logger.Error().
			Err(err).
			Dur("retry_in", config.Cfg.AMQPReconnectDelay).
			Msg("Failed to connect to RabbitMQ broker; retrying")

		select {
		case <-sig:
			logger.Info().Msg("Shutdown signal received before RabbitMQ connection was established")
			return
		case <-time.After(config.Cfg.AMQPReconnectDelay):
		}
	}
	logger.Info().Msg("Connected to RabbitMQ broker successfully")
	defer conn.Close()

	cons := consumer.NewConsumer(conn, chDB, apiClient)
	if err := cons.Start(); err != nil {
		logger.Fatal().Err(err).Msg("Failed to start email archive consumer")
	}

	// Wait for termination signal
	<-sig

	logger.Info().Msg("Shutting down email archive worker...")
	cons.Stop()
}
