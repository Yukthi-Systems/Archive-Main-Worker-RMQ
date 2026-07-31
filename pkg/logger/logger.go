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

// Package logger provides a process-wide structured (JSON) logger built on
// [zerolog], writing to a size- and age-based rotating log file via
// [lumberjack]. Call [Init] once at startup, then use the package-level
// shorthand functions (Info, Warn, Error, etc.) anywhere in the
// application to log through the shared instance.
package logger

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Options configures the global logger created by [Init].
type Options struct {
	// File is the path log entries are written to.
	File string
	// MaxSizeMB is the maximum size in megabytes of a log file before it
	// is rotated.
	MaxSizeMB int
	// MaxBackups is the maximum number of rotated log files to retain.
	MaxBackups int
	// MaxAgeDays is the maximum age in days to retain rotated log files.
	MaxAgeDays int
	// Compress enables gzip compression of rotated log files.
	Compress bool
	// Level is the minimum log level to emit (e.g. "debug", "info",
	// "warn", "error"). Invalid or empty values fall back to info.
	Level string
	// Console, if true, mirrors log output to stderr in addition to File.
	Console bool
}

var (
	log *zerolog.Logger
	mu  sync.RWMutex
)

// Init initializes (or reinitializes) the global JSON logger used by the
// package-level shorthand functions. It is safe to call again to change
// log settings at runtime, but is typically called once at startup before
// any other goroutine logs.
func Init(opts Options) {
	lj := &lumberjack.Logger{
		Filename:   opts.File,
		MaxSize:    opts.MaxSizeMB,
		MaxBackups: opts.MaxBackups,
		MaxAge:     opts.MaxAgeDays,
		Compress:   opts.Compress,
	}

	var out zerolog.LevelWriter
	if opts.Console {
		// JSON to both file and stderr
		out = zerolog.MultiLevelWriter(os.Stderr, lj)
	} else {
		// JSON only to file
		out = zerolog.MultiLevelWriter(lj)
	}

	level := zerolog.InfoLevel
	if l, err := zerolog.ParseLevel(strings.ToLower(opts.Level)); err == nil {
		level = l
	}

	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.TimestampFieldName = "time"

	newLogger := zerolog.New(out).Level(level).With().Timestamp().Logger()

	mu.Lock()
	log = &newLogger
	mu.Unlock()

	log.Info().Msg("Logger initialized")
}

// get returns a thread-safe pointer to the global logger.
func get() *zerolog.Logger {
	mu.RLock()
	defer mu.RUnlock()
	if log == nil {
		tmp := zerolog.New(os.Stdout).With().Timestamp().Logger()
		log = &tmp
	}
	return log
}

// Trace starts a new event at trace level on the global logger.
func Trace() *zerolog.Event { return get().Trace() }

// Debug starts a new event at debug level on the global logger.
func Debug() *zerolog.Event { return get().Debug() }

// Info starts a new event at info level on the global logger.
func Info() *zerolog.Event { return get().Info() }

// Warn starts a new event at warn level on the global logger.
func Warn() *zerolog.Event { return get().Warn() }

// Error starts a new event at error level on the global logger.
func Error() *zerolog.Event { return get().Error() }

// Fatal starts a new event at fatal level on the global logger. Calling
// Msg/Msgf/Send on the returned event terminates the process via os.Exit(1)
// after logging, per zerolog's Fatal semantics.
func Fatal() *zerolog.Event { return get().Fatal() }

// Panic starts a new event at panic level on the global logger. Calling
// Msg/Msgf/Send on the returned event panics after logging, per zerolog's
// Panic semantics.
func Panic() *zerolog.Event { return get().Panic() }
