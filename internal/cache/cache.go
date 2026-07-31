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

// Package cache provides a small, generic, concurrent-safe in-memory
// cache with per-item or per-cache time-to-live expiration.
package cache

import (
	"sync"
	"time"
)

// Item represents a cached value with an expiration timestamp.
type Item[T any] struct {
	Value      T
	Expiration int64
}

// TTLCache is a generic, concurrent-safe in-memory cache.
type TTLCache[T any] struct {
	mu          sync.RWMutex
	items       map[string]Item[T]
	defaultTTL  time.Duration
	stopJanitor chan struct{}
}

// NewTTLCache creates a new instance of TTLCache with the specified default TTL and cleanup interval.
func NewTTLCache[T any](defaultTTL time.Duration, cleanupInterval time.Duration) *TTLCache[T] {
	c := &TTLCache[T]{
		items:       make(map[string]Item[T]),
		defaultTTL:  defaultTTL,
		stopJanitor: make(chan struct{}),
	}
	go c.startJanitor(cleanupInterval)
	return c
}

// Set adds or replaces an item in the cache with the default TTL.
func (c *TTLCache[T]) Set(key string, value T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiration int64
	if c.defaultTTL > 0 {
		expiration = time.Now().Add(c.defaultTTL).UnixNano()
	}

	c.items[key] = Item[T]{
		Value:      value,
		Expiration: expiration,
	}
}

// SetWithTTL adds or replaces an item in the cache with a custom TTL.
func (c *TTLCache[T]) SetWithTTL(key string, value T, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiration int64
	if ttl > 0 {
		expiration = time.Now().Add(ttl).UnixNano()
	}

	c.items[key] = Item[T]{
		Value:      value,
		Expiration: expiration,
	}
}

// Get retrieves an item from the cache. Returns the zero value and false if not found or expired.
func (c *TTLCache[T]) Get(key string) (T, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, found := c.items[key]
	if !found {
		var zero T
		return zero, false
	}

	if item.Expiration > 0 && time.Now().UnixNano() > item.Expiration {
		var zero T
		return zero, false
	}

	return item.Value, true
}

// Delete removes an item from the cache.
func (c *TTLCache[T]) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

// Close stops the janitor goroutine.
func (c *TTLCache[T]) Close() {
	close(c.stopJanitor)
}

// startJanitor runs in its own goroutine, periodically sweeping expired
// entries from the cache until Close is called.
func (c *TTLCache[T]) startJanitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now().UnixNano()
			for k, item := range c.items {
				if item.Expiration > 0 && now > item.Expiration {
					delete(c.items, k)
				}
			}
			c.mu.Unlock()
		case <-c.stopJanitor:
			return
		}
	}
}
