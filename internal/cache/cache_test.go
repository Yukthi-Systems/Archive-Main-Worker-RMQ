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

package cache

import (
	"testing"
	"time"
)

func TestTTLCache_SetAndGet(t *testing.T) {
	c := NewTTLCache[string](1*time.Second, 100*time.Millisecond)
	defer c.Close()

	c.Set("foo", "bar")

	val, found := c.Get("foo")
	if !found {
		t.Fatal("expected to find foo")
	}
	if val != "bar" {
		t.Errorf("expected val to be 'bar', got '%s'", val)
	}
}

func TestTTLCache_Expiration(t *testing.T) {
	c := NewTTLCache[string](50*time.Millisecond, 10*time.Millisecond)
	defer c.Close()

	c.Set("foo", "bar")

	// Verify it exists initially
	val, found := c.Get("foo")
	if !found || val != "bar" {
		t.Fatal("expected 'foo' to be 'bar' initially")
	}

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	_, found = c.Get("foo")
	if found {
		t.Error("expected 'foo' to have expired and not be found")
	}
}

func TestTTLCache_CustomTTL(t *testing.T) {
	c := NewTTLCache[string](1*time.Hour, 10*time.Millisecond)
	defer c.Close()

	c.SetWithTTL("foo", "bar", 20*time.Millisecond)

	val, found := c.Get("foo")
	if !found || val != "bar" {
		t.Fatal("expected 'foo' to be 'bar'")
	}

	time.Sleep(50 * time.Millisecond)

	_, found = c.Get("foo")
	if found {
		t.Error("expected 'foo' to have expired")
	}
}

func TestTTLCache_Delete(t *testing.T) {
	c := NewTTLCache[string](1*time.Hour, 1*time.Second)
	defer c.Close()

	c.Set("foo", "bar")
	c.Delete("foo")

	_, found := c.Get("foo")
	if found {
		t.Error("expected 'foo' to be deleted")
	}
}
