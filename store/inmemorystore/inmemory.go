// Package inmemorystore provides an in-memory implementation of [herdgate.Store], backed by a map.
//
// [herdgate.Store]: https://pkg.go.dev/github.com/giufi/herdgate#Store
package inmemorystore

import (
	"context"
	"log/slog"
	"time"
)

// Store is an in-memory implementation of [herdgate.Store], backed by a map.
//
// Expiration is lazy: a key is evicted the next time Get is called for it, so a key that expires without being read again stays in memory.
//
// [herdgate.Store]: https://pkg.go.dev/github.com/giufi/herdgate#Store
type Store struct {
	entries map[string]storeEntry
	now     func() time.Time
}

var _loggerAttr = slog.String("logger", "herdgate/store/inmemorystore")

type storeEntry struct {
	value []byte

	// expiresAt is the [time.Time] at which the entry expires, or the [time.Time] zero value if it never expires.
	expiresAt time.Time
}

// New creates a new [Store].
func New() *Store {
	return &Store{
		entries: make(map[string]storeEntry),
		now:     func() time.Time { return time.Now().UTC() },
	}
}

// Get returns the value stored for the given key, and reports whether it was found.
//
// An elapsed TTL is treated the same as a missing key.
func (s *Store) Get(ctx context.Context, key string) ([]byte, bool) {
	entry, ok := s.entries[key]
	if !ok {
		return nil, false
	}
	if !entry.expiresAt.IsZero() && s.now().After(entry.expiresAt) {
		delete(s.entries, key)
		slog.DebugContext(ctx, "Cache entry expired", _loggerAttr, slog.String("key", key))
		return nil, false
	}
	return entry.value, true
}

// Set stores the value for the given key, replacing any existing value.
//
// A TTL of 0 or less means the key never expires.
func (s *Store) Set(ctx context.Context, key string, value []byte, ttl time.Duration) {
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = s.now().Add(ttl)
	}
	s.entries[key] = storeEntry{value: value, expiresAt: expiresAt}
}
