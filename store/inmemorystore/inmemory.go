// Package inmemorystore provides an in-memory implementation of [herdgate.Store], backed by a map.
//
// [herdgate.Store]: https://pkg.go.dev/github.com/giufi/herdgate#Store
package inmemorystore

import (
	"bytes"
	"context"
	"log/slog"
	"time"

	"github.com/giufi/herdgate/internal/logging"
)

// Store is an in-memory implementation of [herdgate.Store], backed by a [sync.Map].
//
// Expiration is checked lazily on Get. An expired entry is treated as a miss, but is only removed from the map when a later Set overwrites that key,
// or never, if the key isn't requested again.
//
// [herdgate.Store]: https://pkg.go.dev/github.com/giufi/herdgate#Store
type Store struct {
	entries syncMap[string, storeEntry]
	now     func() time.Time
}

var _logger = logging.New("herdgate/store/inmemorystore")

type storeEntry struct {
	value []byte

	// expiresAt is the [time.Time] at which the entry expires, or the [time.Time] zero value if it never expires.
	expiresAt time.Time
}

// New creates a new [Store].
func New() *Store {
	return &Store{
		now: func() time.Time { return time.Now().UTC() },
	}
}

// Get returns the value stored for the given key, and reports whether it was found.
//
// An elapsed TTL is treated the same as a missing key.
func (s *Store) Get(ctx context.Context, key string) ([]byte, bool) {
	entry, ok := s.entries.Load(ctx, key)
	if !ok {
		return nil, false
	}
	if !entry.expiresAt.IsZero() && s.now().After(entry.expiresAt) {
		_logger.Debug(ctx, "Cache entry expired", slog.String("key", key))
		return nil, false
	}
	return bytes.Clone(entry.value), true
}

// Set stores the value for the given key, replacing any existing value.
//
// A TTL of 0 or less means the key never expires.
func (s *Store) Set(ctx context.Context, key string, value []byte, ttl time.Duration) {
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = s.now().Add(ttl)
	}
	s.entries.Store(key, storeEntry{value: bytes.Clone(value), expiresAt: expiresAt})
}
