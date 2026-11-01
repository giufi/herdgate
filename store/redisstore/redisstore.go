// Package redisstore provides a Redis-backed implementation of [herdgate.Store].
//
// [herdgate.Store]: https://pkg.go.dev/github.com/giufi/herdgate#Store
package redisstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/giufi/herdgate/internal/validation"
	"github.com/giufi/herdgate/internal/validation/redisvalidation"
	"github.com/redis/go-redis/v9"
)

const _storeKeyPrefix = "herdgate:store:"

// Store is a Redis-backed implementation of [herdgate.Store].
//
// [herdgate.Store]: https://pkg.go.dev/github.com/giufi/herdgate#Store
type Store struct {
	client redis.UniversalClient
}

// New creates a [Store] backed by the given client.
//
// It panics if the client is nil or ignores the context deadline
// (ContextTimeoutEnabled off, or ReadTimeout/WriteTimeout disabling
// SetReadDeadline/SetWriteDeadline calls). The deadline check covers only
// [redis.Client], [redis.ClusterClient] and [redis.Ring].
func New(client redis.UniversalClient) *Store {
	if validation.IsNil(client) {
		panic("redisstore: client must not be nil")
	}
	if err := redisvalidation.CheckContextDeadline(client); err != nil {
		panic("redisstore: " + err.Error())
	}
	return &Store{client: client}
}

// Get returns the value stored for the given key, if present.
//
// The found result reports whether the key was found.
// False with a nil error means the key is missing or its TTL has elapsed.
// False with a non-nil error means the lookup attempt failed.
func (s *Store) Get(ctx context.Context, key string) (value []byte, found bool, err error) {
	val, err := s.client.Get(ctx, storeKey(key)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redisstore: get: %w", err)
	}
	return []byte(val), true, nil
}

// Set stores the value for the given key, replacing any existing value.
//
// A nil value is stored as a non-nil, empty []byte.
//
// A TTL of 0 or less means the key never expires. A positive TTL under one
// millisecond is rounded up to one millisecond. Redis automatically deletes
// the key once the TTL expires.
//
// Keys are stored under a "herdgate:store:" prefix to avoid collisions.
//
// If Set returns a non-nil error, the value may still have been stored.
func (s *Store) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := s.client.Set(ctx, storeKey(key), value, clampTTL(ttl)).Err(); err != nil {
		return fmt.Errorf("redisstore: set: %w", err)
	}
	return nil
}

func clampTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0 // no expiry, as Store.Set requires
	}
	return max(ttl, time.Millisecond) // the minimum TTL go-redis supports
}

func storeKey(key string) string {
	return _storeKeyPrefix + key
}
