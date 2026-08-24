package herdgate

import (
	"context"
	"time"
)

// Store is the interface for storing and retrieving key-value pairs with optional per-key TTL expiry.
type Store interface {
	// Get returns the value stored for the given key, and reports whether it was found.
	//
	// An elapsed TTL is treated the same as a missing key.
	Get(ctx context.Context, key string) ([]byte, bool)

	// Set stores the value for the given key, replacing any existing value.
	//
	// A TTL of 0 or less means the key never expires.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration)
}
