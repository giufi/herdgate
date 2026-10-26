package herdgate

import (
	"context"
	"time"
)

// Store is the interface for storing and retrieving key-value pairs with optional per-key TTL expiry.
//
// A Store must be safe for concurrent use by multiple goroutines.
type Store interface {
	// Get returns the value stored for the given key.
	//
	// Modifying the returned value must not affect the Store or any value returned by another call.
	//
	// The found result reports whether the key was found.
	// False with a nil error means the key is missing or its TTL has elapsed,
	// which is expected behavior.
	// False with a non-nil error means the lookup attempt failed.
	Get(ctx context.Context, key string) (value []byte, found bool, err error)

	// Set stores the value for the given key, replacing any existing value.
	//
	// It must not modify the value, or retain it after returning.
	//
	// A nil value must be stored as a non-nil, empty []byte.
	//
	// A TTL of 0 or less means the key never expires.
	//
	// If Set returns a non-nil error, the value may still have been stored.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
}
