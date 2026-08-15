package herdgate

import "time"

// Store is a key-value store with optional per-key TTL expiry.
type Store interface {
	// Get returns the value stored for the given key, and reports whether it was found.
	//
	// An elapsed TTL is treated the same as a missing key.
	Get(key string) ([]byte, bool)

	// Set stores the value for the given key, replacing any existing value.
	//
	// A TTL of 0 or less means the key never expires.
	Set(key string, value []byte, ttl time.Duration)
}
