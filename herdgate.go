// Package herdgate provides a cache backed by a [Store].
package herdgate

import (
	"time"

	"github.com/giu-fi/herdgate/store/inmemorystore"
)

// A Herdgate is a cache backed by a [Store].
type Herdgate struct {
	store Store
}

// New creates a new [Herdgate] backed by an [inmemorystore.Store].
func New() *Herdgate {
	return &Herdgate{
		store: inmemorystore.New(),
	}
}

// Do returns the cached value for the key, or calls fetch and caches its result for TTL.
//
// A TTL of 0 or less means the result never expires.
func (hg *Herdgate) Do(key string, ttl time.Duration, fetch func() ([]byte, error)) ([]byte, error) {
	if cached, ok := hg.store.Get(key); ok {
		return cached, nil
	}
	fresh, err := fetch()
	if err != nil {
		return nil, err
	}
	hg.store.Set(key, fresh, ttl)
	return fresh, nil
}
