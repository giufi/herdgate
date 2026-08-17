// Package herdgate provides a cache backed by a [Store].
//
// A TTL controls how long a cached value stays valid before being refetched.
//
// Create [Herdgate] instances through a [Provider], which supplies the same metrics to each.
package herdgate

import (
	"context"
	"log/slog"
	"time"

	"github.com/giufi/herdgate/store/inmemorystore"
)

// A Herdgate is a cache backed by a [Store].
type Herdgate struct {
	store   Store
	metrics *metrics
}

// newHerdgate creates a [Herdgate] backed by a new [inmemorystore.Store] and the
// given metrics collector.
func newHerdgate(m *metrics) *Herdgate {
	return &Herdgate{
		store:   inmemorystore.New(),
		metrics: m,
	}
}

// Do returns the cached value for the key, or calls fetch and caches its result for ttl.
//
// A ttl of 0 or less means the result never expires.
func (hg *Herdgate) Do(ctx context.Context, key string, ttl time.Duration, fetch func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	if cached, ok := hg.store.Get(ctx, key); ok {
		hg.metrics.incrementCacheHit()
		slog.DebugContext(ctx, "Cache hit", _loggerAttr, slog.String("key", key))
		return cached, nil
	}
	hg.metrics.incrementCacheMiss()
	slog.DebugContext(ctx, "Cache miss", _loggerAttr, slog.String("key", key))

	start := time.Now()
	fresh, err := fetch(ctx)
	hg.metrics.observeFetch(start, err)
	if err != nil {
		return nil, err
	}
	hg.store.Set(ctx, key, fresh, ttl)
	slog.DebugContext(ctx, "Cache set", _loggerAttr, slog.String("key", key), ttlAttr(ttl))
	return fresh, nil
}
