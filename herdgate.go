package herdgate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/singleflight"
)

const defaultFetchTimeout = 30 * time.Second

// A Herdgate is a read-through cache backed by a [Store].
type Herdgate struct {
	store   Store
	metrics *metrics
	group   singleflight.Group
}

// DoOptions holds the options for [Herdgate.Do] and [DoDecode].
type DoOptions struct {
	// TTL is how long a fetched value stays cached.
	//
	// If TTL is zero or negative, the value never expires.
	TTL time.Duration

	// Codec is used by [DoDecode] to convert the T value to and from bytes,
	// which are expected by the wrapped [Herdgate.Do].
	//
	// Required when calling [DoDecode]: a nil Codec makes it return an
	// error.
	//
	// [Herdgate.Do] ignores this field.
	Codec Codec
}

// newHerdgate creates a [Herdgate] backed by the given [Store] and metrics.
func newHerdgate(store Store, m *metrics) *Herdgate {
	return &Herdgate{
		store:   store,
		metrics: m,
	}
}

// Do returns the cached value for the key, if present. On a cache miss,
// the value comes from fetch and is cached according to opts.
//
// Caching is best-effort: a failed read is treated as a miss, and a
// value that was fetched successfully is still returned even if it could
// not be cached.
//
// Concurrent Do calls for the same key are collapsed into a single fetch.
// The shared fetch ignores caller cancellation and is bounded by an
// internal timeout. Canceling ctx ends the wait, not the fetch: it runs
// to completion and caches its result.
//
// A panic from fetch is recovered and returned as an error.
func (hg *Herdgate) Do(ctx context.Context, key string, opts DoOptions, fetch func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	if cached, ok := hg.guardedStoreGet(ctx, key); ok {
		return cached, nil
	}

	sharedCtx := context.WithoutCancel(ctx)
	ch := hg.group.DoChan(key, func() (any, error) {
		return hg.fetchAndStore(sharedCtx, key, opts.TTL, fetch)
	})

	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		val, ok := res.Val.([]byte)
		if !ok {
			return nil, fmt.Errorf("shared result was %T, but []byte was expected", res.Val)
		}
		return val, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// DoDecode wraps [Herdgate.Do]: on a cache hit the cached bytes are
// decoded into T with opts.Codec, whereas on a cache miss fetch's result
// is encoded with it, cached, and returned as T.
//
// Even on a miss, the returned T is a copy decoded from the bytes
// [Herdgate.Do] returns, not fetch's original return value.
//
// See [Herdgate.Do] for more details, such as caching, collapsing, and
// panic recovery.
func DoDecode[T any](ctx context.Context, hg *Herdgate, key string, opts DoOptions, fetch func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	if isNil(opts.Codec) {
		return zero, errors.New("herdgate: DoOptions.Codec must not be nil")
	}
	raw, err := hg.Do(ctx, key, opts, func(ctx context.Context) ([]byte, error) {
		fetched, err := fetch(ctx)
		if err != nil {
			return nil, err
		}
		return guardedMarshal(ctx, opts.Codec, fetched)
	})
	if err != nil {
		return zero, err
	}

	var decoded T
	if err := guardedUnmarshal(ctx, opts.Codec, raw, &decoded); err != nil {
		_logger.Error(ctx, "Unmarshal failed", slog.String("key", key), slog.Any("error", err))
		return zero, err
	}
	return decoded, nil
}

// guardedStoreGet attempts to read the stored value for the key, turning a
// panic into a miss.
//
// Caching is best-effort on lookups: a Store that panics degrades Do to
// an uncached fetch instead of failing it.
func (hg *Herdgate) guardedStoreGet(ctx context.Context, key string) (value []byte, found bool) {
	defer func() {
		if r := recover(); r != nil {
			value, found = nil, false
			hg.metrics.incrementCacheLookupError()
			_logger.Error(ctx, "Cache lookup failed, treating it as a miss",
				slog.String("key", key), slog.Any("error", errFromPanic(ctx, r)))
		}
	}()

	value, found = hg.store.Get(ctx, key)
	if found {
		hg.metrics.incrementCacheHit()
		_logger.Debug(ctx, "Cache hit", slog.String("key", key))
	} else {
		hg.metrics.incrementCacheMiss()
		_logger.Debug(ctx, "Cache miss", slog.String("key", key))
	}
	return value, found
}

// fetchAndStore calls fetch and, on success, attempts to cache the result
// for the key.
//
// Caching is best-effort on writes: a failed cache write doesn't prevent
// the fetched value from being returned.
//
// An unrecovered panic makes [singleflight.Group.DoChan] re-panic it on a
// new goroutine instead of delivering it as an error to the caller.
// Thus, it must never panic.
func (hg *Herdgate) fetchAndStore(ctx context.Context, key string, ttl time.Duration, fetch func(ctx context.Context) ([]byte, error)) (_ []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected panic: %w", errFromPanic(ctx, r))
		}
	}()

	value, err := hg.guardedFetch(ctx, fetch)
	if err != nil {
		_logger.Error(ctx, "Fetch failed", slog.String("key", key), slog.Any("error", err))
		return nil, err
	}

	if err := hg.guardedStoreSet(ctx, key, value, ttl); err != nil {
		_logger.Error(ctx, "Cache write failed, returning fetched value", slog.String("key", key), slog.Any("error", err))
		return value, nil
	}
	_logger.Debug(ctx, "Cache write", slog.String("key", key), ttlAttr(ttl))
	return value, nil
}

// guardedFetch runs fetch under a timeout, turning a panic into an error.
//
// Recovery happens here, not in fetchAndStore, so observeFetch labels
// the fetch as an error.
func (hg *Herdgate) guardedFetch(ctx context.Context, fetch func(ctx context.Context) ([]byte, error)) (_ []byte, err error) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("fetch panicked: %w", errFromPanic(ctx, r))
		}
		hg.metrics.observeFetch(start, err)
	}()

	fetchCtx, cancel := context.WithTimeout(ctx, defaultFetchTimeout)
	defer cancel()
	return fetch(fetchCtx)
}

// guardedStoreSet caches the fetched value for the key, turning a panic
// into an error.
//
// Recovery happens here, not in fetchAndStore, so recordCacheWrite labels
// the write as an error.
func (hg *Herdgate) guardedStoreSet(ctx context.Context, key string, value []byte, ttl time.Duration) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("store.Set panicked: %w", errFromPanic(ctx, r))
		}
		hg.metrics.recordCacheWrite(err)
	}()

	hg.store.Set(ctx, key, value, ttl)
	return nil
}
