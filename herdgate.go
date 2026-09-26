package herdgate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/giufi/herdgate/internal/validation"
	"golang.org/x/sync/singleflight"
)

const (
	_defaultFetchTimeout            = 30 * time.Second
	_defaultCoordinationCallTimeout = 1 * time.Second

	_defaultHeartbeatInterval    = 500 * time.Millisecond
	_defaultLeaseTTL             = 4 * _defaultHeartbeatInterval
	_defaultHeartbeatCallTimeout = _defaultHeartbeatInterval

	_followerPollInterval   = 100 * time.Millisecond
	_electionRetryInterval  = _defaultLeaseTTL / 2
	_defaultFollowerTimeout = _defaultFetchTimeout + _defaultCoordinationCallTimeout + 1*time.Second
)

// ErrClosed is returned by [Herdgate.Do] and [DoDecode] when a value must
// be fetched after [Herdgate.Close] has been called.
var ErrClosed = errors.New("herdgate: closed")

// A Herdgate is a read-through cache backed by a [Store]. It performs
// local request coalescing for concurrent fetches of the same key, with
// optional support for distributed request coalescing via a [Coordination].
//
// Call [Herdgate.Close] when it is no longer needed.
type Herdgate struct {
	store        Store
	coordination Coordination
	metrics      *metrics
	group        singleflight.Group

	closedMu    sync.Mutex
	closed      bool
	inFlight    int
	flightsDone chan struct{}
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
		store:       store,
		metrics:     m,
		flightsDone: make(chan struct{}),
	}
}

// Do returns the cached value for the key, if present. On a cache miss,
// the value comes from fetch and is cached according to opts.
//
// Caching is best-effort: a failed read is treated as a miss, and a value
// fetched successfully is still returned even if it could not be cached.
//
// Concurrent Do calls for the same key are locally coalesced into a single
// fetch. The shared fetch ignores caller cancellation and is bounded by an
// internal timeout. Canceling ctx ends the wait, not the fetch: it runs
// to completion and caches its result.
//
// If a [Coordination] is configured via the functional option
// [WithCoordination], Do also performs distributed request coalescing: at
// most one caller performs the fetch at a time, and other callers wait
// for its result instead of fetching independently. If the leader
// fetches successfully but fails to cache the result, a contended
// caller waits up to an internal timeout before fetching on its own:
// its total wait can then exceed the fetch timeout alone.
//
// Coordination is best-effort: it is bypassed if acquiring the lease
// fails, and a failed release does not affect the caller. A lease lost
// mid-fetch does not stop the fetch, letting another caller fetch
// concurrently.
//
// Do returns [ErrClosed] on a cache miss, once [Herdgate.Close] has
// been called.
//
// A panic from fetch is recovered and returned as an error.
func (hg *Herdgate) Do(ctx context.Context, key string, opts DoOptions, fetch func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	if cached, ok := hg.guardedStoreGet(ctx, key); ok {
		return cached, nil
	}

	sharedCtx := context.WithoutCancel(ctx)
	coalesced := true
	ch := hg.group.DoChan(key, func() (any, error) {
		coalesced = false
		done, admitted := hg.admitFlight()
		if !admitted {
			return nil, ErrClosed
		}
		defer done()
		return hg.coordinatedFill(sharedCtx, key, opts.TTL, fetch)
	})

	select {
	case res := <-ch:
		if coalesced {
			hg.metrics.incrementCoalescedWaiter()
		}
		if res.Err != nil {
			return nil, res.Err
		}
		val, ok := res.Val.([]byte)
		if !ok {
			return nil, fmt.Errorf("shared result was %T, but []byte was expected", res.Val)
		}
		return val, nil
	case <-ctx.Done():
		_logger.Debug(ctx, "Context done", slog.String("key", key), slog.Any("error", ctx.Err()))
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
// See [Herdgate.Do] for more details, such as caching, coalescing,
// panic recovery, and closing.
func DoDecode[T any](ctx context.Context, hg *Herdgate, key string, opts DoOptions, fetch func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	if validation.IsNil(opts.Codec) {
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

// Close stops admitting new fetches and waits for those already in
// flight: it returns nil once they finish, or ctx's error if ctx
// expires first, leaving those fetches running in the background.
//
// A shared fetch that held a [Coordination] lease stops its heartbeat
// and releases the lease separately. That cleanup can still be running
// when Close returns. Close waits for the fetch, not for the cleanup.
//
// Close is idempotent and safe for concurrent use.
func (hg *Herdgate) Close(ctx context.Context) error {
	hg.closedMu.Lock()
	if !hg.closed {
		hg.closed = true
		if hg.inFlight == 0 {
			close(hg.flightsDone)
		}
	}
	noFlights := hg.inFlight == 0
	hg.closedMu.Unlock()

	if noFlights {
		return nil
	}

	select {
	case <-hg.flightsDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

// admitFlight admits a new flight unless [Herdgate.Close] has been
// called.
//
// The returned done must be called exactly once when the flight
// completes. done is a no-op when admitted is false.
func (hg *Herdgate) admitFlight() (done func(), admitted bool) {
	hg.closedMu.Lock()
	defer hg.closedMu.Unlock()

	if hg.closed {
		return func() {}, false
	}
	hg.inFlight++

	return func() {
		hg.closedMu.Lock()
		defer hg.closedMu.Unlock()

		hg.inFlight--
		if hg.closed && hg.inFlight == 0 {
			close(hg.flightsDone)
		}
	}, true
}

// coordinatedFill attempts to fill the specified key's cache entry: it
// obtains the value, either by calling fetch directly or, if a
// [Coordination] is configured, by coordinating with other callers
// first, and finally caches it. See [Herdgate.coordinate] for more
// details.
//
// Caching is best-effort on writes: a failed cache write doesn't prevent
// the fetched value from being returned.
//
// An unrecovered panic makes [singleflight.Group.DoChan] re-panic it on a
// new goroutine instead of delivering it as an error to the caller.
// Thus, it must never panic.
func (hg *Herdgate) coordinatedFill(ctx context.Context, key string, ttl time.Duration, fetch func(ctx context.Context) ([]byte, error)) (_ []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected panic: %w", errFromPanic(ctx, r))
		}
	}()

	value, found, release := hg.coordinate(ctx, key)
	// The lease must be released only after fetching and caching the
	// value, so, if successfully cached, it is available to followers.
	// release is a no-op unless this caller was the leader.
	defer release()
	if found {
		return value, nil
	}

	value, err = hg.guardedFetch(ctx, fetch)
	if err != nil {
		_logger.Error(ctx, "Fetch failed", slog.String("key", key), slog.Any("error", err))
		return nil, err
	}
	_logger.Debug(ctx, "Fetch succeeded", slog.String("key", key))

	if err := hg.guardedStoreSet(ctx, key, value, ttl); err != nil {
		_logger.Error(ctx, "Cache write failed, returning fetched value", slog.String("key", key), slog.Any("error", err))
		return value, nil
	}
	_logger.Debug(ctx, "Cache write", slog.String("key", key), ttlAttr(ttl))
	return value, nil
}

// coordinate attempts to acquire the [Coordination] lease for key, if
// configured, so that at most one caller fetches it at a time:
//   - No Coordination configured: bypassed.
//   - Won: the returned release keeps the lease alive by renewing it
//     periodically until called. If renewal stops mid-fetch and the
//     lease expires, another caller may fetch concurrently.
//   - Lost (contended by another caller): wait for the lease holder to
//     cache the value, retrying the acquisition in case the holder
//     stopped renewing, for about _defaultFollowerTimeout in total.
//   - Acquisition failed (error or panic): bypass Coordination. If the
//     failure was due to a timeout, the lease may have been granted
//     anyway and stays unreleased until it expires.
//
// found reports that a follower obtained the leader's value, in
// which case the caller must return it without ever calling fetch.
//
// release is a no-op unless the lease was won. When it was, calling
// release stops the heartbeat and releases the lease, on a separate
// goroutine that can outlast this call.
func (hg *Herdgate) coordinate(ctx context.Context, key string) (value []byte, found bool, release func()) {
	if hg.coordination == nil {
		return nil, false, func() {}
	}

	waitCtx, cancel := context.WithTimeout(ctx, _defaultFollowerTimeout)
	defer cancel()

	token, acquired, acquireErr := hg.guardedAcquire(ctx, key)
	// A failed poll or a failed lease acquisition ends the loop on
	// purpose, leaving the caller to later fetch directly.
	for acquireErr == nil && !acquired {
		var pollErr error
		value, found, pollErr = hg.awaitLeaderValue(waitCtx, key, _electionRetryInterval)
		if found {
			_logger.Debug(ctx, "Follower value found", slog.String("key", key))
			return value, true, func() {}
		}
		if pollErr != nil || waitCtx.Err() != nil {
			break
		}
		token, acquired, acquireErr = hg.guardedAcquireRetry(ctx, key)
	}

	switch {
	case acquireErr != nil:
		_logger.Error(ctx, "Coordination.Acquire failed, fetching directly", slog.String("key", key), slog.Any("error", acquireErr))
	case acquired:
		return nil, false, hg.holdLease(ctx, key, token)
	default:
		_logger.Debug(ctx, "Follower gave up waiting, fetching directly", slog.String("key", key))
	}
	return nil, false, func() {}
}

// holdLease starts renewing the lease already acquired for the
// specified key and token.
//
// The returned function stops the renewal and releases the lease, on
// a separate goroutine to avoid delaying callers coalesced on the
// specified key.
func (hg *Herdgate) holdLease(ctx context.Context, key, token string) (release func()) {
	stopHeartbeat := hg.startHeartbeat(ctx, key, token)
	return func() {
		go func() {
			stopHeartbeat()
			hg.guardedRelease(ctx, key, token)
		}()
	}
}

// guardedAcquire attempts to acquire the [Coordination] lease for the
// specified key under a timeout, turning a panic into an error.
func (hg *Herdgate) guardedAcquire(ctx context.Context, key string) (token string, acquired bool, err error) {
	return hg.guardedAcquireWith(ctx, key, hg.metrics.recordCoordinationAcquire)
}

// guardedAcquireRetry mirrors [Herdgate.guardedAcquire], for a lease
// acquisition retry operation.
// Retries are recorded under a different coordination operation.
func (hg *Herdgate) guardedAcquireRetry(ctx context.Context, key string) (token string, acquired bool, err error) {
	return hg.guardedAcquireWith(ctx, key, hg.metrics.recordCoordinationAcquireRetry)
}

// guardedAcquireWith attempts to acquire the [Coordination] lease for
// the specified key under a timeout, turning a panic into an error,
// and recording the outcome via recordAcquire.
func (hg *Herdgate) guardedAcquireWith(ctx context.Context, key string, recordAcquire func(start time.Time, acquired bool, err error)) (token string, acquired bool, err error) {
	start := time.Now()
	acquireCtx, cancel := context.WithTimeout(ctx, _defaultCoordinationCallTimeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			token, acquired = "", false
			err = fmt.Errorf("coordination.Acquire panicked: %w", errFromPanic(ctx, r))
		}
		recordAcquire(start, acquired, err)
	}()

	token, acquired, err = hg.coordination.Acquire(acquireCtx, key, _defaultLeaseTTL)
	if err != nil {
		return "", false, err
	}
	if acquired {
		_logger.Debug(ctx, "Lease acquired", slog.String("key", key), slog.String("token", token))
	} else {
		_logger.Debug(ctx, "Lease contended", slog.String("key", key))
	}
	return token, acquired, nil
}

// startHeartbeat starts a goroutine that renews the key's [Coordination]
// lease at a fixed interval until stop is called or the lease is
// confirmed or presumed lost.
//
// The returned stop function is idempotent and safe for concurrent use.
// It cancels the context of any in-flight [Coordination.Renew] call and
// waits for the goroutine to exit before returning. That wait is unbounded
// if [Coordination.Renew] ignores ctx cancellation.
func (hg *Herdgate) startHeartbeat(ctx context.Context, key, token string) (stop func()) {
	heartbeatCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(_defaultHeartbeatInterval)
		defer ticker.Stop()

		lastRenewed := time.Now()
		for {
			select {
			case <-ticker.C:
				renewed, err := hg.guardedRenew(heartbeatCtx, key, token)
				switch {
				case err != nil:
					if heartbeatCtx.Err() != nil { // stop() called
						return
					}
					if time.Since(lastRenewed) > _defaultLeaseTTL {
						_logger.Debug(ctx, "Lease presumed lost, no renewal confirmed within lease TTL", slog.String("key", key), slog.String("token", token))
						return
					}
					_logger.Error(ctx, "Coordination.Renew failed, retrying", slog.String("key", key), slog.String("token", token), slog.Any("error", err))
				case !renewed:
					_logger.Debug(ctx, "Lease lost", slog.String("key", key), slog.String("token", token))
					return
				default:
					lastRenewed = time.Now()
				}
			case <-heartbeatCtx.Done():
				return
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

// guardedRenew attempts to renew the [Coordination] lease for the
// specified key and token under a timeout of one heartbeat interval,
// turning a panic into an error.
func (hg *Herdgate) guardedRenew(ctx context.Context, key, token string) (renewed bool, err error) {
	start := time.Now()
	renewCtx, cancel := context.WithTimeout(ctx, _defaultHeartbeatCallTimeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			renewed, err = false, fmt.Errorf("coordination.Renew panicked: %w", errFromPanic(ctx, r))
		}
		hg.metrics.recordCoordinationRenew(start, renewed, err)
	}()

	renewed, err = hg.coordination.Renew(renewCtx, key, token, _defaultLeaseTTL)
	if err != nil {
		return false, err
	}
	return renewed, nil
}

// guardedRelease attempts to release the Coordination lease for the
// specified key under a timeout. A failure is logged and recorded,
// without ever affecting [Herdgate.Do]'s caller.
func (hg *Herdgate) guardedRelease(ctx context.Context, key, token string) {
	start := time.Now()
	releaseCtx, cancel := context.WithTimeout(ctx, _defaultCoordinationCallTimeout)
	defer cancel()

	var err error
	defer func() {
		if r := recover(); r != nil {
			panicErr := errFromPanic(ctx, r)
			err = fmt.Errorf("coordination.Release panicked: %w", panicErr)
			_logger.Error(ctx, "Coordination.Release panicked",
				slog.String("key", key), slog.String("token", token), slog.Any("error", panicErr))
		}
		hg.metrics.recordCoordinationRelease(start, err)
	}()

	if err = hg.coordination.Release(releaseCtx, key, token); err != nil {
		_logger.Error(ctx, "Coordination.Release failed",
			slog.String("key", key), slog.String("token", token), slog.Any("error", err))
		return
	}
	_logger.Debug(ctx, "Lease released", slog.String("key", key), slog.String("token", token))
}

// awaitLeaderValue polls [Store], under a timeout, for the value
// another caller is expected to cache after winning the [Coordination]
// lease, turning a panic into an error.
//
// found reports whether the value was retrieved from [Store].
//
// A non-nil error means the poll failed.
func (hg *Herdgate) awaitLeaderValue(ctx context.Context, key string, timeout time.Duration) (value []byte, found bool, err error) {
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			panicErr := errFromPanic(ctx, r)
			value, found = nil, false
			err = fmt.Errorf("store.Get panicked: %w", panicErr)
			_logger.Error(ctx, "Poll panicked", slog.String("key", key), slog.Any("error", panicErr))
		}
	}()

	ticker := time.NewTicker(_followerPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if value, found := hg.store.Get(pollCtx, key); found {
				return value, true, nil
			}
		case <-pollCtx.Done():
			return nil, false, nil
		}
	}
}

// guardedFetch runs fetch under a timeout, turning a panic into an error.
//
// Recovery happens here, not in coordinatedFill, so observeFetch labels
// the fetch as an error.
func (hg *Herdgate) guardedFetch(ctx context.Context, fetch func(ctx context.Context) ([]byte, error)) (_ []byte, err error) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("fetch panicked: %w", errFromPanic(ctx, r))
		}
		hg.metrics.observeFetch(start, err)
	}()

	fetchCtx, cancel := context.WithTimeout(ctx, _defaultFetchTimeout)
	defer cancel()
	return fetch(fetchCtx)
}

// guardedStoreSet caches the fetched value for the key, turning a panic
// into an error.
//
// Recovery happens here, not in coordinatedFill, so recordCacheWrite labels
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
