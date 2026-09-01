package herdgate

import (
	"context"
	"time"
)

// Coordination is the interface for coordinating fetches through a
// distributed lease, aiming for only one fetch of a given key at a time
// among callers contending for the same lease.
//
// A Coordination must be safe for concurrent use by multiple goroutines.
type Coordination interface {
	// Acquire attempts to take the lease for the given key atomically.
	// ttl determines how long the lease stays valid. The lease must never
	// outlive that window, expiring on its own even with no call to
	// [Coordination.Release]. Callers must provide a ttl greater than
	// zero.
	//
	// acquired reports whether this call won the lease. False with a nil
	// error means another caller currently holds it, which is expected
	// behavior. False with a non-nil error means the acquisition attempt
	// failed.
	//
	// token identifies this acquisition and is required later by
	// [Coordination.Release]. It is empty unless acquired is true, in
	// which case it is unique to this acquisition.
	Acquire(ctx context.Context, key string, ttl time.Duration) (token string, acquired bool, err error)

	// Release attempts to free the lease for the given key, using the
	// token returned by [Coordination.Acquire]. The lease is freed only if
	// the token matches the one currently stored for that key. The
	// comparison and removal must be atomic.
	//
	// A nil error means the lease is no longer held under that token.
	// Either this call freed the lease, or it had already expired.
	//
	// A non-nil error means the release attempt failed.
	Release(ctx context.Context, key, token string) error
}
