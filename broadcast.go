package herdgate

import "context"

// Broadcast is the interface for delivering a fetched value directly to
// other callers waiting on the same key, instead of through [Store].
//
// A subscription must never receive a value published before
// [Broadcast.Subscribe] was called.
//
// A Broadcast must be safe for concurrent use by multiple goroutines.
type Broadcast interface {
	// Publish sends the value to every active subscription for the given
	// key. Having no subscribers is not an error.
	//
	// A nil error means the value was handed off for delivery, not
	// that any subscriber received it.
	Publish(ctx context.Context, key string, value []byte) error

	// Subscribe starts a subscription to values published for the given
	// key. It returns only once the subscription is active, not merely
	// requested.
	//
	// The values channel delivers each published value as a copy the
	// caller owns, and closes when the subscription ends. It must end
	// after any failure that may have lost a value.
	//
	// unsubscribe ends this subscription without affecting other
	// subscriptions for the same key. It is idempotent and safe for
	// concurrent use.
	//
	// A non-nil error means the subscribe attempt failed, in which case
	// values and unsubscribe are nil.
	Subscribe(ctx context.Context, key string) (values <-chan []byte, unsubscribe func() error, err error)
}
