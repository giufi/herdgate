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
	// key. Once it returns, values published afterwards are delivered on the
	// values channel until the unsubscribe function is called or the channel
	// is closed. Delivery is not guaranteed: a value can be lost without the
	// channel closing.
	//
	// Once Subscribe returns, canceling ctx or reaching its deadline must
	// not affect the subscription.
	//
	// The values channel delivers each published value as a copy the
	// caller owns. It must be closed once the unsubscribe function returns,
	// and after any failure the implementation detects.
	//
	// The unsubscribe function stops this subscription without affecting
	// other subscriptions for the same key. It must return even if a value
	// is still waiting to be read on the values channel. It must be
	// idempotent and safe for concurrent use.
	//
	// A non-nil error means the subscribe attempt failed, in which case
	// values and unsubscribe are nil.
	Subscribe(ctx context.Context, key string) (values <-chan []byte, unsubscribe func() error, err error)
}
