// Package redisbroadcast provides a Redis Pub/Sub implementation of
// [herdgate.Broadcast].
//
// [herdgate.Broadcast]: https://pkg.go.dev/github.com/giufi/herdgate#Broadcast
package redisbroadcast

import (
	"context"
	"fmt"
	"sync"

	"github.com/giufi/herdgate/internal/validation"
	"github.com/giufi/herdgate/internal/validation/redisvalidation"
	"github.com/redis/go-redis/v9"
)

const (
	_broadcastChannelPrefix = "herdgate:broadcast:"
	_maxSubscriptions       = 100
)

// Broadcast is a Redis Pub/Sub implementation of [herdgate.Broadcast].
//
// Channels are named under a "herdgate:broadcast:" prefix to avoid collisions.
//
// Each subscription opens its own connection to Redis, on top of the
// client's pool. A Broadcast holds at most 100 subscriptions at a time.
//
// With the default Redis configuration, a value over 32MB isn't delivered:
// it exceeds the subscribers' output buffer limit, so Redis disconnects
// them and their values channels close.
//
// [herdgate.Broadcast]: https://pkg.go.dev/github.com/giufi/herdgate#Broadcast
type Broadcast struct {
	client redis.UniversalClient
	slots  chan struct{} // semaphore capping concurrent subscriptions
}

// New creates a [Broadcast] backed by the given client.
//
// It panics if the client is nil or ignores the context deadline
// (ContextTimeoutEnabled off, or ReadTimeout/WriteTimeout disabling
// SetReadDeadline/SetWriteDeadline calls). The deadline check covers only
// [redis.Client], [redis.ClusterClient] and [redis.Ring].
func New(client redis.UniversalClient) *Broadcast {
	if validation.IsNil(client) {
		panic("redisbroadcast: client must not be nil")
	}
	if err := redisvalidation.CheckContextDeadline(client); err != nil {
		panic("redisbroadcast: " + err.Error())
	}
	return &Broadcast{client: client, slots: make(chan struct{}, _maxSubscriptions)}
}

// Publish sends the value to every active subscription for the given key.
// Having no subscribers is not an error.
//
// A nil value is delivered as a non-nil, empty []byte.
//
// A nil error means Redis accepted the value, not that any subscriber
// received it. If Publish returns a non-nil error, the value may still
// have been delivered.
func (b *Broadcast) Publish(ctx context.Context, key string, value []byte) error {
	if err := b.client.Publish(ctx, broadcastChannel(key), value).Err(); err != nil {
		return fmt.Errorf("redisbroadcast: publish: %w", err)
	}
	return nil
}

// Subscribe starts a subscription to values published for the given key,
// returning once Redis has confirmed it. It fails immediately when the
// [Broadcast] already holds 100 subscriptions.
//
// Canceling ctx interrupts Subscribe, except during the connection setup,
// which runs until the Redis client's timeouts.
//
// Delivery is not guaranteed. A value can be lost without the values
// channel closing, for example:
//   - when the Redis server hangs without closing the connection
//   - after a [redis.Ring] rebalance maps the key to a different shard
//   - when the Redis client reconnects on its own, for example during
//     server maintenance
//
// Callers should wait for a value with a timeout, as well as watch for the
// channel to close.
//
// After the connection drops, the unsubscribe function can block until the
// Redis client's reconnect attempt finishes or times out.
//
// A non-nil error means the subscribe attempt failed, in which case values
// and unsubscribe are nil.
func (b *Broadcast) Subscribe(ctx context.Context, key string) (values <-chan []byte, unsubscribe func() error, err error) {
	select {
	case b.slots <- struct{}{}:
	default:
		return nil, nil, fmt.Errorf("redisbroadcast: subscribe: too many subscriptions (limit %d)", _maxSubscriptions)
	}
	defer func() {
		if unsubscribe == nil {
			<-b.slots
		}
	}()

	pubsub := b.client.Subscribe(ctx, broadcastChannel(key))
	defer func() {
		if unsubscribe == nil {
			_ = pubsub.Close()
		}
	}()

	if err := confirmSubscription(ctx, pubsub); err != nil {
		return nil, nil, fmt.Errorf("redisbroadcast: subscribe: %w", err)
	}

	// The subscription must outlive ctx, so it can't inherit ctx's cancellation or deadline
	values, unsubscribe = b.startForwarding(context.WithoutCancel(ctx), pubsub)
	return values, unsubscribe, nil
}

// confirmSubscription waits until Redis confirms the subscription or ctx is done.
func confirmSubscription(ctx context.Context, pubsub *redis.PubSub) error {
	// Close pubsub when ctx is done, so that Receive stops waiting
	stop := context.AfterFunc(ctx, func() { _ = pubsub.Close() })
	_, err := pubsub.Receive(ctx)
	if !stop() { // ctx is done
		return ctx.Err()
	}
	return err
}

// startForwarding sends the subscription's messages on the returned
// channel in a new goroutine.
//
// When unsubscribe is called or a read fails, the connection is closed,
// then the goroutine frees the slot Subscribe acquired and closes the
// channel. Unsubscribe returns once these steps have finished.
func (b *Broadcast) startForwarding(ctx context.Context, pubsub *redis.PubSub) (values <-chan []byte, unsubscribe func() error) {
	received := make(chan []byte)
	stop := make(chan struct{})
	finished := make(chan struct{})
	end := sync.OnceValue(func() error {
		close(stop)
		if err := pubsub.Close(); err != nil {
			return fmt.Errorf("redisbroadcast: unsubscribe: %w", err)
		}
		return nil
	})

	go func() {
		forward(ctx, pubsub, received, stop)
		_ = end()
		<-b.slots
		close(received)
		close(finished)
	}()

	return received, func() error {
		err := end()
		<-finished
		return err
	}
}

// forward sends the payload of each message it reads to received.
//
// It returns after a read fails or after unsubscribe is called.
func forward(ctx context.Context, pubsub *redis.PubSub, received chan<- []byte, stop <-chan struct{}) {
	for {
		msg, err := pubsub.ReceiveMessage(ctx) // unlike PubSub.Channel, it fails if the connection is lost
		if err != nil {
			return
		}
		select {
		case received <- []byte(msg.Payload):
		case <-stop:
			return
		}
	}
}

func broadcastChannel(key string) string {
	return _broadcastChannelPrefix + key
}
