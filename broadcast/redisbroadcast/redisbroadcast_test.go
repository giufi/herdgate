package redisbroadcast

import (
	"bytes"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

const _key = "key"

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestBroadcast_NewNilClientPanic(t *testing.T) {
	assert.PanicsWithValue(t, "redisbroadcast: client must not be nil", func() { New(nil) })
}

func TestBroadcast_NewClientIgnoringContextDeadlinePanic(t *testing.T) {
	client := redis.NewClient(&redis.Options{})
	t.Cleanup(func() { _ = client.Close() })

	assert.PanicsWithValue(t, "redisbroadcast: client must set ContextTimeoutEnabled", func() { New(client) })
}

func TestBroadcast_PublishValueDelivered(t *testing.T) {
	_, client := setupRedis(t)
	broadcast := New(client)
	values, unsubscribe, err := broadcast.Subscribe(t.Context(), _key)
	require.NoError(t, err)
	defer func() { require.NoError(t, unsubscribe()) }()

	testCases := map[string][]byte{
		"text":   []byte("value"),
		"binary": {0x00, 0xff, '\r', '\n', 0x80},
		"empty":  {},
	}
	for name, want := range testCases {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, broadcast.Publish(t.Context(), _key, want))

			got, ok := receive(t, values)
			require.True(t, ok)
			assert.Equal(t, want, got)
		})
	}
}

func TestBroadcast_PublishNilValue(t *testing.T) {
	_, client := setupRedis(t)
	broadcast := New(client)
	values, unsubscribe, err := broadcast.Subscribe(t.Context(), _key)
	require.NoError(t, err)
	defer func() { require.NoError(t, unsubscribe()) }()

	require.NoError(t, broadcast.Publish(t.Context(), _key, nil))

	got, ok := receive(t, values)
	require.True(t, ok)
	assert.Equal(t, []byte{}, got)
}

func TestBroadcast_PublishMultipleSubscribers(t *testing.T) {
	_, client := setupRedis(t)
	broadcast := New(client)
	first, unsubscribeFirst, err := broadcast.Subscribe(t.Context(), _key)
	require.NoError(t, err)
	defer func() { require.NoError(t, unsubscribeFirst()) }()
	second, unsubscribeSecond, err := broadcast.Subscribe(t.Context(), _key)
	require.NoError(t, err)
	defer func() { require.NoError(t, unsubscribeSecond()) }()

	require.NoError(t, broadcast.Publish(t.Context(), _key, []byte("value")))

	for _, values := range []<-chan []byte{first, second} {
		got, ok := receive(t, values)
		require.True(t, ok)
		assert.Equal(t, []byte("value"), got)
	}
}

func TestBroadcast_PublishNoSubscribers(t *testing.T) {
	_, client := setupRedis(t)

	require.NoError(t, New(client).Publish(t.Context(), _key, []byte("value")))
}

func TestBroadcast_PublishConnectionError(t *testing.T) {
	server, client := setupRedis(t)
	broadcast := New(client)

	server.Close()

	require.ErrorContains(t, broadcast.Publish(t.Context(), _key, []byte("value")), "redisbroadcast: publish: ")
}

func TestBroadcast_PublishContextCanceled(t *testing.T) {
	_, client := setupRedis(t)
	broadcast := New(client)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, broadcast.Publish(ctx, _key, []byte("value")), context.Canceled)
}

func TestBroadcast_SubscribeChannelPrefix(t *testing.T) {
	server, client := setupRedis(t)
	_, unsubscribe, err := New(client).Subscribe(t.Context(), _key)
	require.NoError(t, err)
	defer func() { require.NoError(t, unsubscribe()) }()

	assert.Equal(t, []string{"herdgate:broadcast:" + _key}, server.PubSubChannels("*"))
}

func TestBroadcast_SubscribeMultipleKeys(t *testing.T) {
	_, client := setupRedis(t)
	broadcast := New(client)
	values, unsubscribe, err := broadcast.Subscribe(t.Context(), _key)
	require.NoError(t, err)
	defer func() { require.NoError(t, unsubscribe()) }()
	otherValues, unsubscribeOther, err := broadcast.Subscribe(t.Context(), "otherKey")
	require.NoError(t, err)
	defer func() { require.NoError(t, unsubscribeOther()) }()

	require.NoError(t, broadcast.Publish(t.Context(), "otherKey", []byte("otherValue")))
	require.NoError(t, broadcast.Publish(t.Context(), _key, []byte("value")))

	got, ok := receive(t, values)
	require.True(t, ok)
	assert.Equal(t, []byte("value"), got)

	got, ok = receive(t, otherValues)
	require.True(t, ok)
	assert.Equal(t, []byte("otherValue"), got)
}

func TestBroadcast_SubscribeEarlierValueNotDelivered(t *testing.T) {
	_, client := setupRedis(t)
	broadcast := New(client)
	require.NoError(t, broadcast.Publish(t.Context(), _key, []byte("earlier")))

	values, unsubscribe, err := broadcast.Subscribe(t.Context(), _key)
	require.NoError(t, err)
	defer func() { require.NoError(t, unsubscribe()) }()
	require.NoError(t, broadcast.Publish(t.Context(), _key, []byte("later")))

	got, ok := receive(t, values)
	require.True(t, ok)
	assert.Equal(t, []byte("later"), got)
}

func TestBroadcast_SubscribeOutlivingContextDeadline(t *testing.T) {
	_, client := setupRedis(t)
	broadcast := New(client)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	values, unsubscribe, err := broadcast.Subscribe(ctx, _key)
	require.NoError(t, err)
	defer func() { require.NoError(t, unsubscribe()) }()
	<-ctx.Done()

	require.NoError(t, broadcast.Publish(t.Context(), _key, []byte("value")))

	got, ok := receive(t, values)
	require.True(t, ok)
	assert.Equal(t, []byte("value"), got)
}

func TestBroadcast_SubscribeLimitReached(t *testing.T) {
	_, client := setupRedis(t)
	broadcast := New(client)

	unsubscribes := make([]func() error, 0, _maxSubscriptions)
	for range _maxSubscriptions {
		_, unsubscribe, err := broadcast.Subscribe(t.Context(), _key)
		require.NoError(t, err)
		unsubscribes = append(unsubscribes, unsubscribe)
	}

	values, unsubscribe, err := broadcast.Subscribe(t.Context(), _key)
	require.EqualError(t, err, "redisbroadcast: subscribe: too many subscriptions (limit 100)")
	assert.Nil(t, values)
	assert.Nil(t, unsubscribe)

	require.NoError(t, unsubscribes[0]())
	_, unsubscribes[0], err = broadcast.Subscribe(t.Context(), _key)
	require.NoError(t, err)

	for _, unsubscribe := range unsubscribes {
		require.NoError(t, unsubscribe())
	}
	assert.Empty(t, broadcast.slots)
}

func TestBroadcast_SubscribeConnectionError(t *testing.T) {
	server, client := setupRedis(t)
	broadcast := New(client)

	server.Close()

	values, unsubscribe, err := broadcast.Subscribe(t.Context(), _key)
	require.ErrorContains(t, err, "redisbroadcast: subscribe: ")
	assert.Nil(t, values)
	assert.Nil(t, unsubscribe)
	assert.Empty(t, broadcast.slots)
}

func TestBroadcast_SubscribeClientPanic(t *testing.T) {
	broadcast := New(subscribePanicClient{})

	assert.PanicsWithValue(t, assert.AnError, func() { _, _, _ = broadcast.Subscribe(t.Context(), _key) })
	assert.Empty(t, broadcast.slots)
}

func TestBroadcast_SubscribeCancelBeforeConfirmation(t *testing.T) {
	server, client := setupRedis(t)
	broadcast := New(client)
	held := holdSubscribe(t, server)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		<-held
		cancel() // while subscribe is held
	}()

	values, unsubscribe, err := broadcast.Subscribe(ctx, _key)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "redisbroadcast: subscribe: ")
	assert.Nil(t, values)
	assert.Nil(t, unsubscribe)
	assert.Empty(t, broadcast.slots)
}

func TestBroadcast_SubscribeDeadlineBeforeConfirmation(t *testing.T) {
	server, client := setupRedis(t)
	broadcast := New(client)
	holdSubscribe(t, server)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	values, unsubscribe, err := broadcast.Subscribe(ctx, _key)
	require.ErrorContains(t, err, "redisbroadcast: subscribe: ")
	assert.Nil(t, values)
	assert.Nil(t, unsubscribe)
	assert.Empty(t, broadcast.slots)
}

func TestBroadcast_SubscribeCancelAfterConfirmation(t *testing.T) {
	server := miniredis.RunT(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client := redis.NewClient(&redis.Options{
		Addr:                  server.Addr(),
		MaxRetries:            -1,
		ContextTimeoutEnabled: true,
		Dialer: func(dialCtx context.Context, network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(dialCtx, network, addr)
			if err != nil {
				return nil, err
			}
			return cancelingConn{Conn: conn, subscribeSent: new(atomic.Bool), cancel: cancel}, nil
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	broadcast := New(client)

	values, unsubscribe, err := broadcast.Subscribe(ctx, _key)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "redisbroadcast: subscribe: ")
	assert.Nil(t, values)
	assert.Nil(t, unsubscribe)
	assert.Empty(t, broadcast.slots)
}

func TestBroadcast_SubscribeConnectionDropped(t *testing.T) {
	testCases := map[string]func(t *testing.T, server *miniredis.Miniredis){
		"server down": func(_ *testing.T, server *miniredis.Miniredis) {
			server.Close()
		},
		"server restarted": func(t *testing.T, server *miniredis.Miniredis) {
			server.Close()
			require.NoError(t, server.Restart())
		},
	}
	for name, disconnect := range testCases {
		t.Run(name, func(t *testing.T) {
			server, client := setupRedis(t)
			broadcast := New(client)
			values, unsubscribe, err := broadcast.Subscribe(t.Context(), _key)
			require.NoError(t, err)

			disconnect(t, server)

			_, ok := receive(t, values)
			assert.False(t, ok)
			require.NoError(t, unsubscribe())
			assert.Empty(t, broadcast.slots)
		})
	}
}

func TestBroadcast_UnsubscribeValuesClosed(t *testing.T) {
	_, client := setupRedis(t)
	broadcast := New(client)
	values, unsubscribe, err := broadcast.Subscribe(t.Context(), _key)
	require.NoError(t, err)

	require.NoError(t, unsubscribe())

	_, ok := receive(t, values)
	assert.False(t, ok)
}

func TestBroadcast_UnsubscribeOtherSubscriptionKept(t *testing.T) {
	_, client := setupRedis(t)
	broadcast := New(client)
	_, unsubscribeFirst, err := broadcast.Subscribe(t.Context(), _key)
	require.NoError(t, err)
	values, unsubscribeSecond, err := broadcast.Subscribe(t.Context(), _key)
	require.NoError(t, err)
	defer func() { require.NoError(t, unsubscribeSecond()) }()

	require.NoError(t, unsubscribeFirst())
	require.NoError(t, broadcast.Publish(t.Context(), _key, []byte("value")))

	got, ok := receive(t, values)
	require.True(t, ok)
	assert.Equal(t, []byte("value"), got)
}

func TestBroadcast_UnsubscribeConcurrentValueUnread(t *testing.T) {
	server, client := setupRedis(t)
	broadcast := New(client)
	values, unsubscribe, err := broadcast.Subscribe(t.Context(), _key)
	require.NoError(t, err)
	require.NoError(t, broadcast.Publish(t.Context(), _key, []byte("value")))

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { assert.NoError(t, unsubscribe()) })
	}
	wg.Wait()

	_, ok := receive(t, values)
	assert.False(t, ok)
	assert.Empty(t, broadcast.slots)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Empty(c, server.PubSubChannels("*"))
	}, time.Second, time.Millisecond)
}

type subscribePanicClient struct {
	redis.UniversalClient
}

func (subscribePanicClient) Subscribe(context.Context, ...string) *redis.PubSub {
	panic(assert.AnError)
}

// cancelingConn cancels a context once the client has requested a
// subscription and read the reply, before Subscribe returns.
type cancelingConn struct {
	net.Conn
	subscribeSent *atomic.Bool
	cancel        context.CancelFunc
}

func (c cancelingConn) Write(b []byte) (int, error) {
	if bytes.Contains(bytes.ToLower(b), []byte("subscribe")) {
		c.subscribeSent.Store(true)
	}
	return c.Conn.Write(b)
}

func (c cancelingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 && c.subscribeSent.Load() {
		c.cancel()
	}
	return n, err
}

func setupRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}

func receive(t *testing.T, values <-chan []byte) (value []byte, ok bool) {
	t.Helper()
	select {
	case value, ok = <-values:
		return value, ok
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting on values")
		return nil, false
	}
}

// holdSubscribe holds every subscribe until the test ends.
// The returned channel closes when the first one reaches the server.
func holdSubscribe(t *testing.T, miniRedis *miniredis.Miniredis) <-chan struct{} {
	t.Helper()
	held := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	var once sync.Once
	miniRedis.Server().SetPreHook(func(_ *server.Peer, cmd string, _ ...string) bool {
		if cmd == "SUBSCRIBE" {
			once.Do(func() { close(held) })
			<-release
		}
		return false
	})
	return held
}
