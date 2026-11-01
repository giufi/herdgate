package redisstore

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	_key   = "key"
	_noTTL = 0
)

func TestStore_NewNilClientPanic(t *testing.T) {
	assert.PanicsWithValue(t, "redisstore: client must not be nil", func() { New(nil) })
}

func TestStore_NewClientIgnoringContextDeadlinePanic(t *testing.T) {
	client := redis.NewClient(&redis.Options{})
	t.Cleanup(func() { _ = client.Close() })

	assert.PanicsWithValue(t, "redisstore: client must set ContextTimeoutEnabled", func() { New(client) })
}

func TestStore_GetMiss(t *testing.T) {
	server, client := setupRedis(t)
	store := New(client)
	require.NoError(t, store.Set(t.Context(), "otherKey", []byte("value"), _noTTL))

	value, found, err := store.Get(t.Context(), _key)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, value)
	assert.False(t, server.Exists(storeKey(_key)))
}

func TestStore_GetHit(t *testing.T) {
	server, client := setupRedis(t)
	store := New(client)

	testCases := map[string][]byte{
		"text":   []byte("value"),
		"binary": {0x00, 0xff, '\r', '\n', 0x80},
		"empty":  {},
	}
	for name, want := range testCases {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, store.Set(t.Context(), _key, want, _noTTL))

			stored, err := server.Get(storeKey(_key))
			require.NoError(t, err)
			assert.Equal(t, string(want), stored)

			value, found, err := store.Get(t.Context(), _key)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, want, value)
		})
	}
}

func TestStore_GetValueSetByOtherInstance(t *testing.T) {
	_, client := setupRedis(t)
	store1 := New(client)
	store2 := New(client)

	require.NoError(t, store1.Set(t.Context(), _key, []byte("value"), time.Minute))

	value, found, err := store2.Get(t.Context(), _key)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("value"), value)
}

func TestStore_GetIndependentCopy(t *testing.T) {
	_, client := setupRedis(t)
	store := New(client)
	require.NoError(t, store.Set(t.Context(), _key, []byte("value"), _noTTL))

	first, found, err := store.Get(t.Context(), _key)
	require.NoError(t, err)
	require.True(t, found)
	first[0] = 'X'

	second, found, err := store.Get(t.Context(), _key)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []byte("value"), second)
}

func TestStore_GetIndependentResults(t *testing.T) {
	store := New(sharedReplyClient{})

	first, found, err := store.Get(t.Context(), _key)
	require.NoError(t, err)
	require.True(t, found)

	second, found, err := store.Get(t.Context(), _key)
	require.NoError(t, err)
	require.True(t, found)

	first[0] = 'X'
	assert.Equal(t, []byte("value"), second)
}

func TestStore_GetConnectionError(t *testing.T) {
	server, client := setupRedis(t)
	store := New(client)

	server.Close()

	value, found, err := store.Get(t.Context(), _key)
	require.ErrorContains(t, err, "redisstore: get: ")
	assert.False(t, found)
	assert.Nil(t, value)
}

func TestStore_GetContextCanceled(t *testing.T) {
	_, client := setupRedis(t)
	store := New(client)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	value, found, err := store.Get(ctx, _key)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, found)
	assert.Nil(t, value)
}

func TestStore_SetKeyPrefix(t *testing.T) {
	server, client := setupRedis(t)
	store := New(client)

	require.NoError(t, store.Set(t.Context(), _key, []byte("value"), _noTTL))

	stored, err := server.Get("herdgate:store:" + _key)
	require.NoError(t, err)
	assert.Equal(t, "value", stored)
}

func TestStore_SetValueOverwrite(t *testing.T) {
	server, client := setupRedis(t)
	store := New(client)

	require.NoError(t, store.Set(t.Context(), _key, []byte("old"), _noTTL))
	require.NoError(t, store.Set(t.Context(), _key, []byte("new"), _noTTL))

	stored, err := server.Get(storeKey(_key))
	require.NoError(t, err)
	assert.Equal(t, "new", stored)
}

func TestStore_SetNilValue(t *testing.T) {
	_, client := setupRedis(t)
	store := New(client)

	require.NoError(t, store.Set(t.Context(), _key, nil, _noTTL))

	value, found, err := store.Get(t.Context(), _key)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []byte{}, value)
}

func TestStore_SetIndependentCopy(t *testing.T) {
	server, client := setupRedis(t)
	store := New(client)

	original := []byte("value")
	require.NoError(t, store.Set(t.Context(), _key, original, _noTTL))
	assert.Equal(t, []byte("value"), original)
	original[0] = 'X'

	stored, err := server.Get(storeKey(_key))
	require.NoError(t, err)
	assert.Equal(t, "value", stored)
}

func TestStore_SetTTL(t *testing.T) {
	testCases := map[string]struct {
		ttl  time.Duration
		want time.Duration
	}{
		"whole seconds":   {ttl: 2 * time.Second, want: 2 * time.Second},
		"milliseconds":    {ttl: 1500 * time.Millisecond, want: 1500 * time.Millisecond},
		"sub-millisecond": {ttl: 500 * time.Microsecond, want: time.Millisecond},
		"zero":            {ttl: _noTTL, want: 0},
		"redis.KeepTTL":   {ttl: redis.KeepTTL, want: 0},
		"negative":        {ttl: -time.Second, want: 0},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			server, client := setupRedis(t)
			store := New(client)
			require.NoError(t, store.Set(t.Context(), _key, []byte("old"), time.Hour))

			require.NoError(t, store.Set(t.Context(), _key, []byte("new"), tc.ttl))

			assert.Equal(t, tc.want, server.TTL(storeKey(_key)))
		})
	}
}

func TestStore_SetTTLExpiry(t *testing.T) {
	server, client := setupRedis(t)
	store := New(client)

	const ttl = time.Second
	require.NoError(t, store.Set(t.Context(), _key, []byte("value"), ttl))

	server.FastForward(ttl * 2)
	assert.False(t, server.Exists(storeKey(_key)))

	value, found, err := store.Get(t.Context(), _key)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, value)
}

func TestStore_SetConnectionError(t *testing.T) {
	server, client := setupRedis(t)
	store := New(client)

	server.Close()

	require.ErrorContains(t, store.Set(t.Context(), _key, []byte("value"), _noTTL), "redisstore: set: ")
}

func TestStore_SetContextCanceled(t *testing.T) {
	server, client := setupRedis(t)
	store := New(client)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, store.Set(ctx, _key, []byte("value"), _noTTL), context.Canceled)
	assert.False(t, server.Exists(storeKey(_key)))
}

// sharedReplyClient answers every Get with the same string.
type sharedReplyClient struct {
	redis.UniversalClient
}

func (sharedReplyClient) Get(context.Context, string) *redis.StringCmd {
	return redis.NewStringResult("value", nil)
}

func setupRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}
