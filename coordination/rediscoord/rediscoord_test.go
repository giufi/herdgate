package rediscoord

import (
	"errors"
	"testing"
	"testing/iotest"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const _key = "key"

func TestCoordination_NewNilClientPanic(t *testing.T) {
	assert.PanicsWithValue(t, "rediscoord: client must not be nil", func() { New(nil) })
}

func TestCoordination_NewClientIgnoringContextDeadlinePanic(t *testing.T) {
	client := redis.NewClient(&redis.Options{})
	t.Cleanup(func() { _ = client.Close() })

	assert.PanicsWithValue(t, "rediscoord: client must set ContextTimeoutEnabled", func() { New(client) })
}

func TestCoordination_AcquireInvalidTTL(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	testCases := map[string]struct {
		ttl     time.Duration
		wantErr string
	}{
		"zero":     {ttl: 0, wantErr: "rediscoord: ttl must be greater than zero, got 0s"},
		"negative": {ttl: -time.Second, wantErr: "rediscoord: ttl must be greater than zero, got -1s"},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			token, acquired, err := coord.Acquire(t.Context(), _key, tc.ttl)
			require.EqualError(t, err, tc.wantErr)
			assert.False(t, acquired)
			assert.Empty(t, token)
			assert.False(t, server.Exists(lockKey(_key)))
		})
	}
}

func TestCoordination_AcquireTokenGenerationError(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)
	coord.rand = iotest.ErrReader(errors.New("read failed"))

	token, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.EqualError(t, err, "rediscoord: token generation: read failed")
	assert.False(t, acquired)
	assert.Empty(t, token)
	assert.False(t, server.Exists(lockKey(_key)))
}

func TestCoordination_AcquireConnectionError(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	server.Close()

	token, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.Error(t, err)
	assert.False(t, acquired)
	assert.Empty(t, token)
	assert.False(t, server.Exists(lockKey(_key)))
}

func TestCoordination_AcquireLockKeyAndToken(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	token, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	assert.Len(t, token, 32)

	stored, err := server.Get("herdgate:lock:" + _key)
	require.NoError(t, err)
	assert.Equal(t, token, stored)
}

func TestCoordination_AcquireUniqueToken(t *testing.T) {
	_, client := setupRedis(t)
	coord := New(client)

	token1, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	require.NoError(t, coord.Release(t.Context(), _key, token1))

	token2, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	assert.NotEqual(t, token1, token2)
}

func TestCoordination_AcquireTTLExpiry(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	const ttl = time.Second
	_, acquired, err := coord.Acquire(t.Context(), _key, ttl)
	require.NoError(t, err)
	require.True(t, acquired)

	server.FastForward(ttl * 2)
	require.False(t, server.Exists(lockKey(_key)))

	token, acquired, err := coord.Acquire(t.Context(), _key, ttl)
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.Equal(t, ttl, server.TTL(lockKey(_key)))

	stored, err := server.Get(lockKey(_key))
	require.NoError(t, err)
	assert.Equal(t, token, stored)
}

func TestCoordination_AcquireContention(t *testing.T) {
	server, client := setupRedis(t)
	coord1 := New(client)
	coord2 := New(client)

	const ttl = time.Minute
	token1, acquired, err := coord1.Acquire(t.Context(), _key, ttl)
	require.NoError(t, err)
	require.True(t, acquired)

	token2, acquired, err := coord2.Acquire(t.Context(), _key, ttl*10)
	require.NoError(t, err)
	assert.False(t, acquired)
	assert.Empty(t, token2)

	assert.Equal(t, ttl, server.TTL(lockKey(_key)))

	stored, err := server.Get(lockKey(_key))
	require.NoError(t, err)
	assert.Equal(t, token1, stored)
}

func TestCoordination_AcquireIndependentKeys(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	_, acquired, err := coord.Acquire(t.Context(), "key-a", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	_, acquired, err = coord.Acquire(t.Context(), "key-b", time.Minute)
	require.NoError(t, err)
	assert.True(t, acquired)

	assert.True(t, server.Exists(lockKey("key-a")))
	assert.True(t, server.Exists(lockKey("key-b")))
}

func TestCoordination_RenewInvalidTTL(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	const ttl = time.Second
	token, acquired, err := coord.Acquire(t.Context(), _key, ttl)
	require.NoError(t, err)
	require.True(t, acquired)

	testCases := map[string]struct {
		ttl     time.Duration
		wantErr string
	}{
		"zero":     {ttl: 0, wantErr: "rediscoord: ttl must be greater than zero, got 0s"},
		"negative": {ttl: -time.Second, wantErr: "rediscoord: ttl must be greater than zero, got -1s"},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			renewed, err := coord.Renew(t.Context(), _key, token, tc.ttl)
			require.EqualError(t, err, tc.wantErr)
			assert.False(t, renewed)
			assert.Equal(t, ttl, server.TTL(lockKey(_key)))
		})
	}
}

func TestCoordination_RenewMismatchedToken(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	const ttl = time.Second
	token, acquired, err := coord.Acquire(t.Context(), _key, ttl)
	require.NoError(t, err)
	require.True(t, acquired)

	renewed, err := coord.Renew(t.Context(), _key, "wrong-token", time.Minute)
	require.NoError(t, err)
	assert.False(t, renewed)
	assert.Equal(t, ttl, server.TTL(lockKey(_key)))

	stored, err := server.Get(lockKey(_key))
	require.NoError(t, err)
	assert.Equal(t, token, stored)
}

func TestCoordination_RenewStaleToken(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	const ttl = time.Second
	staleToken, acquired, err := coord.Acquire(t.Context(), _key, ttl)
	require.NoError(t, err)
	require.True(t, acquired)

	server.FastForward(ttl * 2)

	currentToken, acquired, err := coord.Acquire(t.Context(), _key, ttl)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NotEqual(t, staleToken, currentToken)

	renewed, err := coord.Renew(t.Context(), _key, staleToken, time.Minute)
	require.NoError(t, err)
	assert.False(t, renewed)
	assert.Equal(t, ttl, server.TTL(lockKey(_key)))

	stored, err := server.Get(lockKey(_key))
	require.NoError(t, err)
	assert.Equal(t, currentToken, stored)
}

func TestCoordination_RenewMissingKey(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	renewed, err := coord.Renew(t.Context(), _key, "some-token", time.Minute)
	require.NoError(t, err)
	assert.False(t, renewed)
	assert.False(t, server.Exists(lockKey(_key)))
}

func TestCoordination_RenewConnectionError(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	const ttl = time.Second
	token, acquired, err := coord.Acquire(t.Context(), _key, ttl)
	require.NoError(t, err)
	require.True(t, acquired)

	server.Close()

	renewed, err := coord.Renew(t.Context(), _key, token, time.Minute)
	require.Error(t, err)
	assert.False(t, renewed)
	assert.Equal(t, ttl, server.TTL(lockKey(_key)))

	stored, err := server.Get(lockKey(_key))
	require.NoError(t, err)
	assert.Equal(t, token, stored)
}

func TestCoordination_RenewMatchingToken(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	const ttl = time.Second
	token, acquired, err := coord.Acquire(t.Context(), _key, ttl)
	require.NoError(t, err)
	require.True(t, acquired)
	require.Equal(t, ttl, server.TTL(lockKey(_key)))

	renewed, err := coord.Renew(t.Context(), _key, token, time.Minute)
	require.NoError(t, err)
	assert.True(t, renewed)
	assert.Equal(t, time.Minute, server.TTL(lockKey(_key)))

	stored, err := server.Get(lockKey(_key))
	require.NoError(t, err)
	assert.Equal(t, token, stored)
}

func TestCoordination_RenewSubMillisecondTTL(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	const ttl = time.Second
	token, acquired, err := coord.Acquire(t.Context(), _key, ttl)
	require.NoError(t, err)
	require.True(t, acquired)

	renewed, err := coord.Renew(t.Context(), _key, token, 500*time.Microsecond)
	require.NoError(t, err)
	assert.True(t, renewed)
	assert.True(t, server.Exists(lockKey(_key)))
	assert.Equal(t, time.Millisecond, server.TTL(lockKey(_key)))
}

func TestCoordination_ReleaseMismatchedToken(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	token, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	require.NoError(t, coord.Release(t.Context(), _key, "wrong-token"))

	stored, err := server.Get(lockKey(_key))
	require.NoError(t, err)
	assert.Equal(t, token, stored)
}

func TestCoordination_ReleaseStaleToken(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	const ttl = time.Second
	staleToken, acquired, err := coord.Acquire(t.Context(), _key, ttl)
	require.NoError(t, err)
	require.True(t, acquired)

	server.FastForward(ttl * 2)

	currentToken, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NotEqual(t, staleToken, currentToken)

	require.NoError(t, coord.Release(t.Context(), _key, staleToken))

	stored, err := server.Get(lockKey(_key))
	require.NoError(t, err)
	assert.Equal(t, currentToken, stored)
}

func TestCoordination_ReleaseMissingKey(t *testing.T) {
	_, client := setupRedis(t)
	coord := New(client)

	require.NoError(t, coord.Release(t.Context(), _key, "some-token"))
}

func TestCoordination_ReleaseConnectionError(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	token, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	server.Close()

	require.Error(t, coord.Release(t.Context(), _key, token))

	stored, err := server.Get(lockKey(_key))
	require.NoError(t, err)
	assert.Equal(t, token, stored)
}

func TestCoordination_ReleaseMatchingToken(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	token, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	require.NoError(t, coord.Release(t.Context(), _key, token))

	assert.False(t, server.Exists(lockKey(_key)))
}

func setupRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}
