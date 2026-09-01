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

func TestCoordination_AcquireInvalidTTL(t *testing.T) {
	_, client := setupRedis(t)
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
			_, acquired, err := coord.Acquire(t.Context(), _key, tc.ttl)
			require.EqualError(t, err, tc.wantErr)
			assert.False(t, acquired)
		})
	}
}

func TestCoordination_AcquireTokenGenerationError(t *testing.T) {
	_, client := setupRedis(t)
	coord := New(client)
	coord.rand = iotest.ErrReader(errors.New("read failed"))

	_, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.EqualError(t, err, "rediscoord: token generation: read failed")
	assert.False(t, acquired)
}

func TestCoordination_AcquireConnectionError(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)
	server.Close()

	_, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.Error(t, err)
	assert.False(t, acquired)
}

func TestCoordination_AcquireContention(t *testing.T) {
	_, client := setupRedis(t)
	coord1 := New(client)
	coord2 := New(client)

	_, acquired, err := coord1.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	_, acquired, err = coord2.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	assert.False(t, acquired)
}

func TestCoordination_AcquireTTLExpiry(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	_, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	server.FastForward(time.Minute + time.Second)

	_, acquired, err = coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	assert.True(t, acquired)
}

func TestCoordination_AcquireLockKeyAndToken(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	token, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

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

func TestCoordination_ReleaseMismatchedToken(t *testing.T) {
	_, client := setupRedis(t)
	coord := New(client)

	_, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	require.NoError(t, coord.Release(t.Context(), _key, "wrong-token"))

	// Still held: a second acquire must not succeed.
	_, acquired, err = coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	assert.False(t, acquired)
}

func TestCoordination_ReleaseStaleToken(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	staleToken, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	server.FastForward(time.Minute + time.Second)

	_, acquired, err = coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	require.NoError(t, coord.Release(t.Context(), _key, staleToken))

	// The current lease must survive the stale release.
	_, acquired, err = coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	assert.False(t, acquired)
}

func TestCoordination_ReleaseConnectionError(t *testing.T) {
	server, client := setupRedis(t)
	coord := New(client)

	token, acquired, err := coord.Acquire(t.Context(), _key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	server.Close()

	assert.Error(t, coord.Release(t.Context(), _key, token))
}

func setupRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}
