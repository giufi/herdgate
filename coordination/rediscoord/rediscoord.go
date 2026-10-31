// Package rediscoord provides a Redis-backed implementation of
// [herdgate.Coordination].
//
// [herdgate.Coordination]: https://pkg.go.dev/github.com/giufi/herdgate#Coordination
package rediscoord

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/giufi/herdgate/internal/validation"
	"github.com/giufi/herdgate/internal/validation/redisvalidation"
	"github.com/redis/go-redis/v9"
)

const _lockKeyPrefix = "herdgate:lock:"

var _renewScript = redis.NewScript(`
local currentToken = redis.call("get", KEYS[1])
if currentToken == ARGV[1] then
	return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
`)

var _releaseScript = redis.NewScript(`
local currentToken = redis.call("get", KEYS[1])
if currentToken == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`)

// Coordination is a Redis-backed implementation of [herdgate.Coordination].
type Coordination struct {
	client redis.UniversalClient
	rand   io.Reader
}

// New creates a [Coordination] backed by the given client.
//
// It panics if the client is nil or ignores the context deadline
// (ContextTimeoutEnabled off, or ReadTimeout/WriteTimeout disabling
// SetReadDeadline/SetWriteDeadline calls). The deadline check covers only
// [redis.Client], [redis.ClusterClient] and [redis.Ring].
func New(client redis.UniversalClient) *Coordination {
	if validation.IsNil(client) {
		panic("rediscoord: client must not be nil")
	}
	if err := redisvalidation.CheckContextDeadline(client); err != nil {
		panic("rediscoord: " + err.Error())
	}
	return &Coordination{client: client, rand: rand.Reader}
}

// Acquire attempts to take the lease for the given key. Redis expires the
// entry on its own once ttl elapses, even if [Coordination.Release] is
// never called, unless renewed via [Coordination.Renew].
//
// If the provided ttl is not greater than zero, it returns an error.
//
// Keys are stored under a "herdgate:lock:" prefix to avoid collisions.
func (c *Coordination) Acquire(ctx context.Context, key string, ttl time.Duration) (token string, acquired bool, err error) {
	if err := checkTTL(ttl); err != nil {
		return "", false, err
	}

	token, err = c.generateToken()
	if err != nil {
		return "", false, fmt.Errorf("rediscoord: token generation: %w", err)
	}

	acquired, err = c.client.SetNX(ctx, lockKey(key), token, ttl).Result()
	if err != nil {
		return "", false, fmt.Errorf("rediscoord: %w", err)
	}
	if !acquired {
		return "", false, nil
	}
	return token, true, nil
}

// Renew attempts to extend the lease for the given key, resetting its
// expiry to ttl. The lease is renewed only if the provided token still
// matches.
//
// The comparison and renewal happen atomically via a Lua script.
//
// If the provided ttl is not greater than zero, it returns an error.
//
// A positive ttl under one millisecond is rounded up to one millisecond.
func (c *Coordination) Renew(ctx context.Context, key, token string, ttl time.Duration) (renewed bool, err error) {
	if err := checkTTL(ttl); err != nil {
		return false, err
	}

	ttlMs := max(ttl.Milliseconds(), 1) // a non-positive PEXPIRE deletes the key instead of failing

	result, err := _renewScript.Run(ctx, c.client, []string{lockKey(key)}, token, ttlMs).Int()
	if err != nil {
		return false, fmt.Errorf("rediscoord: %w", err)
	}
	return result == 1, nil
}

// Release attempts to free the lease for the given key.
// The lease is freed only if the provided token still matches.
//
// The comparison and deletion happen atomically via a Lua script.
// On a token mismatch (the script returns 0), no error is returned.
func (c *Coordination) Release(ctx context.Context, key, token string) error {
	if err := _releaseScript.Run(ctx, c.client, []string{lockKey(key)}, token).Err(); err != nil {
		return fmt.Errorf("rediscoord: %w", err)
	}
	return nil
}

func checkTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("rediscoord: ttl must be greater than zero, got %s", ttl)
	}
	return nil
}

func (c *Coordination) generateToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := io.ReadFull(c.rand, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func lockKey(key string) string {
	return _lockKeyPrefix + key
}
