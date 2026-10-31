// Package redisvalidation provides checks for Redis clients.
package redisvalidation

import (
	"errors"

	"github.com/redis/go-redis/v9"
)

// CheckContextDeadline returns an error if the client ignores the context deadline
// (ContextTimeoutEnabled off, or ReadTimeout/WriteTimeout disabling
// SetReadDeadline/SetWriteDeadline calls).
//
// The check covers only [redis.Client], [redis.ClusterClient] and
// [redis.Ring], returning nil for other client types.
func CheckContextDeadline(client redis.UniversalClient) error {
	var ctxTimeoutEnabled, socketDeadlinesDisabled bool
	switch c := client.(type) {
	case *redis.Client:
		opts := c.Options()
		ctxTimeoutEnabled = opts.ContextTimeoutEnabled
		socketDeadlinesDisabled = opts.ReadTimeout < 0 || opts.WriteTimeout < 0
	case *redis.ClusterClient:
		opts := c.Options()
		ctxTimeoutEnabled = opts.ContextTimeoutEnabled
		socketDeadlinesDisabled = opts.ReadTimeout < 0 || opts.WriteTimeout < 0
	case *redis.Ring:
		opts := c.Options()
		ctxTimeoutEnabled = opts.ContextTimeoutEnabled
		// Unlike the other client types, Ring.Options() is not normalized,
		// it keeps the configured value: -1 still sets socket deadlines, -2 skips them.
		socketDeadlinesDisabled = opts.ReadTimeout < -1 || opts.WriteTimeout < -1
	default:
		return nil
	}

	if !ctxTimeoutEnabled {
		return errors.New("client must set ContextTimeoutEnabled")
	}
	if socketDeadlinesDisabled {
		return errors.New("client must not disable socket deadlines via ReadTimeout or WriteTimeout")
	}
	return nil
}
