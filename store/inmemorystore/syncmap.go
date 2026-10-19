package inmemorystore

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// syncMap is a type-safe wrapper around [sync.Map].
type syncMap[K comparable, V any] struct {
	m sync.Map
}

// Load returns the value stored in the map for a key, or the zero value of V if no value is present.
// A stored value not of type V is treated the same as a missing key.
//
// The ok result indicates whether a value of type V was found.
func (sm *syncMap[K, V]) Load(ctx context.Context, key K) (value V, ok bool) {
	v, found := sm.m.Load(key)
	if !found {
		return value, false
	}
	value, ok = v.(V)
	if !ok {
		_logger.Error(ctx, "Stored value has an unexpected type, treating it as a miss",
			slog.Any("key", key), slog.String("got", fmt.Sprintf("%T", v)), slog.String("want", fmt.Sprintf("%T", value)))
	}
	return value, ok
}

// Store sets the value for a key.
func (sm *syncMap[K, V]) Store(key K, value V) {
	sm.m.Store(key, value)
}
