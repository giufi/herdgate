package inmemorystore

import "sync"

// syncMap is a type-safe wrapper around [sync.Map].
type syncMap[K comparable, V any] struct {
	m sync.Map
}

// Load returns the value stored in the map for a key, or the zero value of V if no value is present.
// The ok result indicates whether value was found in the map.
func (sm *syncMap[K, V]) Load(key K) (value V, ok bool) {
	v, ok := sm.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return v.(V), true
}

// Store sets the value for a key.
func (sm *syncMap[K, V]) Store(key K, value V) {
	sm.m.Store(key, value)
}
