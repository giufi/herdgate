// Package herdgate provides a read-through cache backed by a [Store].
//
// A [Herdgate] returns the cached value for a key, or on a miss calls the
// fetch function the caller supplied, then caches its result.
//
// Concurrent misses for the same key are collapsed into a single fetch.
//
// A TTL controls how long a cached value stays valid before being refetched.
//
// Create [Herdgate] instances through a [Provider], which supplies the same metrics to each.
package herdgate
