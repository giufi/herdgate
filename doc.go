// Package herdgate addresses thundering herds, including cache stampedes, in
// distributed systems by coalescing fetches for the same key.
//
// A [Herdgate] returns the cached value for a key, or on a miss calls the
// fetch function supplied by the caller and caches its result.
//
// Concurrent misses for the same key are locally coalesced into a single
// fetch, with optional support for distributed coalescing via a
// [Coordination].
//
// A TTL controls how long a cached value remains valid before being
// refetched.
//
// Create [Herdgate] instances through a [Provider], which supplies shared
// metrics across all instances it creates.
package herdgate
