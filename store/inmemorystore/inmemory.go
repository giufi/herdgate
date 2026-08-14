package inmemorystore

// Store is an in-memory implementation of [herdgate.Store], backed by a map.
//
// [herdgate.Store]: https://pkg.go.dev/github.com/giu-fi/herdgate#Store
type Store struct {
	entries map[string][]byte
}

// New creates a new [Store].
func New() *Store {
	return &Store{
		entries: make(map[string][]byte),
	}
}

// Get returns the value stored for the given key, and reports whether it was found.
func (s *Store) Get(key string) ([]byte, bool) {
	value, ok := s.entries[key]
	return value, ok
}

// Set stores the value for the given key, replacing any existing value.
func (s *Store) Set(key string, value []byte) {
	s.entries[key] = value
}
