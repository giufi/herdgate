package herdgate

// Store is the interface that wraps the methods for reading and writing cached values by key.
type Store interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte)
}
