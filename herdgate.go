package herdgate

// A Herdgate is a cache backed by a key-value store.
type Herdgate struct {
	store map[string][]byte
}

// New creates a new Herdgate.
func New() *Herdgate {
	return &Herdgate{
		store: make(map[string][]byte),
	}
}

// Do returns the cached value for the key, or calls fetch and caches its result.
func (hg *Herdgate) Do(key string, fetch func() ([]byte, error)) ([]byte, error) {
	if cached, ok := hg.store[key]; ok {
		return cached, nil
	}
	fresh, err := fetch()
	if err != nil {
		return nil, err
	}
	hg.store[key] = fresh
	return fresh, nil
}
