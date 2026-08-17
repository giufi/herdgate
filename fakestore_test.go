package herdgate

import (
	"context"
	"time"
)

type setCall struct {
	key   string
	value []byte
	ttl   time.Duration
}

type fakeStore struct {
	getValue []byte
	getFound bool

	getCalls []string
	setCalls []setCall
}

var _ Store = (*fakeStore)(nil)

func (f *fakeStore) Get(ctx context.Context, key string) ([]byte, bool) {
	f.getCalls = append(f.getCalls, key)
	return f.getValue, f.getFound
}

func (f *fakeStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) {
	f.setCalls = append(f.setCalls, setCall{key: key, value: value, ttl: ttl})
}
