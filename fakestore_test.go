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

	getPanic any
	setPanic any

	getCalls []string
	setCalls []setCall

	setNotify chan setCall // when non-nil, receives every Set call
}

var _ Store = (*fakeStore)(nil)

func (f *fakeStore) Get(ctx context.Context, key string) ([]byte, bool) {
	f.getCalls = append(f.getCalls, key)
	if f.getPanic != nil {
		panic(f.getPanic)
	}
	return f.getValue, f.getFound
}

func (f *fakeStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) {
	call := setCall{key: key, value: value, ttl: ttl}
	f.setCalls = append(f.setCalls, call)
	if f.setPanic != nil {
		panic(f.setPanic)
	}
	if f.setNotify != nil {
		select {
		case f.setNotify <- call:
		default:
		}
	}
}
