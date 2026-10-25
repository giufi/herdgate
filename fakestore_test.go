package herdgate

import (
	"bytes"
	"context"
	"slices"
	"sync"
	"time"
)

type getArgs struct {
	key string
}

type setArgs struct {
	key   string
	value []byte
	ttl   time.Duration
}

type fakeStore struct {
	getValue []byte
	getFound bool

	getPanic any
	setPanic any

	guarded struct {
		mu      sync.Mutex
		getArgs []getArgs
		setArgs []setArgs
	}
}

var _ Store = (*fakeStore)(nil)

func (f *fakeStore) Get(ctx context.Context, key string) ([]byte, bool) {
	f.guarded.mu.Lock()
	f.guarded.getArgs = append(f.guarded.getArgs, getArgs{key: key})
	f.guarded.mu.Unlock()

	if f.getPanic != nil {
		panic(f.getPanic)
	}

	return bytes.Clone(f.getValue), f.getFound
}

func (f *fakeStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) {
	args := setArgs{key: key, value: bytes.Clone(value), ttl: ttl}

	f.guarded.mu.Lock()
	f.guarded.setArgs = append(f.guarded.setArgs, args)
	f.guarded.mu.Unlock()

	if f.setPanic != nil {
		panic(f.setPanic)
	}
}

func (f *fakeStore) getArgs() []getArgs {
	f.guarded.mu.Lock()
	defer f.guarded.mu.Unlock()
	return slices.Clone(f.guarded.getArgs)
}

func (f *fakeStore) setArgs() []setArgs {
	f.guarded.mu.Lock()
	defer f.guarded.mu.Unlock()
	return slices.Clone(f.guarded.setArgs)
}
