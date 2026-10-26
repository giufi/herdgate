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

type getResult struct {
	value []byte
	found bool
	err   error
	hang  bool // if true, Get blocks until ctx is canceled, then returns (nil, false, ctx.Err())
}

type setResult struct {
	err error
}

type fakeStore struct {
	getResults []getResult // one entry per Get call, subsequent calls repeat the last one. If empty, Get reports a miss
	setResults []setResult // one entry per Set call, subsequent calls repeat the last one. If empty, Set succeeds

	getPanic any
	setPanic any

	guarded struct {
		mu      sync.Mutex
		getArgs []getArgs
		setArgs []setArgs
	}
}

var _ Store = (*fakeStore)(nil)

func (f *fakeStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	f.guarded.mu.Lock()
	f.guarded.getArgs = append(f.guarded.getArgs, getArgs{key: key})
	callIndex := len(f.guarded.getArgs) - 1
	f.guarded.mu.Unlock()

	if f.getPanic != nil {
		panic(f.getPanic)
	}

	if len(f.getResults) == 0 {
		return nil, false, nil
	}
	resultIndex := min(callIndex, len(f.getResults)-1)
	result := f.getResults[resultIndex]
	if result.hang {
		<-ctx.Done()
		return nil, false, ctx.Err()
	}
	return bytes.Clone(result.value), result.found, result.err
}

func (f *fakeStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	args := setArgs{key: key, value: bytes.Clone(value), ttl: ttl}

	f.guarded.mu.Lock()
	f.guarded.setArgs = append(f.guarded.setArgs, args)
	callIndex := len(f.guarded.setArgs) - 1
	f.guarded.mu.Unlock()

	if f.setPanic != nil {
		panic(f.setPanic)
	}

	if len(f.setResults) == 0 {
		return nil
	}
	resultIndex := min(callIndex, len(f.setResults)-1)
	return f.setResults[resultIndex].err
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
