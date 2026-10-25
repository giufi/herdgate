package herdgate

import (
	"context"
	"slices"
	"sync"
	"time"
)

type acquireArgs struct {
	key string
	ttl time.Duration
}

type renewArgs struct {
	key   string
	token string
	ttl   time.Duration
}

type releaseArgs struct {
	key   string
	token string
}

type acquireResult struct {
	token    string
	acquired bool
	err      error
}

type renewResult struct {
	renewed bool
	err     error
	hang    bool // if true, Renew blocks until ctx is canceled, then returns (false, ctx.Err())
}

type releaseResult struct {
	err error
}

type fakeCoordination struct {
	acquireResults []acquireResult // one entry per Acquire call, subsequent calls repeat the last one. If empty, Acquire reports the lease as contended
	renewResults   []renewResult   // one entry per Renew call, subsequent calls repeat the last one. If empty, Renew reports the lease as lost
	releaseResults []releaseResult // one entry per Release call, subsequent calls repeat the last one. If empty, Release succeeds

	acquirePanic any
	renewPanic   any
	releasePanic any

	guarded struct {
		mu          sync.Mutex
		acquireArgs []acquireArgs
		renewArgs   []renewArgs
		releaseArgs []releaseArgs
	}
}

var _ Coordination = (*fakeCoordination)(nil)

func (f *fakeCoordination) Acquire(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	args := acquireArgs{key: key, ttl: ttl}

	f.guarded.mu.Lock()
	f.guarded.acquireArgs = append(f.guarded.acquireArgs, args)
	callIndex := len(f.guarded.acquireArgs) - 1
	f.guarded.mu.Unlock()

	if f.acquirePanic != nil {
		panic(f.acquirePanic)
	}

	if len(f.acquireResults) == 0 {
		return "", false, nil
	}
	resultIndex := min(callIndex, len(f.acquireResults)-1)
	result := f.acquireResults[resultIndex]
	return result.token, result.acquired, result.err
}

func (f *fakeCoordination) Renew(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	args := renewArgs{key: key, token: token, ttl: ttl}

	f.guarded.mu.Lock()
	f.guarded.renewArgs = append(f.guarded.renewArgs, args)
	callIndex := len(f.guarded.renewArgs) - 1
	f.guarded.mu.Unlock()

	if f.renewPanic != nil {
		panic(f.renewPanic)
	}

	if len(f.renewResults) == 0 {
		return false, nil
	}
	resultIndex := min(callIndex, len(f.renewResults)-1)
	result := f.renewResults[resultIndex]
	if result.hang {
		<-ctx.Done()
		return false, ctx.Err()
	}
	return result.renewed, result.err
}

func (f *fakeCoordination) Release(ctx context.Context, key, token string) error {
	args := releaseArgs{key: key, token: token}

	f.guarded.mu.Lock()
	f.guarded.releaseArgs = append(f.guarded.releaseArgs, args)
	callIndex := len(f.guarded.releaseArgs) - 1
	f.guarded.mu.Unlock()

	if f.releasePanic != nil {
		panic(f.releasePanic)
	}

	if len(f.releaseResults) == 0 {
		return nil
	}
	resultIndex := min(callIndex, len(f.releaseResults)-1)
	result := f.releaseResults[resultIndex]
	return result.err
}

func (f *fakeCoordination) acquireArgs() []acquireArgs {
	f.guarded.mu.Lock()
	defer f.guarded.mu.Unlock()
	return slices.Clone(f.guarded.acquireArgs)
}

func (f *fakeCoordination) renewArgs() []renewArgs {
	f.guarded.mu.Lock()
	defer f.guarded.mu.Unlock()
	return slices.Clone(f.guarded.renewArgs)
}

func (f *fakeCoordination) releaseArgs() []releaseArgs {
	f.guarded.mu.Lock()
	defer f.guarded.mu.Unlock()
	return slices.Clone(f.guarded.releaseArgs)
}
