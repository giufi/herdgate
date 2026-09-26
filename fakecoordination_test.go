package herdgate

import (
	"context"
	"time"
)

type acquireCall struct {
	key string
	ttl time.Duration
}

type renewCall struct {
	key   string
	token string
	ttl   time.Duration
}

type releaseCall struct {
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
}

type releaseResult struct {
	err error
}

type fakeCoordination struct {
	acquireCalls []acquireCall

	acquireResults []acquireResult // one entry per Acquire call, subsequent calls repeat the last one. If empty, Acquire reports the lease as contended
	renewResults   []renewResult   // one entry per Renew call, subsequent calls repeat the last one. If empty, Renew reports the lease as lost
	releaseResults []releaseResult // one entry per Release call, subsequent calls repeat the last one. If empty, Release succeeds

	acquirePanic any
	renewPanic   any
	releasePanic any

	renewHold chan struct{} // when non-nil, holds Renew until closed or ctx is canceled

	renewNotify   chan renewCall   // when non-nil, receives every Renew call
	releaseNotify chan releaseCall // when non-nil, receives every Release call
}

var _ Coordination = (*fakeCoordination)(nil)

func (f *fakeCoordination) Acquire(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	f.acquireCalls = append(f.acquireCalls, acquireCall{key: key, ttl: ttl})
	if f.acquirePanic != nil {
		panic(f.acquirePanic)
	}
	if len(f.acquireResults) == 0 {
		return "", false, nil
	}
	result := f.acquireResults[0]
	if len(f.acquireResults) > 1 {
		f.acquireResults = f.acquireResults[1:]
	}
	return result.token, result.acquired, result.err
}

func (f *fakeCoordination) Renew(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	if f.renewPanic != nil {
		panic(f.renewPanic)
	}
	if f.renewNotify != nil {
		select {
		case f.renewNotify <- renewCall{key: key, token: token, ttl: ttl}:
		default:
		}
	}
	if f.renewHold != nil {
		select {
		case <-f.renewHold:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	if len(f.renewResults) == 0 {
		return false, nil
	}
	result := f.renewResults[0]
	if len(f.renewResults) > 1 {
		f.renewResults = f.renewResults[1:]
	}
	return result.renewed, result.err
}

func (f *fakeCoordination) Release(ctx context.Context, key, token string) error {
	if f.releasePanic != nil {
		panic(f.releasePanic)
	}
	if f.releaseNotify != nil {
		select {
		case f.releaseNotify <- releaseCall{key: key, token: token}:
		default:
		}
	}
	if len(f.releaseResults) == 0 {
		return nil
	}
	result := f.releaseResults[0]
	if len(f.releaseResults) > 1 {
		f.releaseResults = f.releaseResults[1:]
	}
	return result.err
}
