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

type fakeCoordination struct {
	acquireToken    string
	acquireAcquired bool
	acquireErr      error

	renewRenewed bool
	renewErr     error

	releaseErr error

	acquirePanic any
	renewPanic   any
	releasePanic any

	renewHold chan struct{} // when non-nil, holds Renew until closed or ctx is canceled

	renewNotify   chan renewCall   // when non-nil, receives every Renew call
	releaseNotify chan releaseCall // when non-nil, receives every Release call

	acquireCalls []acquireCall
}

var _ Coordination = (*fakeCoordination)(nil)

func (f *fakeCoordination) Acquire(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	f.acquireCalls = append(f.acquireCalls, acquireCall{key: key, ttl: ttl})
	if f.acquirePanic != nil {
		panic(f.acquirePanic)
	}
	return f.acquireToken, f.acquireAcquired, f.acquireErr
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
	return f.renewRenewed, f.renewErr
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
	return f.releaseErr
}
