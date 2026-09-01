package herdgate

import (
	"context"
	"time"
)

type acquireCall struct {
	key string
	ttl time.Duration
}

type releaseCall struct {
	key   string
	token string
}

type fakeCoordination struct {
	acquireToken    string
	acquireAcquired bool
	acquireErr      error

	releaseErr error

	acquirePanic any
	releasePanic any

	acquireCalls []acquireCall

	releaseNotify chan releaseCall // when non-nil, receives every Release call
}

var _ Coordination = (*fakeCoordination)(nil)

func (f *fakeCoordination) Acquire(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	f.acquireCalls = append(f.acquireCalls, acquireCall{key: key, ttl: ttl})
	if f.acquirePanic != nil {
		panic(f.acquirePanic)
	}
	return f.acquireToken, f.acquireAcquired, f.acquireErr
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
