package herdgate

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/giufi/herdgate/coordination/rediscoord"
	"github.com/giufi/herdgate/store/inmemorystore"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

const (
	_key             = "key"
	_token           = "token"
	_wantValue       = "foo"
	_jsonPayloadName = "foo"

	_errorMsg   = "generic error"
	_panicValue = "generic panic value"

	_errValueTestCase    = "error value"
	_nonErrValueTestCase = "non-error value"

	_successTestCase = "success"
	_errorTestCase   = "error"
	_panicTestCase   = "panic"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestHerdgate_DoCacheMiss(t *testing.T) {
	want := []byte(_wantValue)

	testCases := map[string]time.Duration{
		"positive TTL": time.Minute,
		"zero TTL":     0,
		"negative TTL": -time.Minute,
	}

	for name, ttl := range testCases {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			metrics := newMetrics()
			hg := newHerdgate(store, metrics)

			calls := 0
			fetch := func(ctx context.Context) ([]byte, error) {
				calls++
				return want, nil
			}

			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: ttl}, fetch)
			require.NoError(t, err)
			assert.Equal(t, want, got)
			assert.Equal(t, 1, calls)
			assert.Equal(t, []getArgs{{key: _key}}, store.getArgs())
			assert.Equal(t, []setArgs{{key: _key, value: want, ttl: ttl}}, store.setArgs())
			assert.InDelta(t, 1, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("miss")), 0)
			assert.InDelta(t, 0, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("hit")), 0)
			assertHistogramObservedOnce(t, metrics.fetchDuration, "success")
		})
	}
}

func TestHerdgate_DoCacheHit(t *testing.T) {
	want := []byte("cached")
	store := &fakeStore{getValue: want, getFound: true}
	metrics := newMetrics()
	hg := newHerdgate(store, metrics)

	calls := 0
	fetch := func(ctx context.Context) ([]byte, error) {
		calls++
		return []byte("should not be called"), nil
	}

	got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, 0, calls)
	assert.Equal(t, []getArgs{{key: _key}}, store.getArgs())
	assert.Empty(t, store.setArgs())
	assert.InDelta(t, 1, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("hit")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("miss")), 0)
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.fetchDuration))
}

func TestHerdgate_DoStoreGetPanic(t *testing.T) {
	want := []byte(_wantValue)
	store := &fakeStore{getPanic: assert.AnError}
	metrics := newMetrics()
	hg := newHerdgate(store, metrics)

	fetch := func(ctx context.Context) ([]byte, error) {
		return want, nil
	}

	got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.InDelta(t, 1, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("error")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("hit")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("miss")), 0)
}

func TestHerdgate_DoFetchError(t *testing.T) {
	store := &fakeStore{}
	metrics := newMetrics()
	hg := newHerdgate(store, metrics)

	wantErr := assert.AnError
	fetch := func(ctx context.Context) ([]byte, error) {
		return nil, wantErr
	}

	got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, got)
	assert.Equal(t, []getArgs{{key: _key}}, store.getArgs())
	assert.Empty(t, store.setArgs())
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("hit")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("miss")), 0)
	assertHistogramObservedOnce(t, metrics.fetchDuration, "error")
}

func TestHerdgate_DoFetchPanic(t *testing.T) {
	testCases := map[string]struct {
		panicVal    any
		wantErrIs   error
		wantErrText string
	}{
		_errValueTestCase: {
			panicVal:    assert.AnError,
			wantErrIs:   assert.AnError,
			wantErrText: "herdgate: fetch panicked: assert.AnError general error for testing",
		},
		_nonErrValueTestCase: {panicVal: _panicValue, wantErrText: "herdgate: fetch panicked: generic panic value"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			metrics := newMetrics()
			hg := newHerdgate(store, metrics)

			fetch := func(ctx context.Context) ([]byte, error) {
				panic(tc.panicVal)
			}

			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
			require.EqualError(t, err, tc.wantErrText)
			if tc.wantErrIs != nil {
				require.ErrorIs(t, err, tc.wantErrIs)
			}
			assert.Nil(t, got)
			assert.Empty(t, store.setArgs())
			assertHistogramObservedOnce(t, metrics.fetchDuration, "error")
		})
	}
}

func TestHerdgate_DoStoreSetPanic(t *testing.T) {
	want := []byte(_wantValue)
	store := &fakeStore{setPanic: assert.AnError}
	metrics := newMetrics()
	hg := newHerdgate(store, metrics)

	fetch := func(ctx context.Context) ([]byte, error) {
		return want, nil
	}

	got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.InDelta(t, 1, testutil.ToFloat64(metrics.cacheWrites.WithLabelValues("error")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.cacheWrites.WithLabelValues("success")), 0)
}

func TestHerdgate_DoContextCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := []byte(_wantValue)
		ttl := time.Minute
		store := &fakeStore{}
		hg := newTestHerdgate(store)

		unblockFetch := make(chan struct{})
		fetch := func(ctx context.Context) ([]byte, error) {
			<-unblockFetch
			return want, nil
		}

		ctx, cancel := context.WithCancel(t.Context())
		cancel() // canceled before Do runs, so Do returns immediately without waiting for fetch

		got, err := hg.Do(ctx, _key, DoOptions{TTL: ttl}, fetch)
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, got)

		close(unblockFetch)
		synctest.Wait()

		assert.Equal(t, []setArgs{{key: _key, value: want, ttl: ttl}}, store.setArgs())
	})
}

func TestHerdgate_DoSequentialCallsCacheHit(t *testing.T) {
	store := inmemorystore.New()
	hg := newTestHerdgate(store)

	want := []byte(_wantValue)
	calls := 0
	fetch := func(context.Context) ([]byte, error) {
		calls++
		return want, nil
	}

	got1, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got1)

	got2, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got2)

	assert.Equal(t, 1, calls)
	assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.cacheLookups.WithLabelValues("miss")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.cacheLookups.WithLabelValues("hit")), 0)
	assertHistogramObservedOnce(t, hg.metrics.fetchDuration, "success")
}

func TestHerdgate_DoFetchErrorThenCacheMiss(t *testing.T) {
	store := inmemorystore.New()
	hg := newTestHerdgate(store)

	fetchErr := func(context.Context) ([]byte, error) {
		return nil, assert.AnError
	}

	_, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetchErr)
	require.ErrorIs(t, err, assert.AnError)

	want := []byte(_wantValue)
	calls := 0
	fetchSuccess := func(context.Context) ([]byte, error) {
		calls++
		return want, nil
	}

	got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetchSuccess)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	got, err = hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetchSuccess)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	assert.Equal(t, 1, calls)
	assert.InDelta(t, 2, testutil.ToFloat64(hg.metrics.cacheLookups.WithLabelValues("miss")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.cacheLookups.WithLabelValues("hit")), 0)
	assertHistogramObservedOnce(t, hg.metrics.fetchDuration, "error")
	assertHistogramObservedOnce(t, hg.metrics.fetchDuration, "success")
}

func TestHerdgate_DoConcurrentCallsCoalesced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := inmemorystore.New()
		hg := newTestHerdgate(store)

		const callers = 50
		fetchCalls := countConcurrentFetches(t, hg, callers)
		require.Equal(t, 1, fetchCalls)
		assert.InDelta(t, callers-1, testutil.ToFloat64(hg.metrics.coalescedWaiters), 0)
	})
}

func TestHerdgate_DoConcurrentCallsAcrossInstancesNotCoalesced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		provider := NewProvider()
		hg1 := provider.New(inmemorystore.New())
		hg2 := provider.New(inmemorystore.New())
		instances := []*Herdgate{hg1, hg2}

		fetchCalls := countConcurrentFetchesAcross(t, instances)
		require.Equal(t, len(instances), fetchCalls)
		assert.InDelta(t, 0, testutil.ToFloat64(hg1.metrics.coalescedWaiters), 0)
	})
}

func TestHerdgate_DoConcurrentCallsIsolatedResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		hg := newTestHerdgate(inmemorystore.New())

		var calls atomic.Int64
		unblockFetch := make(chan struct{})
		callDo := func() ([]byte, error) {
			return hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, func(context.Context) ([]byte, error) {
				calls.Add(1)
				<-unblockFetch
				return []byte(_wantValue), nil
			})
		}

		var got1, got2 []byte
		var err1, err2 error
		var wg sync.WaitGroup
		wg.Go(func() { got1, err1 = callDo() })
		wg.Go(func() { got2, err2 = callDo() })

		synctest.Wait()
		close(unblockFetch)
		wg.Wait()

		require.NoError(t, err1)
		require.NoError(t, err2)
		require.Equal(t, int64(1), calls.Load())
		require.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coalescedWaiters), 0)

		want := []byte(_wantValue)
		require.Equal(t, want, got1)
		require.Equal(t, want, got2)

		got1[0] = 'X'
		assert.Equal(t, want, got2, "mutating one caller's result changed the other's")
	})
}

func TestHerdgate_DoCoordinationLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{}
		coord := &fakeCoordination{acquireResults: []acquireResult{{token: _token, acquired: true}}}
		hg := newTestHerdgate(store, WithCoordination(coord))

		want := []byte(_wantValue)
		fetchCalls := 0
		fetch := func(ctx context.Context) ([]byte, error) {
			fetchCalls++
			return want, nil
		}

		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		synctest.Wait()

		assert.Equal(t, want, got)
		assert.Equal(t, 1, fetchCalls)
		assert.Equal(t, []acquireArgs{{key: _key, ttl: _defaultLeaseTTL}}, coord.acquireArgs())
		assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquire, _resultAcquired)), 0)
		assert.Equal(t, []releaseArgs{{key: _key, token: _token}}, coord.releaseArgs())
	})
}

func TestHerdgate_DoCoordinationHeartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		coord := &fakeCoordination{
			acquireResults: []acquireResult{{token: _token, acquired: true}},
			renewResults:   []renewResult{{renewed: true}},
		}
		hg := newTestHerdgate(&fakeStore{}, WithCoordination(coord))

		want := []byte(_wantValue)
		unblockFetch := make(chan struct{})
		go func() {
			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, func(context.Context) ([]byte, error) {
				<-unblockFetch
				return want, nil
			})
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		}()

		const ticks = 2
		time.Sleep(ticks * _defaultHeartbeatInterval)
		synctest.Wait()
		close(unblockFetch)
		synctest.Wait()

		wantRenewArgs := slices.Repeat([]renewArgs{{key: _key, token: _token, ttl: _defaultLeaseTTL}}, ticks)
		assert.Equal(t, wantRenewArgs, coord.renewArgs())
		assert.Equal(t, []releaseArgs{{key: _key, token: _token}}, coord.releaseArgs())
	})
}

func TestHerdgate_DoCoordinationAcquireError(t *testing.T) {
	testCases := map[string]*fakeCoordination{
		_errorTestCase: {acquireResults: []acquireResult{{err: assert.AnError}}},
		_panicTestCase: {acquirePanic: assert.AnError},
	}

	for name, coord := range testCases {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			hg := newTestHerdgate(store, WithCoordination(coord))

			want := []byte(_wantValue)
			fetchCalls := 0
			fetch := func(ctx context.Context) ([]byte, error) {
				fetchCalls++
				return want, nil
			}

			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
			require.NoError(t, err)
			assert.Equal(t, want, got)
			assert.Equal(t, 1, fetchCalls)
			assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquire, _resultError)), 0)
		})
	}
}

func TestHerdgate_DoCoordinationReleaseError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{}
		coord := &fakeCoordination{
			acquireResults: []acquireResult{{token: _token, acquired: true}},
			releaseResults: []releaseResult{{err: assert.AnError}},
		}
		hg := newTestHerdgate(store, WithCoordination(coord))

		want := []byte(_wantValue)
		fetchCalls := 0
		fetch := func(ctx context.Context) ([]byte, error) {
			fetchCalls++
			return want, nil
		}

		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		synctest.Wait()

		assert.Equal(t, want, got)
		assert.Equal(t, 1, fetchCalls)
		assert.Equal(t, []releaseArgs{{key: _key, token: _token}}, coord.releaseArgs())
		assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationRelease, _resultError)), 0)
	})
}

func TestHerdgate_DoCoordinationFollower(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := inmemorystore.New()
		coord := &fakeCoordination{acquireResults: []acquireResult{{acquired: false}}}
		hg := newTestHerdgate(store, WithCoordination(coord))

		want := []byte(_wantValue)
		fetchCalls := 0
		fetch := func(ctx context.Context) ([]byte, error) {
			fetchCalls++
			return want, nil
		}

		go func() {
			time.Sleep(2 * _followerPollInterval)
			store.Set(t.Context(), _key, want, time.Minute)
		}()

		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, 0, fetchCalls)
		assert.Len(t, coord.acquireArgs(), 1)
		assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquire, _resultContended)), 0)
		assert.Equal(t, 0, testutil.CollectAndCount(hg.metrics.cacheWrites))
	})
}

func TestHerdgate_DoCoordinationFollowerRetryValueFound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := inmemorystore.New()
		coord := &fakeCoordination{acquireResults: []acquireResult{{acquired: false}}}
		hg := newTestHerdgate(store, WithCoordination(coord))

		want := []byte(_wantValue)
		fetchCalls := 0
		fetch := func(ctx context.Context) ([]byte, error) {
			fetchCalls++
			return want, nil
		}

		go func() {
			time.Sleep(_electionRetryInterval + 2*_followerPollInterval)
			store.Set(t.Context(), _key, want, time.Minute)
		}()

		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, 0, fetchCalls)
		assert.Len(t, coord.acquireArgs(), 2)
		assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquireRetry, _resultContended)), 0)
	})
}

func TestHerdgate_DoCoordinationFollowerRetryAcquired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{}
		coord := &fakeCoordination{
			acquireResults: []acquireResult{{acquired: false}, {token: _token, acquired: true}},
		}
		hg := newTestHerdgate(store, WithCoordination(coord))

		want := []byte(_wantValue)
		fetchCalls := 0
		fetch := func(ctx context.Context) ([]byte, error) {
			fetchCalls++
			return want, nil
		}

		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		synctest.Wait()

		assert.Equal(t, want, got)
		assert.Equal(t, 1, fetchCalls)
		assert.Equal(t, []setArgs{{key: _key, value: want, ttl: time.Minute}}, store.setArgs())
		assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquireRetry, _resultAcquired)), 0)
		assert.Equal(t, []releaseArgs{{key: _key, token: _token}}, coord.releaseArgs())
	})
}

func TestHerdgate_DoCoordinationFollowerRetryAcquireError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{}
		coord := &fakeCoordination{
			acquireResults: []acquireResult{{acquired: false}, {err: assert.AnError}},
		}
		hg := newTestHerdgate(store, WithCoordination(coord))

		want := []byte(_wantValue)
		fetchCalls := 0
		fetch := func(ctx context.Context) ([]byte, error) {
			fetchCalls++
			return want, nil
		}

		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, 1, fetchCalls)
		assert.Len(t, coord.acquireArgs(), 2)
		assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquireRetry, _resultError)), 0)
	})
}

func TestHerdgate_DoCoordinationFollowerPollPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{getPanic: assert.AnError}
		coord := &fakeCoordination{acquireResults: []acquireResult{{acquired: false}}}
		hg := newTestHerdgate(store, WithCoordination(coord))

		want := []byte(_wantValue)
		fetchCalls := 0
		fetch := func(ctx context.Context) ([]byte, error) {
			fetchCalls++
			return want, nil
		}

		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, 1, fetchCalls)
		assert.Len(t, coord.acquireArgs(), 1)
		assert.InDelta(t, 0, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquireRetry, _resultContended)), 0)
	})
}

func TestHerdgate_DoCoordinationConcurrentCallsAcrossInstancesCoalesced(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = redisClient.Close() })
	coord := rediscoord.New(redisClient)

	store := inmemorystore.New()
	provider := NewProvider()
	hg1 := provider.New(store, WithCoordination(coord))
	hg2 := provider.New(store, WithCoordination(coord))

	want := []byte(_wantValue)
	var calls atomic.Int64
	unblockLeaderFetch := make(chan struct{})

	const callersPerInstance = 3
	var wg sync.WaitGroup
	errs := make(chan error, 2*callersPerInstance)

	startConcurrentDoCalls := func(hg *Herdgate, fetch func(context.Context) ([]byte, error)) {
		for range callersPerInstance {
			wg.Go(func() {
				got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
				errs <- err
				assert.Equal(t, want, got)
			})
		}
	}

	startConcurrentDoCalls(hg1, func(context.Context) ([]byte, error) {
		calls.Add(1)
		<-unblockLeaderFetch
		return want, nil
	})

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(hg1.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquire, _resultAcquired)) == 1
	}, time.Second, time.Millisecond, "hg1 never acquired the lease")

	startConcurrentDoCalls(hg2, func(context.Context) ([]byte, error) {
		calls.Add(1)
		return want, nil
	})

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(hg1.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquire, _resultContended)) == 1
	}, time.Second, time.Millisecond, "hg2 never observed the lease as contended")

	close(unblockLeaderFetch)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	assert.Equal(t, int64(1), calls.Load())
	assert.InDelta(t, 1, testutil.ToFloat64(hg1.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquire, _resultAcquired)), 0)
}

func TestHerdgate_DoDecodeNilCodec(t *testing.T) {
	store := &fakeStore{}
	hg := newTestHerdgate(store)

	calls := 0
	fetch := func(ctx context.Context) (jsonPayload, error) {
		calls++
		return jsonPayload{Name: _jsonPayloadName}, nil
	}

	got, err := DoDecode(t.Context(), hg, _key, DoOptions{TTL: time.Minute}, fetch)
	require.EqualError(t, err, "herdgate: DoOptions.Codec must not be nil")
	assert.Equal(t, jsonPayload{}, got)
	assert.Equal(t, 0, calls)
	assert.Empty(t, store.getArgs())
}

func TestHerdgate_DoDecodeFetchError(t *testing.T) {
	store := &fakeStore{}
	hg := newTestHerdgate(store)
	codec := &fakeCodec{}

	wantErr := assert.AnError
	fetch := func(ctx context.Context) (jsonPayload, error) {
		return jsonPayload{}, wantErr
	}

	got, err := DoDecode(t.Context(), hg, _key, DoOptions{TTL: time.Minute, Codec: codec}, fetch)
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, jsonPayload{}, got)
	assert.Empty(t, store.setArgs())
	assert.Zero(t, codec.marshalCount.Load())
}

func TestHerdgate_DoDecodeMarshalError(t *testing.T) {
	codecErr := errors.New(_errorMsg)
	store := &fakeStore{}
	hg := newTestHerdgate(store)
	codec := &fakeCodec{marshalErr: codecErr}

	fetch := func(ctx context.Context) (jsonPayload, error) {
		return jsonPayload{Name: _jsonPayloadName}, nil
	}

	got, err := DoDecode(t.Context(), hg, _key, DoOptions{TTL: time.Minute, Codec: codec}, fetch)
	require.ErrorIs(t, err, codecErr)
	assert.Equal(t, jsonPayload{}, got)
	assert.Empty(t, store.setArgs())
	assert.Zero(t, codec.unmarshalCount.Load())
}

func TestHerdgate_DoDecodeMarshalPanic(t *testing.T) {
	store := &fakeStore{}
	hg := newTestHerdgate(store)
	codec := &fakeCodec{marshalPanic: _panicValue}

	fetch := func(ctx context.Context) (jsonPayload, error) {
		return jsonPayload{Name: _jsonPayloadName}, nil
	}

	got, err := DoDecode(t.Context(), hg, _key, DoOptions{TTL: time.Minute, Codec: codec}, fetch)
	require.EqualError(t, err, "herdgate: marshal herdgate.jsonPayload panicked: generic panic value")
	assert.Equal(t, jsonPayload{}, got)
	assert.Empty(t, store.setArgs())
	assert.Zero(t, codec.unmarshalCount.Load())
}

func TestHerdgate_DoDecodeUnmarshalError(t *testing.T) {
	codecErr := errors.New(_errorMsg)
	store := &fakeStore{getValue: []byte(`{"name":"foo"}`), getFound: true}
	hg := newTestHerdgate(store)
	codec := &fakeCodec{unmarshalErr: codecErr}

	fetch := func(ctx context.Context) (jsonPayload, error) {
		return jsonPayload{}, nil
	}

	got, err := DoDecode(t.Context(), hg, _key, DoOptions{TTL: time.Minute, Codec: codec}, fetch)
	require.ErrorIs(t, err, codecErr)
	assert.Equal(t, jsonPayload{}, got)
	assert.Zero(t, codec.marshalCount.Load())
}

func TestHerdgate_DoDecodeUnmarshalPanic(t *testing.T) {
	store := &fakeStore{getValue: []byte(`{"name":"foo"}`), getFound: true}
	hg := newTestHerdgate(store)
	codec := &fakeCodec{unmarshalPanic: _panicValue}

	fetch := func(ctx context.Context) (jsonPayload, error) {
		return jsonPayload{}, nil
	}

	got, err := DoDecode(t.Context(), hg, _key, DoOptions{TTL: time.Minute, Codec: codec}, fetch)
	require.EqualError(t, err, "herdgate: unmarshal *herdgate.jsonPayload panicked: generic panic value")
	assert.Equal(t, jsonPayload{}, got)
	assert.Zero(t, codec.marshalCount.Load())
}

func TestHerdgate_DoDecodeSequentialCallsCacheHit(t *testing.T) {
	want := jsonPayload{Name: _jsonPayloadName}
	codec := &fakeCodec{}
	hg := newTestHerdgate(inmemorystore.New())

	calls := 0
	fetch := func(ctx context.Context) (jsonPayload, error) {
		calls++
		return want, nil
	}
	opts := DoOptions{TTL: time.Minute, Codec: codec}

	got, err := DoDecode(t.Context(), hg, _key, opts, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	got, err = DoDecode(t.Context(), hg, _key, opts, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	assert.Equal(t, 1, calls)
	assert.Equal(t, int64(1), codec.marshalCount.Load())
	assert.Equal(t, int64(2), codec.unmarshalCount.Load())
}

func TestHerdgate_DoAfterClose(t *testing.T) {
	testCases := map[string]struct {
		store   *fakeStore
		wantVal []byte
		wantErr error
	}{
		"cache hit":  {store: &fakeStore{getValue: []byte(_wantValue), getFound: true}, wantVal: []byte(_wantValue)},
		"cache miss": {store: &fakeStore{}, wantErr: ErrClosed},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			hg := newTestHerdgate(tc.store)
			require.NoError(t, hg.Close(t.Context()))

			fetchCalls := 0
			fetch := func(context.Context) ([]byte, error) {
				fetchCalls++
				return nil, nil
			}

			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantVal, got)
			assert.Equal(t, 0, fetchCalls)
			assert.Equal(t, []getArgs{{key: _key}}, tc.store.getArgs())
		})
	}
}

func TestHerdgate_CloseInFlightFetch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		hg := newTestHerdgate(&fakeStore{})

		want := []byte(_wantValue)
		unblockFetch := make(chan struct{})
		go func() {
			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, func(context.Context) ([]byte, error) {
				<-unblockFetch
				return want, nil
			})
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		}()
		synctest.Wait()

		closeDone := make(chan error, 1)
		go func() { closeDone <- hg.Close(t.Context()) }()
		synctest.Wait()

		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before the in-flight fetch finished: %v", err)
		default:
		}

		close(unblockFetch)
		synctest.Wait()

		select {
		case err := <-closeDone:
			require.NoError(t, err)
		default:
			t.Fatal("Close did not return after the in-flight fetch finished")
		}
	})
}

func TestHerdgate_CloseMultipleInFlightFetches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		hg := newTestHerdgate(inmemorystore.New())

		key1, key2 := _key+"1", _key+"2" // the same key would coalesce into one flight
		want1, want2 := []byte(_wantValue+"1"), []byte(_wantValue+"2")
		unblockFetch1, unblockFetch2 := make(chan struct{}), make(chan struct{})

		startDo := func(key string, want []byte, unblockFetch chan struct{}) {
			go func() {
				got, err := hg.Do(t.Context(), key, DoOptions{TTL: time.Minute}, func(context.Context) ([]byte, error) {
					<-unblockFetch
					return want, nil
				})
				assert.NoError(t, err)
				assert.Equal(t, want, got)
			}()
		}

		startDo(key1, want1, unblockFetch1)
		startDo(key2, want2, unblockFetch2)
		synctest.Wait()

		closeDone := make(chan error, 1)
		go func() { closeDone <- hg.Close(t.Context()) }()
		synctest.Wait()

		close(unblockFetch1)
		synctest.Wait()

		select {
		case err := <-closeDone:
			t.Fatalf("Close returned after only the first of two fetches finished: %v", err)
		default:
		}

		close(unblockFetch2)
		synctest.Wait()

		select {
		case err := <-closeDone:
			require.NoError(t, err)
		default:
			t.Fatal("Close did not return after both fetches finished")
		}
	})
}

func TestHerdgate_CloseContextCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{}
		hg := newTestHerdgate(store)

		want := []byte(_wantValue)
		unblockFetch := make(chan struct{})
		go func() {
			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, func(context.Context) ([]byte, error) {
				<-unblockFetch
				return want, nil
			})
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		}()
		synctest.Wait()

		closeTimeout := 50 * time.Millisecond
		ctx, cancel := context.WithTimeout(t.Context(), closeTimeout)
		defer cancel()
		start := time.Now()
		require.ErrorIs(t, hg.Close(ctx), context.DeadlineExceeded)
		assert.Equal(t, closeTimeout, time.Since(start))

		close(unblockFetch)
		synctest.Wait()

		assert.Equal(t, []setArgs{{key: _key, value: want, ttl: time.Minute}}, store.setArgs())
	})
}

func TestHerdgate_CloseExpiredContextWithoutFlights(t *testing.T) {
	hg := newTestHerdgate(&fakeStore{})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for range 100 {
		require.NoError(t, hg.Close(ctx))
	}
}

func TestHerdgate_CloseIdempotent(t *testing.T) {
	hg := newTestHerdgate(&fakeStore{})

	require.NoError(t, hg.Close(t.Context()))
	require.NoError(t, hg.Close(t.Context()))
}

func TestHerdgate_CloseConcurrencySafe(t *testing.T) {
	hg := newTestHerdgate(&fakeStore{})

	const callers = 10
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Go(func() {
			errs <- hg.Close(t.Context())
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

func TestHerdgate_CloseCoordinationLeaseHeld(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		coord := &fakeCoordination{
			acquireResults: []acquireResult{{token: _token, acquired: true}},
			renewResults:   []renewResult{{renewed: true}},
		}
		hg := newTestHerdgate(&fakeStore{}, WithCoordination(coord))

		want := []byte(_wantValue)
		unblockFetch := make(chan struct{})
		go func() {
			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, func(context.Context) ([]byte, error) {
				<-unblockFetch
				return want, nil
			})
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		}()

		// A renewal confirms the heartbeat is running, so the lease is
		// still held when Close is called next
		time.Sleep(_defaultHeartbeatInterval)
		synctest.Wait()
		require.Len(t, coord.renewArgs(), 1)

		closeDone := make(chan error, 1)
		go func() { closeDone <- hg.Close(t.Context()) }()
		synctest.Wait()

		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before the in-flight fetch finished: %v", err)
		default:
		}
		assert.Empty(t, coord.releaseArgs(), "Coordination.Release was called before the in-flight fetch finished")

		close(unblockFetch)
		synctest.Wait()

		select {
		case err := <-closeDone:
			require.NoError(t, err)
		default:
			t.Fatal("Close did not return after the in-flight fetch finished")
		}
		assert.Equal(t, []releaseArgs{{key: _key, token: _token}}, coord.releaseArgs(), "Coordination.Release was not called exactly once after the in-flight fetch finished")
	})
}

func TestHerdgate_AdmitFlightAfterClose(t *testing.T) {
	hg := newTestHerdgate(&fakeStore{})
	require.NoError(t, hg.Close(t.Context()))

	done, admitted := hg.admitFlight()
	assert.False(t, admitted)
	assert.NotPanics(t, done)
}

func TestHerdgate_StartHeartbeatPeriodicRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		coord := &fakeCoordination{renewResults: []renewResult{{renewed: true}}}
		hg := newTestHerdgate(nil, WithCoordination(coord))

		stop := hg.startHeartbeat(t.Context(), _key, _token)
		defer stop()

		const ticks = 2
		time.Sleep(ticks * _defaultHeartbeatInterval)
		synctest.Wait()

		wantRenewArgs := slices.Repeat([]renewArgs{{key: _key, token: _token, ttl: _defaultLeaseTTL}}, ticks)
		assert.Equal(t, wantRenewArgs, coord.renewArgs())
	})
}

func TestHerdgate_StartHeartbeatStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		coord := &fakeCoordination{renewResults: []renewResult{{renewed: true}}}
		hg := newTestHerdgate(nil, WithCoordination(coord))

		stop := hg.startHeartbeat(t.Context(), _key, _token)
		stop()
		time.Sleep(_defaultHeartbeatInterval) // a stopped heartbeat must not renew on its next tick
		synctest.Wait()

		assert.Empty(t, coord.renewArgs())
	})
}

func TestHerdgate_StartHeartbeatStopDuringRenew(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		coord := &fakeCoordination{renewResults: []renewResult{{hang: true}}}
		hg := newTestHerdgate(nil, WithCoordination(coord))

		stop := hg.startHeartbeat(t.Context(), _key, _token)

		time.Sleep(_defaultHeartbeatInterval)
		synctest.Wait()
		start := time.Now()
		stop()

		assert.Zero(t, time.Since(start), "stop did not cancel the in-flight renewal")
		assert.Equal(t, []renewArgs{{key: _key, token: _token, ttl: _defaultLeaseTTL}}, coord.renewArgs())
		assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationRenew, _resultError)), 0)
	})
}

func TestHerdgate_StartHeartbeatLeaseLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		coord := &fakeCoordination{renewResults: []renewResult{{renewed: false}}}
		hg := newTestHerdgate(nil, WithCoordination(coord))

		stop := hg.startHeartbeat(t.Context(), _key, _token)
		defer stop()

		// A confirmed loss ends the heartbeat on its own, without calling stop
		const ticks = 3
		time.Sleep(ticks * _defaultHeartbeatInterval)
		synctest.Wait()

		assert.Equal(t, []renewArgs{{key: _key, token: _token, ttl: _defaultLeaseTTL}}, coord.renewArgs())
	})
}

func TestHerdgate_StartHeartbeatLeasePresumedLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		coord := &fakeCoordination{renewResults: []renewResult{{err: assert.AnError}}}
		hg := newTestHerdgate(nil, WithCoordination(coord))

		stop := hg.startHeartbeat(t.Context(), _key, _token)
		defer stop()

		// A presumed loss ends the heartbeat on its own, without calling stop.
		// The heartbeat gives up on the first failed renewal past the lease TTL
		time.Sleep(_defaultLeaseTTL + _defaultHeartbeatInterval) // up to the failed renewal after which the heartbeat gives up
		time.Sleep(_defaultHeartbeatInterval)                    // one more tick, which must not renew
		synctest.Wait()

		wantRenews := int(_defaultLeaseTTL/_defaultHeartbeatInterval) + 1
		wantRenewArgs := slices.Repeat([]renewArgs{{key: _key, token: _token, ttl: _defaultLeaseTTL}}, wantRenews)
		assert.Equal(t, wantRenewArgs, coord.renewArgs())
	})
}

func TestHerdgate_GuardedRenewMetrics(t *testing.T) {
	testCases := map[string]struct {
		coord      *fakeCoordination
		wantResult string
	}{
		_resultRenewed: {coord: &fakeCoordination{renewResults: []renewResult{{renewed: true}}}, wantResult: _resultRenewed},
		_resultLost:    {coord: &fakeCoordination{renewResults: []renewResult{{renewed: false}}}, wantResult: _resultLost},
		_errorTestCase: {coord: &fakeCoordination{renewResults: []renewResult{{err: assert.AnError}}}, wantResult: _resultError},
		_panicTestCase: {coord: &fakeCoordination{renewPanic: _panicValue}, wantResult: _resultError},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			hg := newTestHerdgate(nil, WithCoordination(tc.coord))

			_, _ = hg.guardedRenew(t.Context(), _key, _token)
			assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationRenew, tc.wantResult)), 0)
		})
	}
}

func TestHerdgate_GuardedReleaseMetrics(t *testing.T) {
	testCases := map[string]struct {
		coord      *fakeCoordination
		wantResult string
	}{
		_successTestCase: {coord: &fakeCoordination{}, wantResult: _resultSuccess},
		_errorTestCase:   {coord: &fakeCoordination{releaseResults: []releaseResult{{err: assert.AnError}}}, wantResult: _resultError},
		_panicTestCase:   {coord: &fakeCoordination{releasePanic: _panicValue}, wantResult: _resultError},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			hg := newTestHerdgate(nil, WithCoordination(tc.coord))

			hg.guardedRelease(t.Context(), _key, _token)
			assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationRelease, tc.wantResult)), 0)
		})
	}
}

func TestHerdgate_AwaitLeaderValueTimeout(t *testing.T) {
	testCases := map[string]struct {
		parentTimeout time.Duration
		timeout       time.Duration
		wantElapsed   time.Duration
	}{
		"no parent deadline":                   {parentTimeout: 0, timeout: 150 * time.Millisecond, wantElapsed: 150 * time.Millisecond},
		"parent deadline shorter than timeout": {parentTimeout: 150 * time.Millisecond, timeout: time.Minute, wantElapsed: 150 * time.Millisecond},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := &fakeStore{getFound: false}
				hg := newTestHerdgate(store)

				ctx := t.Context()
				if tc.parentTimeout > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tc.parentTimeout)
					defer cancel()
				}

				start := time.Now()
				value, found, err := hg.awaitLeaderValue(ctx, _key, tc.timeout)
				require.NoError(t, err)
				assert.Equal(t, tc.wantElapsed, time.Since(start))
				assert.Nil(t, value)
				assert.False(t, found)
			})
		})
	}
}

func TestHerdgate_AwaitLeaderValueStorePanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{getPanic: assert.AnError}
		hg := newTestHerdgate(store)

		value, found, err := hg.awaitLeaderValue(t.Context(), _key, 250*time.Millisecond)
		require.Error(t, err)
		assert.False(t, found)
		assert.Nil(t, value)
	})
}

func newTestHerdgate(store Store, opts ...HerdgateOption) *Herdgate {
	hg := newHerdgate(store, newMetrics())
	for _, opt := range opts {
		opt(hg)
	}
	return hg
}

// countConcurrentFetches must be called inside a synctest bubble.
func countConcurrentFetches(t *testing.T, hg *Herdgate, callers int) int {
	t.Helper()

	return countConcurrentFetchesAcross(t, slices.Repeat([]*Herdgate{hg}, callers))
}

// countConcurrentFetchesAcross must be called inside a synctest bubble.
func countConcurrentFetchesAcross(t *testing.T, instances []*Herdgate) int {
	t.Helper()

	want := []byte(_wantValue)
	var calls atomic.Int64
	unblockFetch := make(chan struct{})

	var wg sync.WaitGroup
	errs := make(chan error, len(instances))
	for _, hg := range instances {
		wg.Go(func() {
			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, func(context.Context) ([]byte, error) {
				calls.Add(1)
				<-unblockFetch
				return want, nil
			})
			errs <- err
			assert.Equal(t, want, got)
		})
	}

	synctest.Wait() // returns once every caller has started or joined its instance's flight
	close(unblockFetch)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	return int(calls.Load())
}
