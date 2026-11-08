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

	_pollMissTestCase      = "poll miss"
	_pollHangTestCase      = "poll hang"
	_pollHangErrorTestCase = "poll hang then error"
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
			hg := newTestHerdgate(store)

			var calls atomic.Int64
			fetch := func(ctx context.Context) ([]byte, error) {
				calls.Add(1)
				return want, nil
			}

			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: ttl}, fetch)
			require.NoError(t, err)
			assert.Equal(t, want, got)
			assert.Equal(t, int64(1), calls.Load())
			assert.Equal(t, []getArgs{{key: _key}}, store.getArgs())
			assert.Equal(t, []setArgs{{key: _key, value: want, ttl: ttl}}, store.setArgs())
			assertMetricCounts(t, hg.metrics, func(want *metrics) {
				want.recordCacheMiss(_anyStart)
				want.observeFetch(_anyStart, nil)
				want.recordCacheWrite(_anyStart, nil)
			})
		})
	}
}

func TestHerdgate_DoCacheHit(t *testing.T) {
	want := []byte("cached")
	store := &fakeStore{getResults: []getResult{{value: want, found: true}}}
	hg := newTestHerdgate(store)

	var calls atomic.Int64
	fetch := func(ctx context.Context) ([]byte, error) {
		calls.Add(1)
		return []byte("should not be called"), nil
	}

	got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, int64(0), calls.Load())
	assert.Equal(t, []getArgs{{key: _key}}, store.getArgs())
	assert.Empty(t, store.setArgs())
	assertMetricCounts(t, hg.metrics, func(want *metrics) {
		want.recordCacheHit(_anyStart)
	})
}

func TestHerdgate_DoStoreGetError(t *testing.T) {
	testCases := map[string]*fakeStore{
		_errorTestCase:                      {getResults: []getResult{{err: assert.AnError}}},
		"context.Canceled":                  {getResults: []getResult{{err: context.Canceled}}}, // ctx still live
		"error and unexpected cached value": {getResults: []getResult{{value: []byte("cached"), found: true, err: assert.AnError}}},
		_panicTestCase:                      {getPanic: assert.AnError},
	}

	for name, store := range testCases {
		t.Run(name, func(t *testing.T) {
			want := []byte(_wantValue)
			hg := newTestHerdgate(store)

			fetch := func(ctx context.Context) ([]byte, error) {
				return want, nil
			}

			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
			require.NoError(t, err)
			assert.Equal(t, want, got)
			assertMetricCounts(t, hg.metrics, func(want *metrics) {
				want.recordCacheLookupError(_anyStart)
				want.observeFetch(_anyStart, nil)
				want.recordCacheWrite(_anyStart, nil)
			})
		})
	}
}

func TestHerdgate_DoStoreGetTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{getResults: []getResult{{hang: true}}}
		hg := newTestHerdgate(store)

		want := []byte(_wantValue)
		fetch := func(ctx context.Context) ([]byte, error) {
			return want, nil
		}

		start := time.Now()
		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, _defaultStoreCallTimeout, time.Since(start))
		assertMetricCounts(t, hg.metrics, func(want *metrics) {
			want.recordCacheLookupError(_anyStart)
			want.observeFetch(_anyStart, nil)
			want.recordCacheWrite(_anyStart, nil)
		})
		assertHistogramDuration(t, hg.metrics.storeDuration, _defaultStoreCallTimeout, _testStoreOperationGet, _testResultLookupError)
	})
}

func TestHerdgate_GuardedStoreGetContextCanceled(t *testing.T) {
	testCases := map[string]struct {
		store       *fakeStore
		withTimeout bool
		replay      func(want *metrics)
	}{
		"ctx error":                {store: &fakeStore{getResults: []getResult{{hang: true}}}, replay: func(want *metrics) { want.recordCacheLookupContextDone(_anyStart) }},
		"any other error":          {store: &fakeStore{getResults: []getResult{{err: assert.AnError}}}, replay: func(want *metrics) { want.recordCacheLookupError(_anyStart) }},
		"error after ctx deadline": {store: &fakeStore{getResults: []getResult{{err: assert.AnError}}}, withTimeout: true, replay: func(want *metrics) { want.recordCacheLookupContextDone(_anyStart) }},
		_panicTestCase:             {store: &fakeStore{getPanic: assert.AnError}, replay: func(want *metrics) { want.recordCacheLookupError(_anyStart) }},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			hg := newTestHerdgate(tc.store)
			ctx, cancel := context.WithCancel(t.Context())
			if tc.withTimeout {
				ctx, cancel = context.WithTimeout(t.Context(), 0)
			}
			cancel()

			value, found := hg.guardedStoreGet(ctx, _key)
			assert.Nil(t, value)
			assert.False(t, found)
			assertMetricCounts(t, hg.metrics, tc.replay)
		})
	}
}

func TestHerdgate_DoFetchError(t *testing.T) {
	store := &fakeStore{}
	hg := newTestHerdgate(store)

	wantErr := assert.AnError
	fetch := func(ctx context.Context) ([]byte, error) {
		return nil, wantErr
	}

	got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, got)
	assert.Equal(t, []getArgs{{key: _key}}, store.getArgs())
	assert.Empty(t, store.setArgs())
	assertMetricCounts(t, hg.metrics, func(want *metrics) {
		want.recordCacheMiss(_anyStart)
		want.observeFetch(_anyStart, assert.AnError)
	})
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
			hg := newTestHerdgate(store)

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
			assertMetricCounts(t, hg.metrics, func(want *metrics) {
				want.recordCacheMiss(_anyStart)
				want.observeFetch(_anyStart, assert.AnError)
			})
		})
	}
}

func TestHerdgate_DoFetchNilValue(t *testing.T) {
	store := &fakeStore{}
	hg := newTestHerdgate(store)

	fetch := func(ctx context.Context) ([]byte, error) {
		return nil, nil
	}

	got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
	require.NoError(t, err)
	assert.Equal(t, []byte{}, got)
	assert.Equal(t, []setArgs{{key: _key, value: []byte{}, ttl: time.Minute}}, store.setArgs())
}

func TestHerdgate_DoStoreSetError(t *testing.T) {
	testCases := map[string]*fakeStore{
		_errorTestCase: {setResults: []setResult{{err: assert.AnError}}},
		_panicTestCase: {setPanic: assert.AnError},
	}

	for name, store := range testCases {
		t.Run(name, func(t *testing.T) {
			want := []byte(_wantValue)
			hg := newTestHerdgate(store)

			fetch := func(ctx context.Context) ([]byte, error) {
				return want, nil
			}

			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
			require.NoError(t, err)
			assert.Equal(t, want, got)
			assertMetricCounts(t, hg.metrics, func(want *metrics) {
				want.recordCacheMiss(_anyStart)
				want.observeFetch(_anyStart, nil)
				want.recordCacheWrite(_anyStart, assert.AnError)
			})
		})
	}
}

func TestHerdgate_DoStoreSetTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{setResults: []setResult{{hang: true}}}
		hg := newTestHerdgate(store)

		want := []byte(_wantValue)
		fetch := func(ctx context.Context) ([]byte, error) {
			return want, nil
		}

		start := time.Now()
		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, _defaultStoreCallTimeout, time.Since(start))
		assertMetricCounts(t, hg.metrics, func(want *metrics) {
			want.recordCacheMiss(_anyStart)
			want.observeFetch(_anyStart, nil)
			want.recordCacheWrite(_anyStart, assert.AnError)
		})
		assertHistogramDuration(t, hg.metrics.storeDuration, _defaultStoreCallTimeout, _testStoreOperationSet, _testResultError)
	})
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
	var calls atomic.Int64
	fetch := func(context.Context) ([]byte, error) {
		calls.Add(1)
		return want, nil
	}

	got1, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got1)

	got2, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got2)

	assert.Equal(t, int64(1), calls.Load())
	assertMetricCounts(t, hg.metrics, func(want *metrics) {
		want.recordCacheMiss(_anyStart)
		want.observeFetch(_anyStart, nil)
		want.recordCacheWrite(_anyStart, nil)
		want.recordCacheHit(_anyStart)
	})
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
	var calls atomic.Int64
	fetchSuccess := func(context.Context) ([]byte, error) {
		calls.Add(1)
		return want, nil
	}

	got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetchSuccess)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	got, err = hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetchSuccess)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	assert.Equal(t, int64(1), calls.Load())
	assertMetricCounts(t, hg.metrics, func(want *metrics) {
		want.recordCacheMiss(_anyStart)
		want.observeFetch(_anyStart, assert.AnError)
		want.recordCacheMiss(_anyStart)
		want.observeFetch(_anyStart, nil)
		want.recordCacheWrite(_anyStart, nil)
		want.recordCacheHit(_anyStart)
	})
}

func TestHerdgate_DoConcurrentCallsCoalesced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := inmemorystore.New()
		hg := newTestHerdgate(store)

		const callers = 50
		fetchCalls := countConcurrentFetches(t, hg, callers)
		require.Equal(t, 1, fetchCalls)
		assertMetricCounts(t, hg.metrics, func(want *metrics) {
			for range callers {
				want.recordCacheMiss(_anyStart)
			}
			for range callers - 1 {
				want.incrementCoalescedWaiter()
			}
			want.observeFetch(_anyStart, nil)
			want.recordCacheWrite(_anyStart, nil)
		})
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
		assertMetricCounts(t, hg1.metrics, func(want *metrics) {
			want.recordCacheMiss(_anyStart)
			want.recordCacheMiss(_anyStart)
			want.observeFetch(_anyStart, nil)
			want.observeFetch(_anyStart, nil)
			want.recordCacheWrite(_anyStart, nil)
			want.recordCacheWrite(_anyStart, nil)
		})
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

		want := []byte(_wantValue)
		require.Equal(t, want, got1)
		require.Equal(t, want, got2)

		got1[0] = 'X'
		assert.Equal(t, want, got2, "mutating one caller's result changed the other's")

		assertMetricCounts(t, hg.metrics, func(want *metrics) {
			want.recordCacheMiss(_anyStart)
			want.recordCacheMiss(_anyStart)
			want.incrementCoalescedWaiter()
			want.observeFetch(_anyStart, nil)
			want.recordCacheWrite(_anyStart, nil)
		})
	})
}

func TestHerdgate_DoCoordinationLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{}
		coord := &fakeCoordination{acquireResults: []acquireResult{{token: _token, acquired: true}}}
		hg := newTestHerdgate(store, WithCoordination(coord))

		want := []byte(_wantValue)
		var fetchCalls atomic.Int64
		fetch := func(ctx context.Context) ([]byte, error) {
			fetchCalls.Add(1)
			return want, nil
		}

		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		synctest.Wait()

		assert.Equal(t, want, got)
		assert.Equal(t, int64(1), fetchCalls.Load())
		assert.Equal(t, []acquireArgs{{key: _key, ttl: _defaultLeaseTTL}}, coord.acquireArgs())
		assert.Equal(t, []releaseArgs{{key: _key, token: _token}}, coord.releaseArgs())
		assertMetricCounts(t, hg.metrics, func(want *metrics) {
			want.recordCacheMiss(_anyStart)
			want.recordCoordinationAcquire(_anyStart, true, nil)
			want.observeFetch(_anyStart, nil)
			want.recordCacheWrite(_anyStart, nil)
			want.recordCoordinationRelease(_anyStart, nil)
		})
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
			var fetchCalls atomic.Int64
			fetch := func(ctx context.Context) ([]byte, error) {
				fetchCalls.Add(1)
				return want, nil
			}

			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
			require.NoError(t, err)
			assert.Equal(t, want, got)
			assert.Equal(t, int64(1), fetchCalls.Load())
			assertMetricCounts(t, hg.metrics, func(want *metrics) {
				want.recordCacheMiss(_anyStart)
				want.recordCoordinationAcquire(_anyStart, false, assert.AnError)
				want.observeFetch(_anyStart, nil)
				want.recordCacheWrite(_anyStart, nil)
			})
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
		var fetchCalls atomic.Int64
		fetch := func(ctx context.Context) ([]byte, error) {
			fetchCalls.Add(1)
			return want, nil
		}

		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		synctest.Wait()

		assert.Equal(t, want, got)
		assert.Equal(t, int64(1), fetchCalls.Load())
		assert.Equal(t, []releaseArgs{{key: _key, token: _token}}, coord.releaseArgs())
		assertMetricCounts(t, hg.metrics, func(want *metrics) {
			want.recordCacheMiss(_anyStart)
			want.recordCoordinationAcquire(_anyStart, true, nil)
			want.observeFetch(_anyStart, nil)
			want.recordCacheWrite(_anyStart, nil)
			want.recordCoordinationRelease(_anyStart, assert.AnError)
		})
	})
}

func TestHerdgate_DoCoordinationFollower(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := inmemorystore.New()
		coord := &fakeCoordination{acquireResults: []acquireResult{{acquired: false}}}
		hg := newTestHerdgate(store, WithCoordination(coord))

		want := []byte(_wantValue)
		var fetchCalls atomic.Int64
		fetch := func(ctx context.Context) ([]byte, error) {
			fetchCalls.Add(1)
			return want, nil
		}

		go func() {
			time.Sleep(2 * _followerPollInterval)
			assert.NoError(t, store.Set(t.Context(), _key, want, time.Minute))
		}()

		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, int64(0), fetchCalls.Load())
		assert.Len(t, coord.acquireArgs(), 1)
		assertMetricCounts(t, hg.metrics, func(want *metrics) {
			want.recordCacheMiss(_anyStart)
			want.recordCoordinationAcquire(_anyStart, false, nil)
		})
	})
}

func TestHerdgate_DoCoordinationFollowerRetryValueFound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := inmemorystore.New()
		coord := &fakeCoordination{acquireResults: []acquireResult{{acquired: false}}}
		hg := newTestHerdgate(store, WithCoordination(coord))

		want := []byte(_wantValue)
		var fetchCalls atomic.Int64
		fetch := func(ctx context.Context) ([]byte, error) {
			fetchCalls.Add(1)
			return want, nil
		}

		go func() {
			time.Sleep(_electionRetryInterval + 2*_followerPollInterval)
			assert.NoError(t, store.Set(t.Context(), _key, want, time.Minute))
		}()

		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, int64(0), fetchCalls.Load())
		assert.Len(t, coord.acquireArgs(), 2)
		assertMetricCounts(t, hg.metrics, func(want *metrics) {
			want.recordCacheMiss(_anyStart)
			want.recordCoordinationAcquire(_anyStart, false, nil)
			want.recordCoordinationAcquireRetry(_anyStart, false, nil)
		})
	})
}

func TestHerdgate_DoCoordinationFollowerRetryAcquired(t *testing.T) {
	testCases := map[string]*fakeStore{
		_pollMissTestCase:      {},
		_pollHangTestCase:      {getResults: []getResult{{}, {hang: true}}},
		_pollHangErrorTestCase: {getResults: []getResult{{}, {hang: true, err: assert.AnError}}},
	}

	for name, store := range testCases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				coord := &fakeCoordination{
					acquireResults: []acquireResult{{acquired: false}, {token: _token, acquired: true}},
				}
				hg := newTestHerdgate(store, WithCoordination(coord))

				want := []byte(_wantValue)
				var fetchCalls atomic.Int64
				fetch := func(ctx context.Context) ([]byte, error) {
					fetchCalls.Add(1)
					return want, nil
				}

				start := time.Now()
				got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
				require.NoError(t, err)
				elapsed := time.Since(start)
				synctest.Wait()

				assert.Equal(t, want, got)
				assert.Equal(t, _electionRetryInterval, elapsed)
				assert.Equal(t, int64(1), fetchCalls.Load())
				assert.Len(t, coord.acquireArgs(), 2)
				assert.Equal(t, []setArgs{{key: _key, value: want, ttl: time.Minute}}, store.setArgs())
				assert.Equal(t, []releaseArgs{{key: _key, token: _token}}, coord.releaseArgs())
				assertMetricCounts(t, hg.metrics, func(want *metrics) {
					want.recordCacheMiss(_anyStart)
					want.recordCoordinationAcquire(_anyStart, false, nil)
					want.recordCoordinationAcquireRetry(_anyStart, true, nil)
					want.observeFetch(_anyStart, nil)
					want.recordCacheWrite(_anyStart, nil)
					want.recordCoordinationRelease(_anyStart, nil)
				})
			})
		})
	}
}

func TestHerdgate_DoCoordinationFollowerRetryAcquireError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{}
		coord := &fakeCoordination{
			acquireResults: []acquireResult{{acquired: false}, {err: assert.AnError}},
		}
		hg := newTestHerdgate(store, WithCoordination(coord))

		want := []byte(_wantValue)
		var fetchCalls atomic.Int64
		fetch := func(ctx context.Context) ([]byte, error) {
			fetchCalls.Add(1)
			return want, nil
		}

		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, int64(1), fetchCalls.Load())
		assert.Len(t, coord.acquireArgs(), 2)
		assertMetricCounts(t, hg.metrics, func(want *metrics) {
			want.recordCacheMiss(_anyStart)
			want.recordCoordinationAcquire(_anyStart, false, nil)
			want.recordCoordinationAcquireRetry(_anyStart, false, assert.AnError)
			want.observeFetch(_anyStart, nil)
			want.recordCacheWrite(_anyStart, nil)
		})
	})
}

func TestHerdgate_DoCoordinationFollowerPollError(t *testing.T) {
	testCases := map[string]*fakeStore{
		_errorTestCase: {getResults: []getResult{{err: assert.AnError}}},
		_panicTestCase: {getPanic: assert.AnError},
	}

	for name, store := range testCases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				coord := &fakeCoordination{acquireResults: []acquireResult{{acquired: false}}}
				hg := newTestHerdgate(store, WithCoordination(coord))

				want := []byte(_wantValue)
				var fetchCalls atomic.Int64
				fetch := func(ctx context.Context) ([]byte, error) {
					fetchCalls.Add(1)
					return want, nil
				}

				got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
				require.NoError(t, err)
				assert.Equal(t, want, got)
				assert.Equal(t, int64(1), fetchCalls.Load())
				assert.Len(t, coord.acquireArgs(), 1)
				assert.Len(t, store.getArgs(), 2)
				assertMetricCounts(t, hg.metrics, func(want *metrics) {
					want.recordCacheLookupError(_anyStart)
					want.recordCoordinationAcquire(_anyStart, false, nil)
					want.observeFetch(_anyStart, nil)
					want.recordCacheWrite(_anyStart, nil)
				})
			})
		})
	}
}

func TestHerdgate_DoCoordinationFollowerTimeout(t *testing.T) {
	testCases := map[string]*fakeStore{
		_pollMissTestCase:      {},
		_pollHangTestCase:      {getResults: []getResult{{}, {hang: true}}},
		_pollHangErrorTestCase: {getResults: []getResult{{}, {hang: true, err: assert.AnError}}},
	}

	for name, store := range testCases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				coord := &fakeCoordination{acquireResults: []acquireResult{{acquired: false}}}
				hg := newTestHerdgate(store, WithCoordination(coord))

				want := []byte(_wantValue)
				var fetchCalls atomic.Int64
				fetch := func(ctx context.Context) ([]byte, error) {
					fetchCalls.Add(1)
					return want, nil
				}

				start := time.Now()
				got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
				require.NoError(t, err)
				elapsed := time.Since(start)
				synctest.Wait()

				assert.Equal(t, want, got)
				assert.Equal(t, _defaultFollowerTimeout, elapsed)
				assert.Equal(t, int64(1), fetchCalls.Load())
				assert.Equal(t, []setArgs{{key: _key, value: want, ttl: time.Minute}}, store.setArgs())
				assert.Empty(t, coord.releaseArgs())
			})
		})
	}
}

func TestHerdgate_DoCoordinationConcurrentCallsAcrossInstancesCoalesced(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr(), MaxRetries: -1, ContextTimeoutEnabled: true})
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
		return testutil.ToFloat64(hg1.metrics.coordinationOps.WithLabelValues(_testCoordinationOperationAcquire, _testResultAcquired)) == 1
	}, time.Second, time.Millisecond, "hg1 never acquired the lease")

	startConcurrentDoCalls(hg2, func(context.Context) ([]byte, error) {
		calls.Add(1)
		return want, nil
	})

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(hg1.metrics.coordinationOps.WithLabelValues(_testCoordinationOperationAcquire, _testResultContended)) == 1
	}, time.Second, time.Millisecond, "hg2 never observed the lease as contended")

	close(unblockLeaderFetch)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	assert.Equal(t, int64(1), calls.Load())
	assert.InDelta(t, 1, testutil.ToFloat64(hg1.metrics.coordinationOps.WithLabelValues(_testCoordinationOperationAcquire, _testResultAcquired)), 0)
}

func TestHerdgate_DoDecodeNilCodec(t *testing.T) {
	store := &fakeStore{}
	hg := newTestHerdgate(store)

	var calls atomic.Int64
	fetch := func(ctx context.Context) (jsonPayload, error) {
		calls.Add(1)
		return jsonPayload{Name: _jsonPayloadName}, nil
	}

	got, err := DoDecode(t.Context(), hg, _key, DoOptions{TTL: time.Minute}, fetch)
	require.EqualError(t, err, "herdgate: DoOptions.Codec must not be nil")
	assert.Equal(t, jsonPayload{}, got)
	assert.Equal(t, int64(0), calls.Load())
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
	store := &fakeStore{getResults: []getResult{{value: []byte(`{"name":"foo"}`), found: true}}}
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
	store := &fakeStore{getResults: []getResult{{value: []byte(`{"name":"foo"}`), found: true}}}
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

	var calls atomic.Int64
	fetch := func(ctx context.Context) (jsonPayload, error) {
		calls.Add(1)
		return want, nil
	}
	opts := DoOptions{TTL: time.Minute, Codec: codec}

	got, err := DoDecode(t.Context(), hg, _key, opts, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	got, err = DoDecode(t.Context(), hg, _key, opts, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	assert.Equal(t, int64(1), calls.Load())
	assert.Equal(t, int64(1), codec.marshalCount.Load())
	assert.Equal(t, int64(2), codec.unmarshalCount.Load())
}

func TestHerdgate_DoAfterClose(t *testing.T) {
	testCases := map[string]struct {
		store   *fakeStore
		wantVal []byte
		wantErr error
	}{
		"cache hit":  {store: &fakeStore{getResults: []getResult{{value: []byte(_wantValue), found: true}}}, wantVal: []byte(_wantValue)},
		"cache miss": {store: &fakeStore{}, wantErr: ErrClosed},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			hg := newTestHerdgate(tc.store)
			require.NoError(t, hg.Close(t.Context()))

			var fetchCalls atomic.Int64
			fetch := func(context.Context) ([]byte, error) {
				fetchCalls.Add(1)
				return nil, nil
			}

			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantVal, got)
			assert.Equal(t, int64(0), fetchCalls.Load())
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
		assertMetricCounts(t, hg.metrics, func(want *metrics) {
			want.recordCoordinationRenewContextDone(_anyStart)
		})
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
		coord  *fakeCoordination
		replay func(want *metrics)
	}{
		_testResultRenewed: {coord: &fakeCoordination{renewResults: []renewResult{{renewed: true}}}, replay: func(want *metrics) { want.recordCoordinationRenew(_anyStart, true, nil) }},
		_testResultLost:    {coord: &fakeCoordination{renewResults: []renewResult{{renewed: false}}}, replay: func(want *metrics) { want.recordCoordinationRenew(_anyStart, false, nil) }},
		_errorTestCase:     {coord: &fakeCoordination{renewResults: []renewResult{{err: assert.AnError}}}, replay: func(want *metrics) { want.recordCoordinationRenew(_anyStart, false, assert.AnError) }},
		"context.Canceled": {coord: &fakeCoordination{renewResults: []renewResult{{err: context.Canceled}}}, replay: func(want *metrics) { want.recordCoordinationRenew(_anyStart, false, assert.AnError) }}, // ctx still live
		_panicTestCase:     {coord: &fakeCoordination{renewPanic: _panicValue}, replay: func(want *metrics) { want.recordCoordinationRenew(_anyStart, false, assert.AnError) }},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			hg := newTestHerdgate(nil, WithCoordination(tc.coord))

			_, _ = hg.guardedRenew(t.Context(), _key, _token)
			assertMetricCounts(t, hg.metrics, tc.replay)
		})
	}
}

func TestHerdgate_GuardedRenewTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		coord := &fakeCoordination{renewResults: []renewResult{{hang: true}}}
		hg := newTestHerdgate(nil, WithCoordination(coord))

		start := time.Now()
		_, err := hg.guardedRenew(t.Context(), _key, _token)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, _defaultHeartbeatCallTimeout, time.Since(start))
		assertMetricCounts(t, hg.metrics, func(want *metrics) {
			want.recordCoordinationRenew(_anyStart, false, assert.AnError)
		})
	})
}

func TestHerdgate_GuardedRenewContextCanceled(t *testing.T) {
	testCases := map[string]struct {
		coord  *fakeCoordination
		replay func(want *metrics)
	}{
		"ctx error":       {coord: &fakeCoordination{renewResults: []renewResult{{hang: true}}}, replay: func(want *metrics) { want.recordCoordinationRenewContextDone(_anyStart) }},
		"any other error": {coord: &fakeCoordination{renewResults: []renewResult{{err: assert.AnError}}}, replay: func(want *metrics) { want.recordCoordinationRenew(_anyStart, false, assert.AnError) }},
		_panicTestCase:    {coord: &fakeCoordination{renewPanic: _panicValue}, replay: func(want *metrics) { want.recordCoordinationRenew(_anyStart, false, assert.AnError) }},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			hg := newTestHerdgate(nil, WithCoordination(tc.coord))
			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			_, _ = hg.guardedRenew(ctx, _key, _token)
			assertMetricCounts(t, hg.metrics, tc.replay)
		})
	}
}

func TestHerdgate_GuardedReleaseMetrics(t *testing.T) {
	testCases := map[string]struct {
		coord  *fakeCoordination
		replay func(want *metrics)
	}{
		_successTestCase: {coord: &fakeCoordination{}, replay: func(want *metrics) { want.recordCoordinationRelease(_anyStart, nil) }},
		_errorTestCase:   {coord: &fakeCoordination{releaseResults: []releaseResult{{err: assert.AnError}}}, replay: func(want *metrics) { want.recordCoordinationRelease(_anyStart, assert.AnError) }},
		_panicTestCase:   {coord: &fakeCoordination{releasePanic: _panicValue}, replay: func(want *metrics) { want.recordCoordinationRelease(_anyStart, assert.AnError) }},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			hg := newTestHerdgate(nil, WithCoordination(tc.coord))

			hg.guardedRelease(t.Context(), _key, _token)
			assertMetricCounts(t, hg.metrics, tc.replay)
		})
	}
}

func TestHerdgate_AwaitLeaderValueTimeout(t *testing.T) {
	testCases := map[string]struct {
		parentTimeout time.Duration
		timeout       time.Duration
		pollHang      bool
		pollErr       error
		wantElapsed   time.Duration
	}{
		"no parent deadline":                   {parentTimeout: 0, timeout: 150 * time.Millisecond, wantElapsed: 150 * time.Millisecond},
		"parent deadline shorter than timeout": {parentTimeout: 150 * time.Millisecond, timeout: time.Minute, wantElapsed: 150 * time.Millisecond},
		_pollHangTestCase:                      {parentTimeout: 0, timeout: 150 * time.Millisecond, pollHang: true, wantElapsed: 150 * time.Millisecond},
		_pollHangErrorTestCase:                 {parentTimeout: 0, timeout: 150 * time.Millisecond, pollHang: true, pollErr: assert.AnError, wantElapsed: 150 * time.Millisecond},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := &fakeStore{getResults: []getResult{{hang: tc.pollHang, err: tc.pollErr}}}
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

func TestHerdgate_AwaitLeaderValueStoreError(t *testing.T) {
	testCases := map[string]*fakeStore{
		_errorTestCase: {getResults: []getResult{{err: assert.AnError}}},
		_panicTestCase: {getPanic: assert.AnError},
	}

	for name, store := range testCases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				hg := newTestHerdgate(store)

				value, found, err := hg.awaitLeaderValue(t.Context(), _key, 250*time.Millisecond)
				require.ErrorIs(t, err, assert.AnError)
				assert.False(t, found)
				assert.Nil(t, value)
			})
		})
	}
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
