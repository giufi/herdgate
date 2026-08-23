package herdgate

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/giufi/herdgate/store/inmemorystore"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const _key = "key"

func TestHerdgate_DoCacheMiss(t *testing.T) {
	want := []byte("foo")

	testCases := map[string]time.Duration{
		"positive TTL": time.Minute,
		"zero TTL":     0,
		"negative TTL": -time.Minute,
	}

	for name, ttl := range testCases {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			metrics := newMetrics()
			hg := &Herdgate{store: store, metrics: metrics}

			calls := 0
			fetch := func(ctx context.Context) ([]byte, error) {
				calls++
				return want, nil
			}

			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: ttl}, fetch)
			require.NoError(t, err)
			assert.Equal(t, want, got)
			assert.Equal(t, 1, calls)
			assert.Equal(t, []string{_key}, store.getCalls)
			assert.Equal(t, []setCall{{key: _key, value: want, ttl: ttl}}, store.setCalls)
			assert.InDelta(t, 1, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("miss")), 0)
			assert.InDelta(t, 0, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("hit")), 0)
			assertHistogramObservedOnce(t, metrics.upstreamFetchDuration, "success")
		})
	}
}

func TestHerdgate_DoCacheHit(t *testing.T) {
	want := []byte("cached")
	store := &fakeStore{getValue: want, getFound: true}
	metrics := newMetrics()
	hg := &Herdgate{store: store, metrics: metrics}

	calls := 0
	fetch := func(ctx context.Context) ([]byte, error) {
		calls++
		return []byte("should not be called"), nil
	}

	got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, 0, calls)
	assert.Equal(t, []string{_key}, store.getCalls)
	assert.Empty(t, store.setCalls)
	assert.InDelta(t, 1, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("hit")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("miss")), 0)
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.upstreamFetchDuration))
}

func TestHerdgate_DoStoreGetPanic(t *testing.T) {
	want := []byte("foo")
	store := &fakeStore{getPanic: assert.AnError}
	metrics := newMetrics()
	hg := &Herdgate{store: store, metrics: metrics}

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
	hg := &Herdgate{store: store, metrics: metrics}

	wantErr := assert.AnError
	fetch := func(ctx context.Context) ([]byte, error) {
		return nil, wantErr
	}

	got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, got)
	assert.Equal(t, []string{_key}, store.getCalls)
	assert.Empty(t, store.setCalls)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("hit")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("miss")), 0)
	assertHistogramObservedOnce(t, metrics.upstreamFetchDuration, "error")
}

func TestHerdgate_DoFetchPanic(t *testing.T) {
	testCases := map[string]struct {
		panicVal    any
		wantErrIs   error
		wantErrText string
	}{
		"error value":     {panicVal: assert.AnError, wantErrIs: assert.AnError},
		"non-error value": {panicVal: "boom", wantErrText: "fetch panicked: boom"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			metrics := newMetrics()
			hg := &Herdgate{store: store, metrics: metrics}

			fetch := func(ctx context.Context) ([]byte, error) {
				panic(tc.panicVal)
			}

			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetch)
			if tc.wantErrIs != nil {
				require.ErrorIs(t, err, tc.wantErrIs)
			} else {
				require.EqualError(t, err, tc.wantErrText)
			}
			assert.Nil(t, got)
			assert.Empty(t, store.setCalls)
			assertHistogramObservedOnce(t, metrics.upstreamFetchDuration, "error")
		})
	}
}

func TestHerdgate_DoStoreSetPanic(t *testing.T) {
	want := []byte("foo")
	store := &fakeStore{setPanic: assert.AnError}
	metrics := newMetrics()
	hg := &Herdgate{store: store, metrics: metrics}

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
	want := []byte("foo")
	ttl := time.Minute
	store := &fakeStore{setNotify: make(chan setCall, 1)}
	metrics := newMetrics()
	hg := &Herdgate{store: store, metrics: metrics}

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

	select {
	case call := <-store.setNotify:
		assert.Equal(t, setCall{key: _key, value: want, ttl: ttl}, call)
	case <-time.After(time.Second):
		t.Fatal("did not receive the expected call to store.Set")
	}
}

func TestHerdgate_DoSequentialCallsCacheHit(t *testing.T) {
	store := inmemorystore.New()
	hg := NewProvider().New(store)

	want := []byte("foo")
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
	assertHistogramObservedOnce(t, hg.metrics.upstreamFetchDuration, "success")
}

func TestHerdgate_DoFetchErrorThenCacheMiss(t *testing.T) {
	store := inmemorystore.New()
	hg := NewProvider().New(store)

	fetchErr := func(context.Context) ([]byte, error) {
		return nil, assert.AnError
	}

	_, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, fetchErr)
	require.ErrorIs(t, err, assert.AnError)

	want := []byte("foo")
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
	assertHistogramObservedOnce(t, hg.metrics.upstreamFetchDuration, "error")
	assertHistogramObservedOnce(t, hg.metrics.upstreamFetchDuration, "success")
}

func TestHerdgate_DoConcurrentCallsCollapsed(t *testing.T) {
	store := inmemorystore.New()
	hg := NewProvider().New(store)
	assert.Equal(t, 1, countConcurrentFetches(t, hg, 50))
}

func TestHerdgate_DoConcurrentCallsAcrossInstancesNotCollapsed(t *testing.T) {
	provider := NewProvider()
	hg1 := provider.New(inmemorystore.New())
	hg2 := provider.New(inmemorystore.New())
	instances := []*Herdgate{hg1, hg2}
	assert.Equal(t, 2, countConcurrentFetchesAcross(t, instances))
}

func countConcurrentFetches(t *testing.T, hg *Herdgate, callers int) int {
	t.Helper()

	return countConcurrentFetchesAcross(t, slices.Repeat([]*Herdgate{hg}, callers))
}

func countConcurrentFetchesAcross(t *testing.T, instances []*Herdgate) int {
	t.Helper()

	want := []byte("foo")
	var calls atomic.Int64
	unblockFetch := make(chan struct{})

	var ready, done sync.WaitGroup
	ready.Add(len(instances))
	done.Add(len(instances))
	for _, hg := range instances {
		go func() {
			defer done.Done()
			ready.Done()
			got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, func(context.Context) ([]byte, error) {
				calls.Add(1)
				<-unblockFetch // held open until every caller is about to call Do
				return want, nil
			})
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		}()
	}

	ready.Wait()
	close(unblockFetch)
	done.Wait()

	return int(calls.Load())
}
