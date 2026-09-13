package herdgate

import (
	"context"
	"errors"
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
			assertHistogramObservedOnce(t, metrics.fetchDuration, "success")
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
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.fetchDuration))
}

func TestHerdgate_DoStoreGetPanic(t *testing.T) {
	want := []byte(_wantValue)
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
			wantErrText: "fetch panicked: assert.AnError general error for testing",
		},
		_nonErrValueTestCase: {panicVal: _panicValue, wantErrText: "fetch panicked: generic panic value"},
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
			require.EqualError(t, err, tc.wantErrText)
			if tc.wantErrIs != nil {
				require.ErrorIs(t, err, tc.wantErrIs)
			}
			assert.Nil(t, got)
			assert.Empty(t, store.setCalls)
			assertHistogramObservedOnce(t, metrics.fetchDuration, "error")
		})
	}
}

func TestHerdgate_DoStoreSetPanic(t *testing.T) {
	want := []byte(_wantValue)
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
	want := []byte(_wantValue)
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
	hg := NewProvider().New(store)

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
	store := inmemorystore.New()
	hg := NewProvider().New(store)

	const callers = 50
	fetchCalls := countConcurrentFetches(t, hg, callers)
	require.Equal(t, 1, fetchCalls)
	assert.InDelta(t, callers-1, testutil.ToFloat64(hg.metrics.coalescedWaiters), 0)
}

func TestHerdgate_DoConcurrentCallsAcrossInstancesNotCoalesced(t *testing.T) {
	provider := NewProvider()
	hg1 := provider.New(inmemorystore.New())
	hg2 := provider.New(inmemorystore.New())
	instances := []*Herdgate{hg1, hg2}

	fetchCalls := countConcurrentFetchesAcross(t, instances)
	require.Equal(t, 2, fetchCalls)
	assert.InDelta(t, 0, testutil.ToFloat64(hg1.metrics.coalescedWaiters), 0)
}

func TestHerdgate_DoCoordinationLeader(t *testing.T) {
	store := &fakeStore{}
	coord := &fakeCoordination{
		acquireResults: []acquireResult{{token: _token, acquired: true}},
		releaseNotify:  make(chan releaseCall, 1),
	}
	hg := &Herdgate{store: store, coordination: coord, metrics: newMetrics()}

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
	assert.Equal(t, []acquireCall{{key: _key, ttl: _defaultLeaseTTL}}, coord.acquireCalls)
	assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquire, _resultAcquired)), 0)

	select {
	case call := <-coord.releaseNotify:
		assert.Equal(t, releaseCall{key: _key, token: _token}, call)
	case <-time.After(time.Second):
		t.Fatal("did not receive the expected call to Coordination.Release")
	}
}

func TestHerdgate_DoCoordinationHeartbeat(t *testing.T) {
	coord := &fakeCoordination{
		acquireResults: []acquireResult{{token: _token, acquired: true}},
		renewResults:   []renewResult{{renewed: true}},
		renewNotify:    make(chan renewCall, 10),
	}
	hg := &Herdgate{store: &fakeStore{}, coordination: coord, metrics: newMetrics()}

	want := []byte(_wantValue)
	unblockFetch := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, func(context.Context) ([]byte, error) {
			<-unblockFetch
			return want, nil
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	}()

	select {
	case call := <-coord.renewNotify:
		assert.Equal(t, renewCall{key: _key, token: _token, ttl: _defaultLeaseTTL}, call)
	case <-time.After(time.Second):
		t.Fatal("did not receive the expected call to Coordination.Renew")
	}
	close(unblockFetch)

	// Wait for the Do call to finish
	<-done
}

func TestHerdgate_DoCoordinationAcquireError(t *testing.T) {
	testCases := map[string]*fakeCoordination{
		_errorTestCase: {acquireResults: []acquireResult{{err: assert.AnError}}},
		_panicTestCase: {acquirePanic: assert.AnError},
	}

	for name, coord := range testCases {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			hg := &Herdgate{store: store, coordination: coord, metrics: newMetrics()}

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
	store := &fakeStore{}
	coord := &fakeCoordination{
		acquireResults: []acquireResult{{token: _token, acquired: true}},
		releaseResults: []releaseResult{{err: assert.AnError}},
		releaseNotify:  make(chan releaseCall, 1),
	}
	hg := &Herdgate{store: store, coordination: coord, metrics: newMetrics()}

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

	select {
	case call := <-coord.releaseNotify:
		assert.Equal(t, releaseCall{key: _key, token: _token}, call)
	case <-time.After(time.Second):
		t.Fatal("did not receive the expected call to Coordination.Release")
	}
}

func TestHerdgate_DoCoordinationFollower(t *testing.T) {
	store := inmemorystore.New()
	coord := &fakeCoordination{acquireResults: []acquireResult{{acquired: false}}}
	hg := NewProvider().New(store, WithCoordination(coord))

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
	assert.Len(t, coord.acquireCalls, 1)
	assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquire, _resultContended)), 0)
	assert.Equal(t, 0, testutil.CollectAndCount(hg.metrics.cacheWrites))
}

func TestHerdgate_DoCoordinationFollowerRetryValueFound(t *testing.T) {
	store := inmemorystore.New()
	coord := &fakeCoordination{acquireResults: []acquireResult{{acquired: false}}}
	hg := NewProvider().New(store, WithCoordination(coord))

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
	assert.Len(t, coord.acquireCalls, 2)
	assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquireRetry, _resultContended)), 0)
}

func TestHerdgate_DoCoordinationFollowerRetryAcquired(t *testing.T) {
	store := &fakeStore{}
	coord := &fakeCoordination{
		acquireResults: []acquireResult{{acquired: false}, {token: _token, acquired: true}},
		renewResults:   []renewResult{{renewed: true}},
		releaseNotify:  make(chan releaseCall, 1),
	}
	hg := &Herdgate{store: store, coordination: coord, metrics: newMetrics()}

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
	assert.Equal(t, []setCall{{key: _key, value: want, ttl: time.Minute}}, store.setCalls)
	assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquireRetry, _resultAcquired)), 0)

	select {
	case call := <-coord.releaseNotify:
		assert.Equal(t, releaseCall{key: _key, token: _token}, call)
	case <-time.After(time.Second):
		t.Fatal("did not receive the expected call to Coordination.Release")
	}
}

func TestHerdgate_DoCoordinationFollowerRetryAcquireError(t *testing.T) {
	store := &fakeStore{}
	coord := &fakeCoordination{
		acquireResults: []acquireResult{{acquired: false}, {err: assert.AnError}},
	}
	hg := &Herdgate{store: store, coordination: coord, metrics: newMetrics()}

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
	assert.Len(t, coord.acquireCalls, 2)
	assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquireRetry, _resultError)), 0)
}

func TestHerdgate_DoCoordinationFollowerPollPanic(t *testing.T) {
	store := &fakeStore{getPanic: assert.AnError}
	coord := &fakeCoordination{acquireResults: []acquireResult{{acquired: false}}}
	hg := &Herdgate{store: store, coordination: coord, metrics: newMetrics()}

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
	assert.Len(t, coord.acquireCalls, 1)
	assert.InDelta(t, 0, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationAcquireRetry, _resultContended)), 0)
}

func TestHerdgate_DoDecodeNilCodec(t *testing.T) {
	store := &fakeStore{}
	hg := &Herdgate{store: store, metrics: newMetrics()}

	calls := 0
	fetch := func(ctx context.Context) (jsonPayload, error) {
		calls++
		return jsonPayload{Name: _jsonPayloadName}, nil
	}

	got, err := DoDecode(t.Context(), hg, _key, DoOptions{TTL: time.Minute}, fetch)
	require.EqualError(t, err, "herdgate: DoOptions.Codec must not be nil")
	assert.Equal(t, jsonPayload{}, got)
	assert.Equal(t, 0, calls)
	assert.Empty(t, store.getCalls)
}

func TestHerdgate_DoDecodeFetchError(t *testing.T) {
	store := &fakeStore{}
	hg := &Herdgate{store: store, metrics: newMetrics()}
	codec := &fakeCodec{}

	wantErr := assert.AnError
	fetch := func(ctx context.Context) (jsonPayload, error) {
		return jsonPayload{}, wantErr
	}

	got, err := DoDecode(t.Context(), hg, _key, DoOptions{TTL: time.Minute, Codec: codec}, fetch)
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, jsonPayload{}, got)
	assert.Empty(t, store.setCalls)
	assert.Equal(t, 0, codec.marshalCalls)
}

func TestHerdgate_DoDecodeMarshalError(t *testing.T) {
	codecErr := errors.New(_errorMsg)
	store := &fakeStore{}
	hg := &Herdgate{store: store, metrics: newMetrics()}
	codec := &fakeCodec{marshalErr: codecErr}

	fetch := func(ctx context.Context) (jsonPayload, error) {
		return jsonPayload{Name: _jsonPayloadName}, nil
	}

	got, err := DoDecode(t.Context(), hg, _key, DoOptions{TTL: time.Minute, Codec: codec}, fetch)
	require.ErrorIs(t, err, codecErr)
	assert.Equal(t, jsonPayload{}, got)
	assert.Empty(t, store.setCalls)
	assert.Equal(t, 0, codec.unmarshalCalls)
}

func TestHerdgate_DoDecodeMarshalPanic(t *testing.T) {
	store := &fakeStore{}
	hg := &Herdgate{store: store, metrics: newMetrics()}
	codec := &fakeCodec{marshalPanic: _panicValue}

	fetch := func(ctx context.Context) (jsonPayload, error) {
		return jsonPayload{Name: _jsonPayloadName}, nil
	}

	got, err := DoDecode(t.Context(), hg, _key, DoOptions{TTL: time.Minute, Codec: codec}, fetch)
	require.EqualError(t, err, "marshal herdgate.jsonPayload panicked: generic panic value")
	assert.Equal(t, jsonPayload{}, got)
	assert.Empty(t, store.setCalls)
	assert.Equal(t, 0, codec.unmarshalCalls)
}

func TestHerdgate_DoDecodeUnmarshalError(t *testing.T) {
	codecErr := errors.New(_errorMsg)
	store := &fakeStore{getValue: []byte(`{"name":"foo"}`), getFound: true}
	hg := &Herdgate{store: store, metrics: newMetrics()}
	codec := &fakeCodec{unmarshalErr: codecErr}

	fetch := func(ctx context.Context) (jsonPayload, error) {
		return jsonPayload{}, nil
	}

	got, err := DoDecode(t.Context(), hg, _key, DoOptions{TTL: time.Minute, Codec: codec}, fetch)
	require.ErrorIs(t, err, codecErr)
	assert.Equal(t, jsonPayload{}, got)
	assert.Equal(t, 0, codec.marshalCalls)
}

func TestHerdgate_DoDecodeUnmarshalPanic(t *testing.T) {
	store := &fakeStore{getValue: []byte(`{"name":"foo"}`), getFound: true}
	hg := &Herdgate{store: store, metrics: newMetrics()}
	codec := &fakeCodec{unmarshalPanic: _panicValue}

	fetch := func(ctx context.Context) (jsonPayload, error) {
		return jsonPayload{}, nil
	}

	got, err := DoDecode(t.Context(), hg, _key, DoOptions{TTL: time.Minute, Codec: codec}, fetch)
	require.EqualError(t, err, "unmarshal *herdgate.jsonPayload panicked: generic panic value")
	assert.Equal(t, jsonPayload{}, got)
	assert.Equal(t, 0, codec.marshalCalls)
}

func TestHerdgate_DoDecodeSequentialCallsCacheHit(t *testing.T) {
	want := jsonPayload{Name: _jsonPayloadName}
	codec := &fakeCodec{}
	hg := NewProvider().New(inmemorystore.New())

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
	assert.Equal(t, 1, codec.marshalCalls)
	assert.Equal(t, 2, codec.unmarshalCalls)
}

func TestHerdgate_StartHeartbeatPeriodicRenewal(t *testing.T) {
	coord := &fakeCoordination{renewResults: []renewResult{{renewed: true}}, renewNotify: make(chan renewCall, 10)}
	hg := &Herdgate{coordination: coord, metrics: newMetrics()}

	stop := hg.startHeartbeat(t.Context(), _key, _token)
	defer stop()

	for range 2 {
		select {
		case call := <-coord.renewNotify:
			assert.Equal(t, renewCall{key: _key, token: _token, ttl: _defaultLeaseTTL}, call)
		case <-time.After(time.Second):
			t.Fatal("did not receive the expected call to Coordination.Renew")
		}
	}
}

func TestHerdgate_StartHeartbeatStop(t *testing.T) {
	coord := &fakeCoordination{renewResults: []renewResult{{renewed: true}}}
	hg := &Herdgate{coordination: coord, metrics: newMetrics()}

	stop := hg.startHeartbeat(t.Context(), _key, _token)

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not return")
	}
}

func TestHerdgate_StartHeartbeatStopDuringRenew(t *testing.T) {
	coord := &fakeCoordination{
		renewResults: []renewResult{{renewed: true}},
		renewNotify:  make(chan renewCall, 10),
		renewHold:    make(chan struct{}),
	}
	hg := &Herdgate{coordination: coord, metrics: newMetrics()}

	stop := hg.startHeartbeat(t.Context(), _key, _token)

	select {
	case <-coord.renewNotify:
	case <-time.After(time.Second):
		t.Fatal("did not receive the expected call to Coordination.Renew")
	}

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not return during an in-flight renewal")
	}

	assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationRenew, _resultError)), 0)
}

func TestHerdgate_StartHeartbeatLeaseLost(t *testing.T) {
	coord := &fakeCoordination{renewResults: []renewResult{{renewed: false}}, renewNotify: make(chan renewCall, 10)}
	hg := &Herdgate{coordination: coord, metrics: newMetrics()}

	stop := hg.startHeartbeat(t.Context(), _key, _token)
	defer stop()

	select {
	case <-coord.renewNotify:
	case <-time.After(time.Second):
		t.Fatal("did not receive the expected call to Coordination.Renew")
	}

	// A confirmed loss ends the heartbeat on its own, without calling stop
	select {
	case <-coord.renewNotify:
		t.Fatal("received an unexpected call to Coordination.Renew after the lease was lost")
	case <-time.After(2 * _defaultHeartbeatInterval):
	}
}

func TestHerdgate_StartHeartbeatLeasePresumedLost(t *testing.T) {
	coord := &fakeCoordination{renewResults: []renewResult{{err: assert.AnError}}, renewNotify: make(chan renewCall, 10)}
	hg := &Herdgate{coordination: coord, metrics: newMetrics()}

	stop := hg.startHeartbeat(t.Context(), _key, _token)
	defer stop()

	// A presumed loss ends the heartbeat on its own, without calling stop
	renews := 0
	for {
		select {
		case <-coord.renewNotify:
			renews++
		case <-time.After(2 * _defaultHeartbeatInterval):
			assert.GreaterOrEqual(t, renews, int(_defaultLeaseTTL/_defaultHeartbeatInterval))
			return
		}
	}
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
			hg := &Herdgate{coordination: tc.coord, metrics: newMetrics()}

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
			hg := &Herdgate{coordination: tc.coord, metrics: newMetrics()}

			hg.guardedRelease(t.Context(), _key, _token)
			assert.InDelta(t, 1, testutil.ToFloat64(hg.metrics.coordinationOps.WithLabelValues(_coordinationOperationRelease, tc.wantResult)), 0)
		})
	}
}

func TestHerdgate_AwaitLeaderValueTimeout(t *testing.T) {
	testCases := map[string]struct {
		parentTimeout time.Duration
		timeout       time.Duration
	}{
		"no parent deadline":                   {parentTimeout: 0, timeout: 150 * time.Millisecond},
		"parent deadline shorter than timeout": {parentTimeout: 150 * time.Millisecond, timeout: time.Minute},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{getFound: false}
			hg := &Herdgate{store: store, metrics: newMetrics()}

			ctx := t.Context()
			if tc.parentTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.parentTimeout)
				defer cancel()
			}

			value, found, err := hg.awaitLeaderValue(ctx, _key, tc.timeout)
			require.NoError(t, err)
			assert.Nil(t, value)
			assert.False(t, found)
		})
	}
}

func TestHerdgate_AwaitLeaderValueStorePanic(t *testing.T) {
	store := &fakeStore{getPanic: assert.AnError}
	hg := &Herdgate{store: store, metrics: newMetrics()}

	value, found, err := hg.awaitLeaderValue(t.Context(), _key, 250*time.Millisecond)
	require.Error(t, err)
	assert.False(t, found)
	assert.Nil(t, value)
}

func countConcurrentFetches(t *testing.T, hg *Herdgate, callers int) int {
	t.Helper()

	return countConcurrentFetchesAcross(t, slices.Repeat([]*Herdgate{hg}, callers))
}

func countConcurrentFetchesAcross(t *testing.T, instances []*Herdgate) int {
	t.Helper()

	want := []byte(_wantValue)
	var calls atomic.Int64
	leaderStarted := make(chan struct{})
	var closeOnce sync.Once

	var done sync.WaitGroup
	done.Add(len(instances))
	errs := make(chan error, len(instances))
	callDo := func(hg *Herdgate) {
		defer done.Done()
		got, err := hg.Do(t.Context(), _key, DoOptions{TTL: time.Minute}, func(context.Context) ([]byte, error) {
			calls.Add(1)
			closeOnce.Do(func() { close(leaderStarted) })
			time.Sleep(50 * time.Millisecond) // margin for all the Do calls to happen
			return want, nil
		})
		errs <- err
		assert.Equal(t, want, got)
	}

	go callDo(instances[0])
	<-leaderStarted

	for _, hg := range instances[1:] {
		go callDo(hg)
	}

	done.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	return int(calls.Load())
}
