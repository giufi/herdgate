package herdgate

import (
	"context"
	"testing"
	"time"

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

			got, err := hg.Do(t.Context(), _key, ttl, fetch)
			require.NoError(t, err)
			assert.Equal(t, want, got)
			assert.Equal(t, 1, calls)
			assert.Equal(t, []string{_key}, store.getCalls)
			assert.Equal(t, []setCall{{key: _key, value: want, ttl: ttl}}, store.setCalls)
			assert.InDelta(t, 1, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("miss")), 0)
			assert.InDelta(t, 0, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("hit")), 0)
			assertHistogramCount(t, metrics.upstreamFetchDuration, "success", 1)
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

	got, err := hg.Do(t.Context(), _key, time.Minute, fetch)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, 0, calls)
	assert.Equal(t, []string{_key}, store.getCalls)
	assert.Empty(t, store.setCalls)
	assert.InDelta(t, 1, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("hit")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("miss")), 0)
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.upstreamFetchDuration))
}

func TestHerdgate_DoFetchError(t *testing.T) {
	store := &fakeStore{}
	metrics := newMetrics()
	hg := &Herdgate{store: store, metrics: metrics}

	wantErr := assert.AnError
	fetch := func(ctx context.Context) ([]byte, error) {
		return nil, wantErr
	}

	got, err := hg.Do(t.Context(), _key, time.Minute, fetch)
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, got)
	assert.Equal(t, []string{_key}, store.getCalls)
	assert.Empty(t, store.setCalls)
	assert.InDelta(t, 0, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("hit")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(metrics.cacheLookups.WithLabelValues("miss")), 0)
	assertHistogramCount(t, metrics.upstreamFetchDuration, "error", 1)
}
