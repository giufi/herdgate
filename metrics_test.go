package herdgate

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestMetrics_Collect(t *testing.T) {
	m := newMetrics()

	m.incrementCacheMiss()
	m.observeFetch(time.Now(), nil)
	assert.Equal(t, 2, testutil.CollectAndCount(m))
}

func TestMetrics_IncrementCacheHit(t *testing.T) {
	m := newMetrics()

	m.incrementCacheHit()
	assert.InDelta(t, 1, testutil.ToFloat64(m.cacheLookups.WithLabelValues("hit")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(m.cacheLookups.WithLabelValues("miss")), 0)
}

func TestMetrics_IncrementCacheMiss(t *testing.T) {
	m := newMetrics()

	m.incrementCacheMiss()
	assert.InDelta(t, 1, testutil.ToFloat64(m.cacheLookups.WithLabelValues("miss")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(m.cacheLookups.WithLabelValues("hit")), 0)
}

func TestMetrics_ObserveFetch(t *testing.T) {
	testCases := map[string]struct {
		err   error
		label string
	}{
		"success": {err: nil, label: "success"},
		"error":   {err: assert.AnError, label: "error"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m := newMetrics()

			m.observeFetch(time.Now(), tc.err)
			assertHistogramCount(t, m.upstreamFetchDuration, tc.label, 1)
		})
	}
}
