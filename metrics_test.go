package herdgate

import (
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	_testResultSuccess = "success"
	_testResultError   = "error"
)

func TestMetrics_IncrementCacheHit(t *testing.T) {
	m := newMetrics()

	m.incrementCacheHit()
	assert.InDelta(t, 1, testutil.ToFloat64(m.cacheLookups.WithLabelValues("hit")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(m.cacheLookups.WithLabelValues("miss")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(m.cacheLookups.WithLabelValues("error")), 0)
}

func TestMetrics_IncrementCacheMiss(t *testing.T) {
	m := newMetrics()

	m.incrementCacheMiss()
	assert.InDelta(t, 1, testutil.ToFloat64(m.cacheLookups.WithLabelValues("miss")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(m.cacheLookups.WithLabelValues("hit")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(m.cacheLookups.WithLabelValues("error")), 0)
}

func TestMetrics_IncrementCacheLookupError(t *testing.T) {
	m := newMetrics()

	m.incrementCacheLookupError()
	assert.InDelta(t, 1, testutil.ToFloat64(m.cacheLookups.WithLabelValues("error")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(m.cacheLookups.WithLabelValues("hit")), 0)
	assert.InDelta(t, 0, testutil.ToFloat64(m.cacheLookups.WithLabelValues("miss")), 0)
}

func TestMetrics_RecordCacheWrite(t *testing.T) {
	testCases := map[string]struct {
		err   error
		label string
	}{
		"success": {err: nil, label: _testResultSuccess},
		"error":   {err: assert.AnError, label: _testResultError},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m := newMetrics()

			m.recordCacheWrite(tc.err)
			assert.InDelta(t, 1, testutil.ToFloat64(m.cacheWrites.WithLabelValues(tc.label)), 0)
		})
	}
}

func TestMetrics_ObserveFetch(t *testing.T) {
	testCases := map[string]struct {
		err   error
		label string
	}{
		"success": {err: nil, label: _testResultSuccess},
		"error":   {err: assert.AnError, label: _testResultError},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m := newMetrics()

			m.observeFetch(time.Now(), tc.err)
			assertHistogramObservedOnce(t, m.fetchDuration, tc.label)
		})
	}
}

func TestMetrics_Collectors(t *testing.T) {
	m := newMetrics()

	var collectorPointers []uintptr
	for _, c := range m.collectors() {
		collectorPointers = append(collectorPointers, reflect.ValueOf(c).Pointer())
	}

	assert.ElementsMatch(t, collectorFieldPointers(m), collectorPointers)
}

func TestMetrics_Register(t *testing.T) {
	m := newMetrics()
	registry := prometheus.NewPedanticRegistry()

	for _, c := range m.collectors() {
		require.NoError(t, registry.Register(c))
	}
}

func collectorFieldPointers(m *metrics) []uintptr {
	collectorType := reflect.TypeFor[prometheus.Collector]()
	value := reflect.ValueOf(*m)

	var pointers []uintptr
	for index := range value.NumField() {
		field := value.Field(index)
		if field.Type().Implements(collectorType) {
			pointers = append(pointers, field.Pointer())
		}
	}

	return pointers
}
