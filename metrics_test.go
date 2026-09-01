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

	_testResultAcquired  = "acquired"
	_testResultContended = "contended"

	_testOperationAcquire = "acquire"
	_testOperationRelease = "release"
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
		err        error
		wantResult string
	}{
		_testResultSuccess: {err: nil, wantResult: _testResultSuccess},
		_testResultError:   {err: assert.AnError, wantResult: _testResultError},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m := newMetrics()

			m.recordCacheWrite(tc.err)
			assert.InDelta(t, 1, testutil.ToFloat64(m.cacheWrites.WithLabelValues(tc.wantResult)), 0)
		})
	}
}

func TestMetrics_RecordAcquire(t *testing.T) {
	testCases := map[string]struct {
		acquired   bool
		err        error
		wantResult string
	}{
		_testResultAcquired:  {acquired: true, err: nil, wantResult: _testResultAcquired},
		_testResultContended: {acquired: false, err: nil, wantResult: _testResultContended},
		_testResultError:     {acquired: false, err: assert.AnError, wantResult: _testResultError},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m := newMetrics()

			m.recordAcquire(time.Now(), tc.acquired, tc.err)
			assert.InDelta(t, 1, testutil.ToFloat64(m.coordinationOps.WithLabelValues(_testOperationAcquire, tc.wantResult)), 0)
			assertHistogramObservedOnce(t, m.coordinationDuration, _testOperationAcquire, tc.wantResult)
		})
	}
}

func TestMetrics_RecordRelease(t *testing.T) {
	testCases := map[string]struct {
		err        error
		wantResult string
	}{
		_testResultSuccess: {err: nil, wantResult: _testResultSuccess},
		_testResultError:   {err: assert.AnError, wantResult: _testResultError},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m := newMetrics()

			m.recordRelease(time.Now(), tc.err)
			assert.InDelta(t, 1, testutil.ToFloat64(m.coordinationOps.WithLabelValues(_testOperationRelease, tc.wantResult)), 0)
			assertHistogramObservedOnce(t, m.coordinationDuration, _testOperationRelease, tc.wantResult)
		})
	}
}

func TestMetrics_ObserveFetch(t *testing.T) {
	testCases := map[string]struct {
		err        error
		wantResult string
	}{
		_testResultSuccess: {err: nil, wantResult: _testResultSuccess},
		_testResultError:   {err: assert.AnError, wantResult: _testResultError},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m := newMetrics()

			m.observeFetch(time.Now(), tc.err)
			assertHistogramObservedOnce(t, m.fetchDuration, tc.wantResult)
		})
	}
}

func TestMetrics_IncrementCoalescedWaiter(t *testing.T) {
	m := newMetrics()

	m.incrementCoalescedWaiter()
	assert.InDelta(t, 1, testutil.ToFloat64(m.coalescedWaiters), 0)
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

func TestMetrics_Gather(t *testing.T) {
	m := newMetrics()
	registry := prometheus.NewPedanticRegistry()

	require.NoError(t, registry.Register(m))
	_, err := registry.Gather()
	require.NoError(t, err)
}

func collectorFieldPointers(m *metrics) []uintptr {
	collectorType := reflect.TypeFor[prometheus.Collector]()
	value := reflect.ValueOf(*m)

	var pointers []uintptr
	for index := range value.NumField() {
		field := value.Field(index)
		if !field.Type().Implements(collectorType) {
			continue
		}
		// Interface-typed fields (e.g., prometheus.Counter) need unwrapping.
		if field.Kind() == reflect.Interface {
			field = field.Elem()
		}
		pointers = append(pointers, field.Pointer())
	}

	return pointers
}
