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
	_testResultLookupHit         = "hit"
	_testResultLookupMiss        = "miss"
	_testResultLookupError       = "error"
	_testResultLookupContextDone = "context_done"

	_testResultSuccess     = "success"
	_testResultError       = "error"
	_testResultContextDone = "context_done"

	_testResultAcquired  = "acquired"
	_testResultContended = "contended"

	_testResultRenewed = "renewed"
	_testResultLost    = "lost"

	_testOperationAcquire      = "acquire"
	_testOperationAcquireRetry = "acquire_retry"
	_testOperationRelease      = "release"
	_testOperationRenew        = "renew"
)

func TestMetrics_IncrementCacheHit(t *testing.T) {
	m := newMetrics()

	m.incrementCacheHit()
	assert.InDelta(t, 1, testutil.ToFloat64(m.cacheLookups.WithLabelValues(_testResultLookupHit)), 0)
	assert.Equal(t, 1, testutil.CollectAndCount(m.cacheLookups))
}

func TestMetrics_IncrementCacheMiss(t *testing.T) {
	m := newMetrics()

	m.incrementCacheMiss()
	assert.InDelta(t, 1, testutil.ToFloat64(m.cacheLookups.WithLabelValues(_testResultLookupMiss)), 0)
	assert.Equal(t, 1, testutil.CollectAndCount(m.cacheLookups))
}

func TestMetrics_IncrementCacheLookupError(t *testing.T) {
	m := newMetrics()

	m.incrementCacheLookupError()
	assert.InDelta(t, 1, testutil.ToFloat64(m.cacheLookups.WithLabelValues(_testResultLookupError)), 0)
	assert.Equal(t, 1, testutil.CollectAndCount(m.cacheLookups))
}

func TestMetrics_IncrementCacheLookupContextDone(t *testing.T) {
	m := newMetrics()

	m.incrementCacheLookupContextDone()
	assert.InDelta(t, 1, testutil.ToFloat64(m.cacheLookups.WithLabelValues(_testResultLookupContextDone)), 0)
	assert.Equal(t, 1, testutil.CollectAndCount(m.cacheLookups))
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
			assert.Equal(t, 1, testutil.CollectAndCount(m.cacheWrites))
		})
	}
}

func TestMetrics_RecordCoordinationAcquire(t *testing.T) {
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

			m.recordCoordinationAcquire(time.Now(), tc.acquired, tc.err)
			assert.InDelta(t, 1, testutil.ToFloat64(m.coordinationOps.WithLabelValues(_testOperationAcquire, tc.wantResult)), 0)
			assertHistogramObservedOnce(t, m.coordinationDuration, _testOperationAcquire, tc.wantResult)
			assert.Equal(t, 1, testutil.CollectAndCount(m.coordinationOps))
			assert.Equal(t, 1, testutil.CollectAndCount(m.coordinationDuration))
		})
	}
}

func TestMetrics_RecordCoordinationAcquireRetry(t *testing.T) {
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

			m.recordCoordinationAcquireRetry(time.Now(), tc.acquired, tc.err)
			assert.InDelta(t, 1, testutil.ToFloat64(m.coordinationOps.WithLabelValues(_testOperationAcquireRetry, tc.wantResult)), 0)
			assertHistogramObservedOnce(t, m.coordinationDuration, _testOperationAcquireRetry, tc.wantResult)
			assert.Equal(t, 1, testutil.CollectAndCount(m.coordinationOps))
			assert.Equal(t, 1, testutil.CollectAndCount(m.coordinationDuration))
		})
	}
}

func TestMetrics_RecordCoordinationRenew(t *testing.T) {
	testCases := map[string]struct {
		renewed    bool
		err        error
		wantResult string
	}{
		_testResultRenewed: {renewed: true, err: nil, wantResult: _testResultRenewed},
		_testResultLost:    {renewed: false, err: nil, wantResult: _testResultLost},
		_testResultError:   {renewed: false, err: assert.AnError, wantResult: _testResultError},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m := newMetrics()

			m.recordCoordinationRenew(time.Now(), tc.renewed, tc.err)
			assert.InDelta(t, 1, testutil.ToFloat64(m.coordinationOps.WithLabelValues(_testOperationRenew, tc.wantResult)), 0)
			assertHistogramObservedOnce(t, m.coordinationDuration, _testOperationRenew, tc.wantResult)
			assert.Equal(t, 1, testutil.CollectAndCount(m.coordinationOps))
			assert.Equal(t, 1, testutil.CollectAndCount(m.coordinationDuration))
		})
	}
}

func TestMetrics_RecordCoordinationRenewContextDone(t *testing.T) {
	m := newMetrics()

	m.recordCoordinationRenewContextDone(time.Now())
	assert.InDelta(t, 1, testutil.ToFloat64(m.coordinationOps.WithLabelValues(_testOperationRenew, _testResultContextDone)), 0)
	assertHistogramObservedOnce(t, m.coordinationDuration, _testOperationRenew, _testResultContextDone)
	assert.Equal(t, 1, testutil.CollectAndCount(m.coordinationOps))
	assert.Equal(t, 1, testutil.CollectAndCount(m.coordinationDuration))
}

func TestMetrics_RecordCoordinationRelease(t *testing.T) {
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

			m.recordCoordinationRelease(time.Now(), tc.err)
			assert.InDelta(t, 1, testutil.ToFloat64(m.coordinationOps.WithLabelValues(_testOperationRelease, tc.wantResult)), 0)
			assertHistogramObservedOnce(t, m.coordinationDuration, _testOperationRelease, tc.wantResult)
			assert.Equal(t, 1, testutil.CollectAndCount(m.coordinationOps))
			assert.Equal(t, 1, testutil.CollectAndCount(m.coordinationDuration))
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
			assert.Equal(t, 1, testutil.CollectAndCount(m.fetchDuration))
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
