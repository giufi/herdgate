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

	_testStoreOperationGet = "get"
	_testStoreOperationSet = "set"

	_testCoordinationOperationAcquire      = "acquire"
	_testCoordinationOperationAcquireRetry = "acquire_retry"
	_testCoordinationOperationRelease      = "release"
	_testCoordinationOperationRenew        = "renew"
)

func TestMetrics_RecordCacheHit(t *testing.T) {
	m := newMetrics()

	m.recordCacheHit(time.Now())
	assert.InDelta(t, 1, testutil.ToFloat64(m.storeOps.WithLabelValues(_testStoreOperationGet, _testResultLookupHit)), 0)
	assertHistogramObservedOnce(t, m.storeDuration, _testStoreOperationGet, _testResultLookupHit)
	assert.Equal(t, 1, testutil.CollectAndCount(m.storeOps))
	assert.Equal(t, 1, testutil.CollectAndCount(m.storeDuration))
}

func TestMetrics_RecordCacheMiss(t *testing.T) {
	m := newMetrics()

	m.recordCacheMiss(time.Now())
	assert.InDelta(t, 1, testutil.ToFloat64(m.storeOps.WithLabelValues(_testStoreOperationGet, _testResultLookupMiss)), 0)
	assertHistogramObservedOnce(t, m.storeDuration, _testStoreOperationGet, _testResultLookupMiss)
	assert.Equal(t, 1, testutil.CollectAndCount(m.storeOps))
	assert.Equal(t, 1, testutil.CollectAndCount(m.storeDuration))
}

func TestMetrics_RecordCacheLookupError(t *testing.T) {
	m := newMetrics()

	m.recordCacheLookupError(time.Now())
	assert.InDelta(t, 1, testutil.ToFloat64(m.storeOps.WithLabelValues(_testStoreOperationGet, _testResultLookupError)), 0)
	assertHistogramObservedOnce(t, m.storeDuration, _testStoreOperationGet, _testResultLookupError)
	assert.Equal(t, 1, testutil.CollectAndCount(m.storeOps))
	assert.Equal(t, 1, testutil.CollectAndCount(m.storeDuration))
}

func TestMetrics_RecordCacheLookupContextDone(t *testing.T) {
	m := newMetrics()

	m.recordCacheLookupContextDone(time.Now())
	assert.InDelta(t, 1, testutil.ToFloat64(m.storeOps.WithLabelValues(_testStoreOperationGet, _testResultLookupContextDone)), 0)
	assertHistogramObservedOnce(t, m.storeDuration, _testStoreOperationGet, _testResultLookupContextDone)
	assert.Equal(t, 1, testutil.CollectAndCount(m.storeOps))
	assert.Equal(t, 1, testutil.CollectAndCount(m.storeDuration))
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

			m.recordCacheWrite(time.Now(), tc.err)
			assert.InDelta(t, 1, testutil.ToFloat64(m.storeOps.WithLabelValues(_testStoreOperationSet, tc.wantResult)), 0)
			assertHistogramObservedOnce(t, m.storeDuration, _testStoreOperationSet, tc.wantResult)
			assert.Equal(t, 1, testutil.CollectAndCount(m.storeOps))
			assert.Equal(t, 1, testutil.CollectAndCount(m.storeDuration))
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
			assert.InDelta(t, 1, testutil.ToFloat64(m.coordinationOps.WithLabelValues(_testCoordinationOperationAcquire, tc.wantResult)), 0)
			assertHistogramObservedOnce(t, m.coordinationDuration, _testCoordinationOperationAcquire, tc.wantResult)
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
			assert.InDelta(t, 1, testutil.ToFloat64(m.coordinationOps.WithLabelValues(_testCoordinationOperationAcquireRetry, tc.wantResult)), 0)
			assertHistogramObservedOnce(t, m.coordinationDuration, _testCoordinationOperationAcquireRetry, tc.wantResult)
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
			assert.InDelta(t, 1, testutil.ToFloat64(m.coordinationOps.WithLabelValues(_testCoordinationOperationRenew, tc.wantResult)), 0)
			assertHistogramObservedOnce(t, m.coordinationDuration, _testCoordinationOperationRenew, tc.wantResult)
			assert.Equal(t, 1, testutil.CollectAndCount(m.coordinationOps))
			assert.Equal(t, 1, testutil.CollectAndCount(m.coordinationDuration))
		})
	}
}

func TestMetrics_RecordCoordinationRenewContextDone(t *testing.T) {
	m := newMetrics()

	m.recordCoordinationRenewContextDone(time.Now())
	assert.InDelta(t, 1, testutil.ToFloat64(m.coordinationOps.WithLabelValues(_testCoordinationOperationRenew, _testResultContextDone)), 0)
	assertHistogramObservedOnce(t, m.coordinationDuration, _testCoordinationOperationRenew, _testResultContextDone)
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
			assert.InDelta(t, 1, testutil.ToFloat64(m.coordinationOps.WithLabelValues(_testCoordinationOperationRelease, tc.wantResult)), 0)
			assertHistogramObservedOnce(t, m.coordinationDuration, _testCoordinationOperationRelease, tc.wantResult)
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
