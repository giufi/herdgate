package herdgate

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics holds the Prometheus metrics for a [Herdgate] instance.
//
// It implements [prometheus.Collector].
type metrics struct {
	storeOps             *prometheus.CounterVec
	storeDuration        *prometheus.HistogramVec
	coordinationOps      *prometheus.CounterVec
	coordinationDuration *prometheus.HistogramVec
	fetchDuration        *prometheus.HistogramVec
	coalescedWaiters     prometheus.Counter
}

var _ prometheus.Collector = (*metrics)(nil)

const (
	_labelResult    = "result"
	_labelOperation = "operation"

	_resultLookupHit         = "hit"
	_resultLookupMiss        = "miss"
	_resultLookupError       = "error"
	_resultLookupContextDone = "context_done"

	_resultSuccess     = "success"
	_resultError       = "error"
	_resultContextDone = "context_done"

	_resultAcquired  = "acquired"
	_resultContended = "contended"

	_resultRenewed = "renewed"
	_resultLost    = "lost"

	_storeOperationGet = "get"
	_storeOperationSet = "set"

	_coordinationOperationAcquire      = "acquire"
	_coordinationOperationAcquireRetry = "acquire_retry"
	_coordinationOperationRelease      = "release"
	_coordinationOperationRenew        = "renew"
)

func newMetrics() *metrics {
	return &metrics{
		storeOps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "herdgate_store_operations_total",
			Help: "Total Store operations, by operation and result.",
		}, []string{_labelOperation, _labelResult}),
		storeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "herdgate_store_duration_seconds",
			Help: "Duration of Store operations, in seconds, by operation and result.",
		}, []string{_labelOperation, _labelResult}),
		coordinationOps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "herdgate_coordination_operations_total",
			Help: "Total Coordination operations, by operation and result.",
		}, []string{_labelOperation, _labelResult}),
		coordinationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "herdgate_coordination_duration_seconds",
			Help: "Duration of Coordination operations, in seconds, by operation and result.",
		}, []string{_labelOperation, _labelResult}),
		fetchDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "herdgate_fetch_duration_seconds",
			Help: "Duration of fetch calls, in seconds. With DoDecode, each duration also includes marshaling the fetched value.",
		}, []string{_labelResult}),
		coalescedWaiters: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "herdgate_coalesced_waiters_total",
			Help: "Total callers that waited for an in-flight fetch result.",
		}),
	}
}

func (m *metrics) recordCacheHit(start time.Time) {
	m.recordStoreOperation(_storeOperationGet, _resultLookupHit, start)
}

func (m *metrics) recordCacheMiss(start time.Time) {
	m.recordStoreOperation(_storeOperationGet, _resultLookupMiss, start)
}

func (m *metrics) recordCacheLookupError(start time.Time) {
	m.recordStoreOperation(_storeOperationGet, _resultLookupError, start)
}

func (m *metrics) recordCacheLookupContextDone(start time.Time) {
	m.recordStoreOperation(_storeOperationGet, _resultLookupContextDone, start)
}

func (m *metrics) recordCacheWrite(start time.Time, err error) {
	result := _resultSuccess
	if err != nil {
		result = _resultError
	}
	m.recordStoreOperation(_storeOperationSet, result, start)
}

func (m *metrics) recordStoreOperation(operation, result string, start time.Time) {
	m.storeOps.WithLabelValues(operation, result).Inc()
	m.storeDuration.WithLabelValues(operation, result).Observe(time.Since(start).Seconds())
}

func (m *metrics) recordCoordinationAcquire(start time.Time, acquired bool, err error) {
	var result string
	switch {
	case err != nil:
		result = _resultError
	case acquired:
		result = _resultAcquired
	default:
		result = _resultContended
	}
	m.recordCoordinationOperation(_coordinationOperationAcquire, result, start)
}

func (m *metrics) recordCoordinationAcquireRetry(start time.Time, acquired bool, err error) {
	var result string
	switch {
	case err != nil:
		result = _resultError
	case acquired:
		result = _resultAcquired
	default:
		result = _resultContended
	}
	m.recordCoordinationOperation(_coordinationOperationAcquireRetry, result, start)
}

func (m *metrics) recordCoordinationRenew(start time.Time, renewed bool, err error) {
	var result string
	switch {
	case err != nil:
		result = _resultError
	case renewed:
		result = _resultRenewed
	default:
		result = _resultLost
	}
	m.recordCoordinationOperation(_coordinationOperationRenew, result, start)
}

func (m *metrics) recordCoordinationRenewContextDone(start time.Time) {
	m.recordCoordinationOperation(_coordinationOperationRenew, _resultContextDone, start)
}

func (m *metrics) recordCoordinationRelease(start time.Time, err error) {
	result := _resultSuccess
	if err != nil {
		result = _resultError
	}
	m.recordCoordinationOperation(_coordinationOperationRelease, result, start)
}

func (m *metrics) recordCoordinationOperation(operation, result string, start time.Time) {
	m.coordinationOps.WithLabelValues(operation, result).Inc()
	m.coordinationDuration.WithLabelValues(operation, result).Observe(time.Since(start).Seconds())
}

func (m *metrics) observeFetch(start time.Time, err error) {
	result := _resultSuccess
	if err != nil {
		result = _resultError
	}
	m.fetchDuration.WithLabelValues(result).Observe(time.Since(start).Seconds())
}

func (m *metrics) incrementCoalescedWaiter() {
	m.coalescedWaiters.Inc()
}

func (m *metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.storeOps,
		m.storeDuration,
		m.coordinationOps,
		m.coordinationDuration,
		m.fetchDuration,
		m.coalescedWaiters,
	}
}

// Describe implements [prometheus.Collector].
func (m *metrics) Describe(ch chan<- *prometheus.Desc) {
	for _, c := range m.collectors() {
		c.Describe(ch)
	}
}

// Collect implements [prometheus.Collector].
func (m *metrics) Collect(ch chan<- prometheus.Metric) {
	for _, c := range m.collectors() {
		c.Collect(ch)
	}
}
