package herdgate

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics holds the Prometheus metrics for a [Herdgate] instance.
//
// It implements [prometheus.Collector].
type metrics struct {
	cacheLookups  *prometheus.CounterVec
	cacheWrites   *prometheus.CounterVec
	fetchDuration *prometheus.HistogramVec
}

var _ prometheus.Collector = (*metrics)(nil)

const (
	_labelResult = "result"

	_valueLookupHit   = "hit"
	_valueLookupMiss  = "miss"
	_valueLookupError = "error"

	_valueSuccess = "success"
	_valueError   = "error"
)

func newMetrics() *metrics {
	return &metrics{
		cacheLookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "herdgate_cache_lookups_total",
			Help: "Total cache lookup attempts, by result.",
		}, []string{_labelResult}),
		cacheWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "herdgate_cache_writes_total",
			Help: "Total cache write attempts, by result.",
		}, []string{_labelResult}),
		fetchDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "herdgate_fetch_duration_seconds",
			Help: "Duration of fetch calls, in seconds. With DoDecode, each duration also includes marshaling the fetched value.",
		}, []string{_labelResult}),
	}
}

func (m *metrics) incrementCacheHit() {
	m.cacheLookups.WithLabelValues(_valueLookupHit).Inc()
}

func (m *metrics) incrementCacheMiss() {
	m.cacheLookups.WithLabelValues(_valueLookupMiss).Inc()
}

func (m *metrics) incrementCacheLookupError() {
	m.cacheLookups.WithLabelValues(_valueLookupError).Inc()
}

func (m *metrics) recordCacheWrite(err error) {
	result := _valueSuccess
	if err != nil {
		result = _valueError
	}
	m.cacheWrites.WithLabelValues(result).Inc()
}

func (m *metrics) observeFetch(start time.Time, err error) {
	result := _valueSuccess
	if err != nil {
		result = _valueError
	}
	m.fetchDuration.WithLabelValues(result).Observe(time.Since(start).Seconds())
}

func (m *metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.cacheLookups, m.cacheWrites, m.fetchDuration}
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
