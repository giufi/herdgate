package herdgate

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics holds the Prometheus metrics for a [Herdgate] instance.
//
// It implements [prometheus.Collector].
type metrics struct {
	cacheLookups          *prometheus.CounterVec
	upstreamFetchDuration *prometheus.HistogramVec
}

var _ prometheus.Collector = (*metrics)(nil)

const (
	_fetchResultSuccess = "success"
	_fetchResultError   = "error"
)

func newMetrics() *metrics {
	return &metrics{
		cacheLookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "herdgate_cache_lookups_total",
			Help: "Total cache lookups.",
		}, []string{"result"}),
		upstreamFetchDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "herdgate_upstream_fetch_duration_seconds",
			Help: "Duration of upstream fetches, in seconds.",
		}, []string{"result"}),
	}
}

func (m *metrics) incrementCacheHit() {
	m.cacheLookups.WithLabelValues("hit").Inc()
}

func (m *metrics) incrementCacheMiss() {
	m.cacheLookups.WithLabelValues("miss").Inc()
}

func (m *metrics) observeFetch(start time.Time, err error) {
	result := _fetchResultSuccess
	if err != nil {
		result = _fetchResultError
	}
	m.upstreamFetchDuration.WithLabelValues(result).Observe(time.Since(start).Seconds())
}

func (m *metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.cacheLookups, m.upstreamFetchDuration}
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
