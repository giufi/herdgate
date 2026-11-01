package herdgate

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _anyStart time.Time // assertMetricCounts ignores the start argument of replayed metric calls

// assertMetricCounts asserts that got holds the same counter values and histogram sample counts
// as new replayed metrics.
// Replayed arguments only select the series: values such as observed durations are not compared.
// The start argument can therefore be any value, such as _anyStart.
func assertMetricCounts(t *testing.T, got *metrics, replay func(want *metrics)) {
	t.Helper()
	want := newMetrics()
	replay(want)
	assert.Equal(t, recordedCounts(t, want), recordedCounts(t, got))
}

// recordedCounts maps each series in m to its counter value or histogram sample count.
func recordedCounts(t *testing.T, m *metrics) map[string]float64 {
	t.Helper()
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(m))
	families, err := registry.Gather()
	require.NoError(t, err)

	counts := make(map[string]float64)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			series := family.GetName() + fmt.Sprint(metric.GetLabel())
			counts[series] = metric.GetCounter().GetValue() + float64(metric.GetHistogram().GetSampleCount())
		}
	}
	return counts
}

func assertHistogramObservedOnce(t *testing.T, h *prometheus.HistogramVec, labels ...string) {
	t.Helper()
	assert.EqualValues(t, 1, histogramSnapshot(t, h, labels...).GetSampleCount())
}

func assertHistogramDuration(t *testing.T, h *prometheus.HistogramVec, want time.Duration, labels ...string) {
	t.Helper()
	assert.InDelta(t, want.Seconds(), histogramSnapshot(t, h, labels...).GetSampleSum(), 0)
}

func histogramSnapshot(t *testing.T, h *prometheus.HistogramVec, labels ...string) *dto.Histogram {
	t.Helper()
	histogram, ok := h.WithLabelValues(labels...).(prometheus.Histogram)
	require.True(t, ok)
	var metric dto.Metric
	require.NoError(t, histogram.Write(&metric))
	return metric.GetHistogram()
}
