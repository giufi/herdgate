package herdgate

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertHistogramCount(t *testing.T, h *prometheus.HistogramVec, label string, want int) {
	t.Helper()
	histogram, ok := h.WithLabelValues(label).(prometheus.Histogram)
	require.True(t, ok)
	var sample dto.Metric
	require.NoError(t, histogram.Write(&sample))
	assert.EqualValues(t, want, sample.GetHistogram().GetSampleCount())
}
