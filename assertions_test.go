package herdgate

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertHistogramObservedOnce(t *testing.T, h *prometheus.HistogramVec, labels ...string) {
	t.Helper()
	histogram, ok := h.WithLabelValues(labels...).(prometheus.Histogram)
	require.True(t, ok)
	var sample dto.Metric
	require.NoError(t, histogram.Write(&sample))
	assert.EqualValues(t, 1, sample.GetHistogram().GetSampleCount())
}
