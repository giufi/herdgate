package herdgate

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProvider_NewProviderMetricsInitialized(t *testing.T) {
	p := NewProvider()

	assert.ElementsMatch(t, describe(newMetrics()), describe(p.metrics))
}

func TestProvider_NewProviderMetricsIsolated(t *testing.T) {
	p1 := NewProvider()
	p2 := NewProvider()

	assert.NotSame(t, p1.Metrics(), p2.Metrics())
}

func TestProvider_MetricsSameAsInstance(t *testing.T) {
	p := NewProvider()
	assert.Same(t, p.metrics, p.Metrics())
}

func TestProvider_MetricsZeroValuePanic(t *testing.T) {
	p := &Provider{}
	assert.PanicsWithValue(t, "herdgate: Provider must be created with NewProvider", func() { p.Metrics() })
}

func TestProvider_NewMetricsShared(t *testing.T) {
	provider := NewProvider()
	require.NotNil(t, provider.Metrics())

	hg1 := provider.New(&fakeStore{})
	hg2 := provider.New(&fakeStore{})
	assert.Same(t, provider.Metrics(), hg1.metrics)
	assert.Same(t, provider.Metrics(), hg2.metrics)
}

func TestProvider_NewStoreSameAsGiven(t *testing.T) {
	provider := NewProvider()
	store := &fakeStore{}

	hg := provider.New(store)
	assert.Same(t, store, hg.store)
}

func TestProvider_NewNilStorePanic(t *testing.T) {
	testCases := map[string]Store{
		"nil":         nil,
		"nil pointer": (*fakeStore)(nil),
	}

	for name, store := range testCases {
		t.Run(name, func(t *testing.T) {
			p := NewProvider()
			assert.PanicsWithValue(t, "herdgate: store must not be nil", func() { p.New(store) })
		})
	}
}

func TestProvider_NewZeroValuePanic(t *testing.T) {
	p := &Provider{}
	assert.PanicsWithValue(t, "herdgate: Provider must be created with NewProvider", func() { p.New(&fakeStore{}) })
}

func describe(c prometheus.Collector) []string {
	ch := make(chan *prometheus.Desc, 10)
	c.Describe(ch)
	close(ch)

	var descs []string
	for d := range ch {
		descs = append(descs, d.String())
	}
	return descs
}
