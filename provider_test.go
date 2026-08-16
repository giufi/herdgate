package herdgate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProvider_NewProviderMetricsIsolated(t *testing.T) {
	provider1 := NewProvider()
	provider2 := NewProvider()

	assert.NotSame(t, provider1.Metrics(), provider2.Metrics())
}

func TestProvider_NewMetricsShared(t *testing.T) {
	provider := NewProvider()
	require.NotNil(t, provider.Metrics())

	hg1 := provider.New()
	hg2 := provider.New()

	assert.Same(t, provider.Metrics(), hg1.metrics)
	assert.Same(t, provider.Metrics(), hg2.metrics)
}

func TestProvider_NewStoreIsolated(t *testing.T) {
	provider := NewProvider()

	hg1 := provider.New()
	hg2 := provider.New()

	assert.NotSame(t, hg1.store, hg2.store)
}
