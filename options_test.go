package herdgate

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProviderOption_WithRegistererNilPanic(t *testing.T) {
	assert.PanicsWithValue(t, "herdgate: registerer must not be nil", func() { NewProvider(WithRegisterer(nil)) })
}

func TestProviderOption_WithRegistererCollisionPanic(t *testing.T) {
	registry := prometheus.NewRegistry()
	firstProvider := NewProvider(WithRegisterer(registry))

	panicValue := recoverPanic(func() { NewProvider(WithRegisterer(registry)) })
	err, ok := panicValue.(error)
	require.True(t, ok)

	var target prometheus.AlreadyRegisteredError
	require.ErrorAs(t, err, &target)
	assert.Same(t, firstProvider.metrics, target.ExistingCollector)
}

func TestProviderOption_WithRegistererRegistered(t *testing.T) {
	registry := prometheus.NewRegistry()

	p := NewProvider(WithRegisterer(registry))
	assert.True(t, registry.Unregister(p.metrics))
}

func recoverPanic(fn func()) (panicValue any) {
	defer func() { panicValue = recover() }()
	fn()
	return
}
