package herdgate

import (
	"reflect"

	"github.com/prometheus/client_golang/prometheus"
)

// A Provider creates new [Herdgate] instances, sharing metrics among them.
//
// Register the metrics with a [prometheus.Registerer] via [Provider.Metrics].
//
// Use one Provider per [prometheus.Registerer]. Registering two Providers on
// the same Registerer fails: their metric names are identical.
type Provider struct {
	metrics *metrics
}

// NewProvider creates a [Provider].
//
// Register its metrics with a [prometheus.Registerer] via [Provider.Metrics].
//
// Use one Provider per Registerer: registering two Providers on the same
// Registerer fails, since their metric names are identical.
func NewProvider() *Provider {
	return &Provider{metrics: newMetrics()}
}

// Metrics returns a [prometheus.Collector] holding the provider's metrics.
// Every [Herdgate] the provider creates shares the returned collector.
//
// Register the returned collector with a [prometheus.Registerer].
//
// It panics if p was not created with [NewProvider].
func (p *Provider) Metrics() prometheus.Collector {
	p.checkMetrics()
	return p.metrics
}

// New creates a [Herdgate] backed by the given [Store] and sharing the
// provider's metrics.
//
// It panics if the given store is nil, or if p was not created with
// [NewProvider].
func (p *Provider) New(store Store) *Herdgate {
	if isNilStore(store) {
		panic("herdgate: store must not be nil")
	}
	p.checkMetrics()
	return newHerdgate(store, p.metrics)
}

func (p *Provider) checkMetrics() {
	if p.metrics == nil {
		panic("herdgate: Provider must be created with NewProvider")
	}
}

func isNilStore(store Store) bool {
	if store == nil {
		return true
	}
	v := reflect.ValueOf(store)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map:
		return v.IsNil()
	default:
		return false
	}
}
