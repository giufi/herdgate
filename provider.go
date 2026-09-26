package herdgate

import (
	"github.com/giufi/herdgate/internal/validation"
	"github.com/prometheus/client_golang/prometheus"
)

// A Provider creates new [Herdgate] instances, sharing metrics among them.
//
// Register the metrics with a [prometheus.Registerer]. See [NewProvider] and
// [Provider.Metrics] for further details.
//
// Use one Provider per [prometheus.Registerer]. Registering two Providers on
// the same Registerer fails: their metric names are identical.
type Provider struct {
	metrics *metrics
}

// NewProvider creates a [Provider], configurable with functional options.
//
// Options are applied in order to the new [Provider] before it is returned,
// and may panic on invalid input.
//
// Register its metrics with a [prometheus.Registerer]: automatically at
// construction by passing [WithRegisterer], or manually afterwards via
// [Provider.Metrics].
//
// Use one Provider per [prometheus.Registerer]: registering two Providers on
// the same Registerer fails, since their metric names are identical.
func NewProvider(opts ...ProviderOption) *Provider {
	p := &Provider{metrics: newMetrics()}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Metrics returns a [prometheus.Collector] holding the provider's metrics.
// Every [Herdgate] the provider creates shares the returned collector.
//
// Register the returned collector with a [prometheus.Registerer], unless it
// was already registered at construction (see [NewProvider]). Registering it
// again with the same Registerer fails.
//
// It panics if p was not created with [NewProvider].
func (p *Provider) Metrics() prometheus.Collector {
	p.checkMetrics()
	return p.metrics
}

// New creates a [Herdgate] backed by the given [Store] and sharing the
// provider's metrics.
//
// It is configurable with functional options, applied in order before the
// [Herdgate] is returned. An option may panic on invalid input.
//
// Call [Herdgate.Close] on the returned value when it is no longer needed.
//
// It panics if the given store is nil, or if p was not created with
// [NewProvider].
func (p *Provider) New(store Store, opts ...HerdgateOption) *Herdgate {
	if validation.IsNil(store) {
		panic("herdgate: store must not be nil")
	}
	p.checkMetrics()
	hg := newHerdgate(store, p.metrics)
	for _, opt := range opts {
		opt(hg)
	}
	return hg
}

func (p *Provider) checkMetrics() {
	if p.metrics == nil {
		panic("herdgate: Provider must be created with NewProvider")
	}
}
