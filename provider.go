package herdgate

import "github.com/prometheus/client_golang/prometheus"

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
func (p *Provider) Metrics() prometheus.Collector {
	return p.metrics
}

// New creates a [Herdgate] sharing the provider's metrics.
func (p *Provider) New() *Herdgate {
	return newHerdgate(p.metrics)
}
