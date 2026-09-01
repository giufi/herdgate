package herdgate

import (
	"fmt"

	"github.com/giufi/herdgate/internal/validation"
	"github.com/prometheus/client_golang/prometheus"
)

// ProviderOption is a functional option for [NewProvider], used to configure
// a [Provider] during construction.
type ProviderOption func(*Provider)

// WithRegisterer registers the [Provider]'s metrics with r when calling
// [NewProvider], instead of manually registering them later by calling
// [Provider.Metrics].
//
// It panics if r is nil, or if registration fails (e.g., r already has a
// collector registered under the same metric names).
func WithRegisterer(r prometheus.Registerer) ProviderOption {
	return func(p *Provider) {
		if validation.IsNil(r) {
			panic("herdgate: registerer must not be nil")
		}
		if err := r.Register(p.metrics); err != nil {
			panic(fmt.Errorf("herdgate: metrics registration: %w", err))
		}
	}
}

// HerdgateOption is a functional option for [Provider.New], used to
// configure a [Herdgate] during construction.
type HerdgateOption func(*Herdgate)

// WithCoordination sets c as the [Herdgate]'s [Coordination].
//
// It panics if c is nil.
func WithCoordination(c Coordination) HerdgateOption {
	return func(hg *Herdgate) {
		if validation.IsNil(c) {
			panic("herdgate: coordination must not be nil")
		}
		hg.coordination = c
	}
}
