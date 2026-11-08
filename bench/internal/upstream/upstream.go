// Package upstream provides a simulated upstream with configurable latency
// and concurrency limit.
package upstream

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// ErrConcurrencyLimit is returned when no slot frees up within [Config.SlotWait].
var ErrConcurrencyLimit = errors.New("upstream: concurrency limit reached")

// Config holds the [Simulator] settings.
type Config struct {
	LatencyMedian time.Duration
	LatencySigma  float64 // lognormal sigma, 0 for a constant latency
	LatencyMax    time.Duration
	MaxConcurrent int
	SlotWait      time.Duration // max wait for a free slot
	Seed          uint64
}

// Simulator is a simulated upstream. It is safe for concurrent use.
type Simulator struct {
	config Config
	slots  chan struct{}

	guarded struct {
		mu     sync.Mutex
		rand   *rand.Rand
		counts map[string]RequestCounts
	}
}

// RequestCounts holds the number of requests made for one key, by outcome.
type RequestCounts struct {
	Accepted int
	Rejected int // by the concurrency limit
}

// New returns a [Simulator] with the given config.
func New(config Config) *Simulator {
	s := &Simulator{
		config: config,
		slots:  make(chan struct{}, config.MaxConcurrent),
	}
	s.guarded.rand = rand.New(rand.NewPCG(config.Seed, config.Seed))
	s.guarded.counts = make(map[string]RequestCounts)
	return s
}

// Invoke handles a request for the given key.
// A request counts as accepted once it gets a slot, even if ctx ends first.
func (s *Simulator) Invoke(ctx context.Context, key string) error {
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-time.After(s.config.SlotWait):
		s.recordRejected(key)
		return ErrConcurrencyLimit
	}

	s.recordAccepted(key)
	select {
	case <-time.After(s.sampleLatency()):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RequestCounts returns the [RequestCounts] for the given key.
func (s *Simulator) RequestCounts(key string) RequestCounts {
	s.guarded.mu.Lock()
	defer s.guarded.mu.Unlock()
	return s.guarded.counts[key]
}

func (s *Simulator) recordAccepted(key string) {
	s.guarded.mu.Lock()
	defer s.guarded.mu.Unlock()
	c := s.guarded.counts[key]
	c.Accepted++
	s.guarded.counts[key] = c
}

func (s *Simulator) recordRejected(key string) {
	s.guarded.mu.Lock()
	defer s.guarded.mu.Unlock()
	c := s.guarded.counts[key]
	c.Rejected++
	s.guarded.counts[key] = c
}

func (s *Simulator) sampleLatency() time.Duration {
	s.guarded.mu.Lock()
	z := s.guarded.rand.NormFloat64() // standard normal deviate, z ~ N(0, 1)
	s.guarded.mu.Unlock()

	// Lognormal latency
	latency := time.Duration(float64(s.config.LatencyMedian) * math.Exp(s.config.LatencySigma*z))

	return min(latency, s.config.LatencyMax)
}

// Total returns the total number of requests across all outcomes.
func (c RequestCounts) Total() int { return c.Accepted + c.Rejected }
