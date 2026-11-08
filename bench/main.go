// Bench simulates repeated thundering herds.
// In each trial, N simulated instances with M callers each request
// the same new key at once, and Bench counts how many of those requests
// [upstream.Simulator] accepts and how many it rejects.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/giufi/herdgate/bench/internal/upstream"
)

const (
	_upstreamLatencyMedian = 400 * time.Millisecond
	_upstreamLatencySigma  = 0.5
	_upstreamLatencyMax    = 3 * time.Second
	_upstreamMaxConcurrent = 4
	_upstreamSlotWait      = 100 * time.Millisecond
)

type runOptions struct {
	trials             int
	instances          int
	callersPerInstance int
	upstreamSeed       uint64
}

func main() {
	var options runOptions
	flag.IntVar(&options.trials, "trials", 20, "number of thundering herd trials, each with a new key")
	flag.IntVar(&options.instances, "instances", 20, "number of simulated instances")
	flag.IntVar(&options.callersPerInstance, "callers-per-instance", 100, "concurrent callers per instance")
	flag.Uint64Var(&options.upstreamSeed, "upstream-seed", 1, "seed for the simulated upstream latency")
	flag.Parse()

	if err := run(options); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(options runOptions) error {
	if options.trials < 1 || options.instances < 1 || options.callersPerInstance < 1 {
		return errors.New("-trials, -instances and -callers-per-instance must be at least 1")
	}

	up := upstream.New(upstream.Config{
		LatencyMedian: _upstreamLatencyMedian,
		LatencySigma:  _upstreamLatencySigma,
		LatencyMax:    _upstreamLatencyMax,
		MaxConcurrent: _upstreamMaxConcurrent,
		SlotWait:      _upstreamSlotWait,
		Seed:          options.upstreamSeed,
	})
	callers := options.instances * options.callersPerInstance

	var total upstream.RequestCounts
	for trial := range options.trials {
		key := fmt.Sprintf("quote:%d", trial)
		ungatedHerd(up, callers, key)
		got := up.RequestCounts(key)
		if got.Total() != callers {
			return fmt.Errorf("ungated, trial %d: %d upstream requests, want %d", trial, got.Total(), callers)
		}
		total.Accepted += got.Accepted
		total.Rejected += got.Rejected
	}
	fmt.Printf("ungated: %d instances x %d callers, %d trials, %d requests, %d accepted, %d rejected\n",
		options.instances, options.callersPerInstance, options.trials, total.Total(), total.Accepted, total.Rejected)
	return nil
}

// ungatedHerd sends concurrent upstream requests without using Herdgate to prevent
// a thundering herd.
func ungatedHerd(up *upstream.Simulator, callers int, key string) {
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			<-start
			_ = up.Invoke(context.Background(), key)
		})
	}
	close(start)
	wg.Wait()
}
