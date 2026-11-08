// Bench runs N simulated instances of a service. The service reads from a
// slow upstream and caches the value in Redis. In each trial, every instance
// handles M concurrent requests for one uncached key, and Bench counts how
// many requests reach the upstream.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/giufi/herdgate"
	"github.com/giufi/herdgate/coordination/rediscoord"
	"github.com/giufi/herdgate/store/redisstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/giufi/herdgate/bench/internal/upstream"
)

const (
	_modeUngated      = "ungated"
	_modeSingleflight = "singleflight"
	_modeCoordination = "coordination"

	_upstreamLatencyMedian = 400 * time.Millisecond
	_upstreamLatencySigma  = 0.5
	_upstreamLatencyMax    = 3 * time.Second
	_upstreamMaxConcurrent = 4
	_upstreamSlotWait      = 100 * time.Millisecond
	_upstreamValue         = "value"

	_cacheTTL = 60 * time.Second

	_fetchMetricName = "herdgate_fetch_duration_seconds"

	_callerTimeout = 10 * time.Second
	_closeTimeout  = 5 * time.Second
)

type runOptions struct {
	mode               string
	trials             int
	instances          int
	callersPerInstance int
	upstreamSeed       uint64
	redisAddr          string
}

// instance represents one of the N simulated service instances.
type instance struct {
	client   *redis.Client
	store    *redisstore.Store
	hg       *herdgate.Herdgate
	registry *prometheus.Registry
}

type trialResult struct {
	requests      upstream.RequestCounts
	failedCallers int
}

func main() {
	var options runOptions
	flag.StringVar(&options.mode, "mode", _modeUngated, "ungated, singleflight or coordination")
	flag.IntVar(&options.trials, "trials", 20, "number of thundering herd trials, each with a new key")
	flag.IntVar(&options.instances, "instances", 20, "number of simulated instances")
	flag.IntVar(&options.callersPerInstance, "callers-per-instance", 100, "concurrent callers per instance")
	flag.Uint64Var(&options.upstreamSeed, "upstream-seed", 1, "seed for the simulated upstream latency")
	flag.StringVar(&options.redisAddr, "redis", "127.0.0.1:6379", "Redis address")
	flag.Parse()

	slog.SetDefault(slog.New(slog.DiscardHandler))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := run(ctx, options)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, options runOptions) (err error) {
	if options.trials < 1 || options.instances < 1 || options.callersPerInstance < 1 {
		return errors.New("-trials, -instances and -callers-per-instance must be at least 1")
	}
	switch options.mode {
	case _modeUngated, _modeSingleflight, _modeCoordination:
	default:
		return fmt.Errorf("unknown mode %q", options.mode)
	}

	up := upstream.New(upstream.Config{
		LatencyMedian: _upstreamLatencyMedian,
		LatencySigma:  _upstreamLatencySigma,
		LatencyMax:    _upstreamLatencyMax,
		MaxConcurrent: _upstreamMaxConcurrent,
		SlotWait:      _upstreamSlotWait,
		Seed:          options.upstreamSeed,
	})

	instances := newInstances(
		options.redisAddr, options.instances, options.callersPerInstance, options.mode)
	defer func() { err = errors.Join(err, closeInstances(ctx, instances)) }()
	if err := warmUp(ctx, instances, options.callersPerInstance); err != nil {
		return fmt.Errorf("redis at %s: %w", options.redisAddr, err)
	}

	// Each run uses new keys in order to start from an empty cache
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	var (
		total         upstream.RequestCounts
		maxRequests   int
		failedCallers int
	)
	for trial := range options.trials {
		key := fmt.Sprintf("key:%s:%d", runID, trial)
		result, err := runTrial(ctx, up, instances, options, key)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			return fmt.Errorf("%s, trial %d: %w", options.mode, trial, err)
		}
		total.Accepted += result.requests.Accepted
		total.Rejected += result.requests.Rejected
		maxRequests = max(maxRequests, result.requests.Total())
		failedCallers += result.failedCallers
	}

	callers := options.trials * options.instances * options.callersPerInstance
	fmt.Printf("%s: %d instances with %d callers, %d trials\n",
		options.mode, options.instances, options.callersPerInstance, options.trials)
	fmt.Printf("  upstream: %d requests (max %d per trial), %d accepted, %d rejected\n",
		total.Total(), maxRequests, total.Accepted, total.Rejected)
	fmt.Printf("  callers: %d of %d failed\n", failedCallers, callers)
	return nil
}

// newInstances creates the requested instances.
// Each Redis client has one connection per caller, so callers never wait
// for a free connection.
func newInstances(addr string, n, callersPerInstance int, mode string) []instance {
	instances := make([]instance, n)
	for i := range instances {
		client := redis.NewClient(&redis.Options{
			Addr:                  addr,
			ContextTimeoutEnabled: true,
			MaxRetries:            -1,
			DialerRetries:         1,
			PoolSize:              callersPerInstance,
		})
		inst := instance{client: client, store: redisstore.New(client)}
		if mode != _modeUngated {
			inst.registry = prometheus.NewRegistry()
			provider := herdgate.NewProvider(herdgate.WithRegisterer(inst.registry))
			var opts []herdgate.HerdgateOption
			if mode == _modeCoordination {
				opts = append(opts, herdgate.WithCoordination(rediscoord.New(client)))
			}
			inst.hg = provider.New(inst.store, opts...)
		}
		instances[i] = inst
	}
	return instances
}

// warmUp sends conns concurrent pings to each instance, one instance at a
// time, so every connection is already open before the first trial.
func warmUp(ctx context.Context, instances []instance, conns int) error {
	for _, inst := range instances {
		failed := runConcurrently(ctx, 1, conns, func(ctx context.Context, _ int) error {
			return inst.client.Ping(ctx).Err()
		})
		if failed > 0 {
			return fmt.Errorf("%d of %d warm-up pings failed", failed, conns)
		}
	}
	return nil
}

// closeInstances closes each instance's Herdgate and Redis client, waiting
// up to _closeTimeout.
func closeInstances(ctx context.Context, instances []instance) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), _closeTimeout)
	defer cancel()
	var errs []error
	for _, inst := range instances {
		if inst.hg != nil {
			errs = append(errs, inst.hg.Close(ctx))
		}
		errs = append(errs, inst.client.Close())
	}
	return errors.Join(errs...)
}

func runTrial(
	ctx context.Context, up *upstream.Simulator, instances []instance,
	options runOptions, key string,
) (trialResult, error) {
	if options.mode == _modeUngated {
		failedCallers := ungatedHerd(ctx, up, instances, options.callersPerInstance, key)
		result := trialResult{requests: up.RequestCounts(key), failedCallers: failedCallers}
		expected := options.instances * options.callersPerInstance
		if result.requests.Total() != expected {
			return result, fmt.Errorf("%d upstream requests, %d expected",
				result.requests.Total(), expected)
		}
		return result, nil
	}

	fetchesBefore, err := countFetches(instances)
	if err != nil {
		return trialResult{}, err
	}
	failedCallers := gatedHerd(ctx, up, instances, options.callersPerInstance, key)
	fetchesAfter, err := countFetches(instances)
	if err != nil {
		return trialResult{}, err
	}
	result := trialResult{requests: up.RequestCounts(key), failedCallers: failedCallers}
	total := result.requests.Total()
	if total < 1 {
		return result, errors.New("no upstream requests, at least one expected")
	}
	// coordination mode happy path: at most one request per instance
	if options.mode == _modeCoordination && total > options.instances {
		return result, fmt.Errorf("%d upstream requests, at most %d expected in a healthy trial",
			total, options.instances)
	}
	if fetches := fetchesAfter - fetchesBefore; fetches != total {
		return result, fmt.Errorf("upstream received %d requests but Herdgate counted %d fetches",
			total, fetches)
	}
	return result, nil
}

// ungatedHerd runs callersPerInstance callers per instance for the given key,
// each doing cache-aside on its store, without using Herdgate.
// It returns the number of callers that failed.
func ungatedHerd(
	ctx context.Context, up *upstream.Simulator, instances []instance,
	callersPerInstance int, key string,
) int {
	call := func(ctx context.Context, i int) error {
		store := instances[i].store
		if _, found, err := store.Get(ctx, key); err == nil && found {
			return nil
		}
		if err := up.Invoke(ctx, key); err != nil {
			return err
		}
		_ = store.Set(ctx, key, []byte(_upstreamValue), _cacheTTL)
		return nil
	}
	return runConcurrently(ctx, len(instances), callersPerInstance, call)
}

// gatedHerd runs callersPerInstance callers per instance for the given key,
// each using its Herdgate to retrieve the upstream value.
// It returns the number of callers that failed.
func gatedHerd(
	ctx context.Context, up *upstream.Simulator, instances []instance,
	callersPerInstance int, key string,
) int {
	fetch := func(ctx context.Context) ([]byte, error) {
		if err := up.Invoke(ctx, key); err != nil {
			return nil, err
		}
		return []byte(_upstreamValue), nil
	}
	call := func(ctx context.Context, i int) error {
		_, err := instances[i].hg.Do(ctx, key, herdgate.DoOptions{TTL: _cacheTTL}, fetch)
		return err
	}
	return runConcurrently(ctx, len(instances), callersPerInstance, call)
}

// countFetches returns the number of fetches recorded in the Herdgate
// metrics of the given service instances.
func countFetches(instances []instance) (int, error) {
	total := 0
	for _, inst := range instances {
		count, err := countRegistryFetches(inst.registry)
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

// countRegistryFetches returns the number of fetches recorded in the given
// registry.
func countRegistryFetches(registry *prometheus.Registry) (int, error) {
	families, err := registry.Gather()
	if err != nil {
		return 0, fmt.Errorf("gather metrics: %w", err)
	}
	for _, family := range families {
		if family.GetName() != _fetchMetricName {
			continue
		}
		var count uint64
		for _, metric := range family.GetMetric() {
			count += metric.GetHistogram().GetSampleCount()
		}
		return int(count), nil
	}
	return 0, nil
}

// runConcurrently runs perInstance calls for each of the n instances, each in
// its own goroutine with its own timeout, starting them all at once.
// It returns the number of calls that failed.
func runConcurrently(
	ctx context.Context, n, perInstance int, call func(ctx context.Context, i int) error,
) int {
	start := make(chan struct{})
	var (
		wg     sync.WaitGroup
		failed atomic.Int64
	)
	for i := range n {
		for range perInstance {
			wg.Go(func() {
				<-start
				callerCtx, cancel := context.WithTimeout(ctx, _callerTimeout)
				defer cancel()
				if call(callerCtx, i) != nil {
					failed.Add(1)
				}
			})
		}
	}
	close(start)
	wg.Wait()
	return int(failed.Load())
}
