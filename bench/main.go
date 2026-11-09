// Bench runs N simulated instances of a service. The service reads from a
// slow upstream and caches the value in Redis. In each trial, every instance
// handles M concurrent requests for one uncached key, and Bench counts how
// many requests reach the upstream.
package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
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

	_defaultTrials                   = 20
	_defaultInstances                = 20
	_defaultCallersPerInstance       = 100
	_defaultUpstreamSeed             = 1
	_defaultUpstreamConcurrencyLimit = 10
	_defaultRedisAddr                = "127.0.0.1:6379"

	_upstreamLatencyMedian = 400 * time.Millisecond
	_upstreamLatencySigma  = 0.5
	_upstreamLatencyMax    = 3 * time.Second
	_upstreamSlotWait      = 100 * time.Millisecond
	_upstreamValue         = "value"

	_cacheTTL = 60 * time.Second

	_fetchMetricName           = "herdgate_fetch_duration_seconds"
	_storeOperationsMetricName = "herdgate_store_operations_total"

	_callerTimeout = 10 * time.Second
	_closeTimeout  = 5 * time.Second
)

type runOptions struct {
	mode                     string
	trials                   int
	instances                int
	callersPerInstance       int
	upstreamSeed             uint64
	upstreamConcurrencyLimit int
	redisAddr                string
}

func (o *runOptions) validate() error {
	for _, f := range []struct {
		name  string
		value int
	}{
		{"trials", o.trials},
		{"instances", o.instances},
		{"callers-per-instance", o.callersPerInstance},
		{"upstream-concurrency-limit", o.upstreamConcurrencyLimit},
	} {
		if f.value < 1 {
			return fmt.Errorf("-%s must be at least 1, got %d", f.name, f.value)
		}
	}
	switch o.mode {
	case _modeUngated, _modeSingleflight, _modeCoordination:
		return nil
	default:
		return fmt.Errorf("unknown mode %q", o.mode)
	}
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
	storeHits     int
}

// metricsSnapshot holds the fetch and store hit counts from Herdgate's metrics.
type metricsSnapshot struct {
	fetches   int
	storeHits int
}

func main() {
	var (
		options runOptions
		csvPath string
	)
	flag.StringVar(&options.mode, "mode", _modeUngated, "ungated, singleflight or coordination")
	flag.IntVar(&options.trials, "trials", _defaultTrials,
		"number of thundering herd trials, each with a new key")
	flag.IntVar(&options.instances, "instances", _defaultInstances,
		"number of simulated service instances")
	flag.IntVar(&options.callersPerInstance, "callers-per-instance", _defaultCallersPerInstance,
		"concurrent callers per service instance")
	flag.Uint64Var(&options.upstreamSeed, "upstream-seed", _defaultUpstreamSeed,
		"seed for the simulated upstream latency")
	flag.IntVar(&options.upstreamConcurrencyLimit, "upstream-concurrency-limit",
		_defaultUpstreamConcurrencyLimit,
		"maximum number of requests the simulated upstream handles at once")
	flag.StringVar(&options.redisAddr, "redis", _defaultRedisAddr, "Redis address")
	flag.StringVar(&csvPath, "csv", "", "write one row per trial to the given file")
	flag.Parse()

	slog.SetDefault(slog.New(slog.DiscardHandler))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := run(ctx, &options, csvPath)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, options *runOptions, csvPath string) (err error) {
	if err := options.validate(); err != nil {
		return err
	}

	up := upstream.New(upstream.Config{
		LatencyMedian: _upstreamLatencyMedian,
		LatencySigma:  _upstreamLatencySigma,
		LatencyMax:    _upstreamLatencyMax,
		MaxConcurrent: options.upstreamConcurrencyLimit,
		SlotWait:      _upstreamSlotWait,
		Seed:          options.upstreamSeed,
	})

	var rows *csv.Writer
	if csvPath != "" {
		file, createErr := os.Create(csvPath)
		if createErr != nil {
			return createErr
		}
		defer func() { err = errors.Join(err, file.Close()) }()
		rows = csv.NewWriter(file)
		if csvErr := writeRow(rows, trialHeader()); csvErr != nil {
			return fmt.Errorf("write csv: %w", csvErr)
		}
	}

	instances := newInstances(
		options.redisAddr, options.instances, options.callersPerInstance, options.mode)
	defer func() { err = errors.Join(err, closeInstances(ctx, instances)) }()
	if err := warmUp(ctx, instances, options.callersPerInstance); err != nil {
		return fmt.Errorf("redis at %s: %w", options.redisAddr, err)
	}

	// Each run uses new keys in order to start from an empty cache
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	results := make([]trialResult, 0, options.trials)
	for trial := range options.trials {
		key := fmt.Sprintf("key:%s:%d", runID, trial)
		result, err := runTrial(ctx, up, instances, options, key)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if rows != nil {
			row := trialRow(options.mode, trial, result, err)
			if csvErr := writeRow(rows, row); csvErr != nil {
				return fmt.Errorf("write csv: %w", csvErr)
			}
		}
		if err != nil {
			return fmt.Errorf("%s, trial %d: %w", options.mode, trial, err)
		}
		results = append(results, result)
	}

	printSummary(options, results)
	return nil
}

// writeRow flushes after each row, in order to prevent buffered rows from
// being lost.
func writeRow(w *csv.Writer, record []string) error {
	if err := w.Write(record); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

func trialHeader() []string {
	return []string{
		"mode", "trial", "accepted", "rejected", "store_hits", "failed_callers", "error",
	}
}

func trialRow(mode string, trial int, result trialResult, err error) []string {
	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}
	return []string{
		mode,
		strconv.Itoa(trial),
		strconv.Itoa(result.requests.Accepted),
		strconv.Itoa(result.requests.Rejected),
		strconv.Itoa(result.storeHits),
		strconv.Itoa(result.failedCallers),
		errMsg,
	}
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
	options *runOptions, key string,
) (trialResult, error) {
	if options.mode == _modeUngated {
		failedCallers, storeHits := ungatedHerd(ctx, up, instances, options.callersPerInstance, key)
		result := trialResult{
			requests:      up.RequestCounts(key),
			failedCallers: failedCallers,
			storeHits:     storeHits,
		}
		callers := options.instances * options.callersPerInstance
		if total := result.requests.Total(); total+storeHits != callers {
			return result, fmt.Errorf(
				"%d callers accounted for (%d upstream requests, %d store hits), %d expected",
				total+storeHits, total, storeHits, callers)
		}
		return result, nil
	}

	snapshotBefore, err := snapshotMetrics(instances)
	if err != nil {
		return trialResult{}, err
	}
	failedCallers := gatedHerd(ctx, up, instances, options.callersPerInstance, key)
	snapshotAfter, err := snapshotMetrics(instances)
	if err != nil {
		return trialResult{}, err
	}
	result := trialResult{
		requests:      up.RequestCounts(key),
		failedCallers: failedCallers,
		storeHits:     snapshotAfter.storeHits - snapshotBefore.storeHits,
	}
	total := result.requests.Total()
	if total < 1 {
		return result, errors.New("no upstream requests, at least one expected")
	}
	// Coordination should never make more upstream requests than singleflight's
	// happy path, i.e., one per instance
	if options.mode == _modeCoordination && total > options.instances {
		return result, fmt.Errorf("%d upstream requests, at most %d expected in a healthy trial",
			total, options.instances)
	}
	if fetches := snapshotAfter.fetches - snapshotBefore.fetches; fetches != total {
		return result, fmt.Errorf("upstream received %d requests but Herdgate counted %d fetches",
			total, fetches)
	}
	return result, nil
}

// ungatedHerd runs callersPerInstance callers per instance for the given key,
// each doing cache-aside on its store, without using Herdgate.
// It returns the number of callers that failed and the number that found the
// value in the store.
func ungatedHerd(
	ctx context.Context, up *upstream.Simulator, instances []instance,
	callersPerInstance int, key string,
) (failedCallers, storeHits int) {
	var hitCount atomic.Int64
	call := func(ctx context.Context, i int) error {
		store := instances[i].store
		if _, found, err := store.Get(ctx, key); err == nil && found {
			hitCount.Add(1)
			return nil
		}
		if err := up.Invoke(ctx, key); err != nil {
			return err
		}
		_ = store.Set(ctx, key, []byte(_upstreamValue), _cacheTTL)
		return nil
	}
	failedCallers = runConcurrently(ctx, len(instances), callersPerInstance, call)
	return failedCallers, int(hitCount.Load())
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

// snapshotMetrics returns a snapshot of the fetch and store hit metrics
// recorded by the Herdgate of each given service instance.
func snapshotMetrics(instances []instance) (metricsSnapshot, error) {
	var sum metricsSnapshot
	for _, inst := range instances {
		snapshot, err := snapshotRegistry(inst.registry)
		if err != nil {
			return metricsSnapshot{}, err
		}
		sum.fetches += snapshot.fetches
		sum.storeHits += snapshot.storeHits
	}
	return sum, nil
}

// snapshotRegistry returns a snapshot of the fetch and store hit metrics
// recorded in the given registry.
func snapshotRegistry(registry *prometheus.Registry) (metricsSnapshot, error) {
	families, err := registry.Gather()
	if err != nil {
		return metricsSnapshot{}, fmt.Errorf("gather metrics: %w", err)
	}
	var snapshot metricsSnapshot
	for _, family := range families {
		switch family.GetName() {
		case _fetchMetricName:
			for _, metric := range family.GetMetric() {
				snapshot.fetches += int(metric.GetHistogram().GetSampleCount())
			}
		case _storeOperationsMetricName:
			for _, metric := range family.GetMetric() {
				labels := make(map[string]string)
				for _, label := range metric.GetLabel() {
					labels[label.GetName()] = label.GetValue()
				}
				if labels["operation"] == "get" && labels["result"] == "hit" {
					snapshot.storeHits += int(metric.GetCounter().GetValue())
				}
			}
		}
	}
	return snapshot, nil
}

// printSummary prints the run's settings and a Markdown table of its results.
func printSummary(options *runOptions, results []trialResult) {
	requestsPerTrial := make([]int, len(results))
	var (
		oneRequest, twoToNRequests, overNRequests int
		duplicateRequests, rejectedRequests       int
		storeHits, failedCallers                  int
	)
	for i, result := range results {
		n := result.requests.Total()
		requestsPerTrial[i] = n
		switch {
		case n == 1:
			oneRequest++
		case n <= options.instances:
			twoToNRequests++
		default:
			overNRequests++
		}
		duplicateRequests += n - 1
		rejectedRequests += result.requests.Rejected
		storeHits += result.storeHits
		failedCallers += result.failedCallers
	}
	slices.Sort(requestsPerTrial)
	callers := len(results) * options.instances * options.callersPerInstance

	fmt.Printf("%d instances, %d callers each, upstream latency seed %d\n",
		options.instances, options.callersPerInstance, options.upstreamSeed)
	fmt.Printf("upstream: latency median %v, sigma %v, max %v, "+
		"concurrency limit %d, %v slot wait\n\n",
		_upstreamLatencyMedian, _upstreamLatencySigma, _upstreamLatencyMax,
		options.upstreamConcurrencyLimit, _upstreamSlotWait)
	fmt.Printf("| mode | trials | median requests per trial | max requests per trial "+
		"| duplicate requests (total) | trials with 1 / 2 to %d / over %d requests "+
		"| rejected requests | store hits | failed callers |\n",
		options.instances, options.instances)
	fmt.Println("|---|---|---|---|---|---|---|---|---|")
	fmt.Printf("| %s | %d | %g | %d | %d | %d / %d / %d | %d | %d | %d of %d |\n",
		options.mode, len(results), median(requestsPerTrial),
		requestsPerTrial[len(requestsPerTrial)-1],
		duplicateRequests, oneRequest, twoToNRequests, overNRequests,
		rejectedRequests, storeHits, failedCallers, callers)
}

// median returns the median of sorted, which must not be empty.
func median(sorted []int) float64 {
	n := len(sorted)
	if n%2 == 1 {
		return float64(sorted[n/2])
	}
	return float64(sorted[n/2-1]+sorted[n/2]) / 2
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
