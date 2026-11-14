package upstream_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/giufi/herdgate/bench/internal/upstream"
)

const (
	_key           = "key"
	_latency       = 400 * time.Millisecond
	_maxConcurrent = 4
	_slotWait      = 100 * time.Millisecond
)

func TestSimulator_InvokeOutcomes(t *testing.T) {
	tests := []struct {
		name        string
		requests    int
		ctxTimeout  time.Duration // if 0, no timeout
		wantCounts  upstream.RequestCounts
		wantResults map[error]int
	}{
		{
			name:        "under the limit",
			requests:    _maxConcurrent - 1,
			wantCounts:  upstream.RequestCounts{Accepted: _maxConcurrent - 1},
			wantResults: map[error]int{nil: _maxConcurrent - 1},
		},
		{
			name:        "over the limit",
			requests:    _maxConcurrent + 6,
			wantCounts:  upstream.RequestCounts{Accepted: _maxConcurrent, Rejected: 6},
			wantResults: map[error]int{nil: _maxConcurrent, upstream.ErrConcurrencyLimit: 6},
		},
		{
			name:        "ctx deadline exceeded",
			requests:    1,
			ctxTimeout:  _latency / 2,
			wantCounts:  upstream.RequestCounts{Accepted: 1},
			wantResults: map[error]int{context.DeadlineExceeded: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sim := upstream.New(upstream.Config{
					LatencyMedian: _latency,
					LatencySigma:  0,
					LatencyMax:    _latency,
					MaxConcurrent: _maxConcurrent,
					SlotWait:      _slotWait,
				})

				ctx := t.Context()
				if tt.ctxTimeout > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tt.ctxTimeout)
					defer cancel()
				}

				var (
					mu      sync.Mutex
					results = map[error]int{}
					wg      sync.WaitGroup
				)
				for range tt.requests {
					wg.Go(func() {
						err := sim.Invoke(ctx, _key)
						mu.Lock()
						defer mu.Unlock()
						results[err]++
					})
				}
				wg.Wait()

				assert.Equal(t, tt.wantCounts, sim.RequestCounts(_key))
				assert.Equal(t, tt.wantResults, results)
			})
		})
	}
}

func TestSimulator_InvokeCtxDoneDuringSlotWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sim := upstream.New(upstream.Config{
			LatencyMedian: _latency,
			LatencySigma:  0,
			LatencyMax:    _latency,
			MaxConcurrent: _maxConcurrent,
			SlotWait:      _slotWait,
		})

		var wg sync.WaitGroup
		for range _maxConcurrent {
			wg.Go(func() { _ = sim.Invoke(t.Context(), _key) })
		}
		synctest.Wait()

		ctx, cancel := context.WithTimeout(t.Context(), _slotWait/2)
		defer cancel()
		err := sim.Invoke(ctx, _key)
		wg.Wait()

		assert.Equal(t, upstream.RequestCounts{Accepted: _maxConcurrent}, sim.RequestCounts(_key))
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}
