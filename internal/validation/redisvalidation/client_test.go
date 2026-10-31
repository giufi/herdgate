package redisvalidation

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const (
	_wantContextTimeoutEnabledErr   = "client must set ContextTimeoutEnabled"
	_wantSocketDeadlinesDisabledErr = "client must not disable socket deadlines via ReadTimeout or WriteTimeout"

	_noTimeout         = time.Duration(-1)
	_noSocketDeadlines = time.Duration(-2)
)

var _timeouts = []time.Duration{0, 10 * time.Second, _noTimeout, _noSocketDeadlines, -time.Second}

func TestCheckContextDeadline_KnownClientTypes(t *testing.T) {
	newClients := map[string]func(ctxTimeoutEnabled bool, readTimeout, writeTimeout time.Duration) redis.UniversalClient{
		"client": func(ctxTimeoutEnabled bool, readTimeout, writeTimeout time.Duration) redis.UniversalClient {
			return redis.NewClient(&redis.Options{ContextTimeoutEnabled: ctxTimeoutEnabled, ReadTimeout: readTimeout, WriteTimeout: writeTimeout})
		},
		"cluster": func(ctxTimeoutEnabled bool, readTimeout, writeTimeout time.Duration) redis.UniversalClient {
			return redis.NewClusterClient(&redis.ClusterOptions{ContextTimeoutEnabled: ctxTimeoutEnabled, ReadTimeout: readTimeout, WriteTimeout: writeTimeout})
		},
		"ring": func(ctxTimeoutEnabled bool, readTimeout, writeTimeout time.Duration) redis.UniversalClient {
			return redis.NewRing(&redis.RingOptions{ContextTimeoutEnabled: ctxTimeoutEnabled, ReadTimeout: readTimeout, WriteTimeout: writeTimeout})
		},
		"failover": func(ctxTimeoutEnabled bool, readTimeout, writeTimeout time.Duration) redis.UniversalClient {
			return redis.NewFailoverClient(&redis.FailoverOptions{ContextTimeoutEnabled: ctxTimeoutEnabled, ReadTimeout: readTimeout, WriteTimeout: writeTimeout})
		},
		"failover cluster": func(ctxTimeoutEnabled bool, readTimeout, writeTimeout time.Duration) redis.UniversalClient {
			return redis.NewFailoverClusterClient(&redis.FailoverOptions{ContextTimeoutEnabled: ctxTimeoutEnabled, ReadTimeout: readTimeout, WriteTimeout: writeTimeout})
		},
	}

	for clientName, newClient := range newClients {
		for _, ctxTimeoutEnabled := range []bool{false, true} {
			for _, readTimeout := range _timeouts {
				for _, writeTimeout := range _timeouts {
					name := fmt.Sprintf("%s/ctxTimeoutEnabled=%t/readTimeout=%s/writeTimeout=%s", clientName, ctxTimeoutEnabled, readTimeout, writeTimeout)
					t.Run(name, func(t *testing.T) {
						client := newClient(ctxTimeoutEnabled, readTimeout, writeTimeout)
						t.Cleanup(func() { _ = client.Close() })

						err := CheckContextDeadline(client)
						socketDeadlineDisabled := readTimeout < _noTimeout || writeTimeout < _noTimeout
						switch {
						case !ctxTimeoutEnabled:
							require.EqualError(t, err, _wantContextTimeoutEnabledErr)
						case socketDeadlineDisabled:
							require.EqualError(t, err, _wantSocketDeadlinesDisabledErr)
						default:
							require.NoError(t, err)
						}
					})
				}
			}
		}
	}
}

func TestCheckContextDeadline_ClientWithTimeout(t *testing.T) {
	for _, ctxTimeoutEnabled := range []bool{false, true} {
		for _, timeout := range _timeouts {
			t.Run(fmt.Sprintf("ctxTimeoutEnabled=%t/timeout=%s", ctxTimeoutEnabled, timeout), func(t *testing.T) {
				client := redis.NewClient(&redis.Options{ContextTimeoutEnabled: ctxTimeoutEnabled})
				t.Cleanup(func() { _ = client.Close() })

				err := CheckContextDeadline(client.WithTimeout(timeout))
				socketDeadlineDisabled := timeout < 0
				switch {
				case !ctxTimeoutEnabled:
					require.EqualError(t, err, _wantContextTimeoutEnabledErr)
				case socketDeadlineDisabled:
					require.EqualError(t, err, _wantSocketDeadlinesDisabledErr)
				default:
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestCheckContextDeadline_UnknownClientType(t *testing.T) {
	client := redis.NewClient(&redis.Options{})
	t.Cleanup(func() { _ = client.Close() })

	require.NoError(t, CheckContextDeadline(struct{ redis.UniversalClient }{client}))
}

func TestCheckContextDeadline_DeadlineHonored(t *testing.T) {
	const ctxTimeout = 2 * time.Second // below go-redis default timeout (5s)

	redisServer := miniredis.RunT(t)
	addr := redisServer.Addr()

	newClients := map[string]func(recorder *deadlineRecorder, ctxTimeoutEnabled bool, readTimeout, writeTimeout time.Duration) redis.UniversalClient{
		"client": func(recorder *deadlineRecorder, ctxTimeoutEnabled bool, readTimeout, writeTimeout time.Duration) redis.UniversalClient {
			return redis.NewClient(&redis.Options{
				Addr: addr, Dialer: recorder.dial, MaxRetries: -1,
				ContextTimeoutEnabled: ctxTimeoutEnabled, ReadTimeout: readTimeout, WriteTimeout: writeTimeout,
			})
		},
		"cluster": func(recorder *deadlineRecorder, ctxTimeoutEnabled bool, readTimeout, writeTimeout time.Duration) redis.UniversalClient {
			return redis.NewClusterClient(&redis.ClusterOptions{
				Addrs: []string{addr}, Dialer: recorder.dial, MaxRetries: -1,
				ContextTimeoutEnabled: ctxTimeoutEnabled, ReadTimeout: readTimeout, WriteTimeout: writeTimeout,
			})
		},
		"ring": func(recorder *deadlineRecorder, ctxTimeoutEnabled bool, readTimeout, writeTimeout time.Duration) redis.UniversalClient {
			return redis.NewRing(&redis.RingOptions{
				Addrs: map[string]string{"shard": addr}, Dialer: recorder.dial, MaxRetries: -1, HeartbeatFrequency: time.Hour,
				ContextTimeoutEnabled: ctxTimeoutEnabled, ReadTimeout: readTimeout, WriteTimeout: writeTimeout,
			})
		},
	}

	for clientName, newClient := range newClients {
		for _, ctxTimeoutEnabled := range []bool{false, true} {
			for _, readTimeout := range _timeouts {
				for _, writeTimeout := range _timeouts {
					name := fmt.Sprintf("%s/ctxTimeoutEnabled=%t/readTimeout=%s/writeTimeout=%s", clientName, ctxTimeoutEnabled, readTimeout, writeTimeout)
					t.Run(name, func(t *testing.T) {
						recorder := &deadlineRecorder{}
						client := newClient(recorder, ctxTimeoutEnabled, readTimeout, writeTimeout)
						t.Cleanup(func() { _ = client.Close() })

						// Connection setup
						require.ErrorIs(t, client.Get(t.Context(), "key").Err(), redis.Nil)

						ctx, cancel := context.WithTimeout(t.Context(), ctxTimeout)
						defer cancel()
						ctxDeadline, _ := ctx.Deadline()

						require.ErrorIs(t, client.Get(ctx, "key").Err(), redis.Nil)
						require.Equal(t, CheckContextDeadline(client) == nil, recorder.sawOnReadAndWrite(ctxDeadline))
					})
				}
			}
		}
	}
}

// deadlineRecorder collects socket deadlines set on every connection it dials
type deadlineRecorder struct {
	mu    sync.Mutex
	read  []time.Time
	write []time.Time
}

func (r *deadlineRecorder) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return &recordingConn{Conn: conn, recorder: r}, nil
}

func (r *deadlineRecorder) sawOnReadAndWrite(deadline time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.ContainsFunc(r.read, deadline.Equal) && slices.ContainsFunc(r.write, deadline.Equal)
}

// recordingConn reports socket deadlines to the recorder, then applies them to the wrapped connection
type recordingConn struct {
	net.Conn
	recorder *deadlineRecorder
}

func (c *recordingConn) SetReadDeadline(deadline time.Time) error {
	c.recorder.mu.Lock()
	c.recorder.read = append(c.recorder.read, deadline)
	c.recorder.mu.Unlock()
	return c.Conn.SetReadDeadline(deadline)
}

func (c *recordingConn) SetWriteDeadline(deadline time.Time) error {
	c.recorder.mu.Lock()
	c.recorder.write = append(c.recorder.write, deadline)
	c.recorder.mu.Unlock()
	return c.Conn.SetWriteDeadline(deadline)
}
