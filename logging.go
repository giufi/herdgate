package herdgate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/giufi/herdgate/internal/logging"
)

var _logger = logging.New("herdgate")

// ttlAttr returns the "ttl" attribute for a log record.
//
// A ttl of 0 or less is logged as "never", per [Store.Set]. Other values use
// [time.Duration.String] to stay readable under [slog.JSONHandler].
func ttlAttr(ttl time.Duration) slog.Attr {
	if ttl <= 0 {
		return slog.String("ttl", "never")
	}
	return slog.String("ttl", ttl.String())
}

// errFromPanic converts a recovered panic value to an error. It also logs
// the stack trace, since panic values don't carry one.
//
// It must be called inside the deferred recover, otherwise the stack is
// not accurate.
func errFromPanic(ctx context.Context, panicVal any) error {
	_logger.Error(ctx, "Panic recovered", slog.String("stack", string(debug.Stack())))

	if err, ok := panicVal.(error); ok {
		return err
	}
	return errors.New(fmt.Sprint(panicVal))
}
