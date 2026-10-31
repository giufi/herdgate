package herdgate

import (
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

// errFromPanic returns the stack trace as a log attribute and an error
// built from the recovered panic value.
//
// It must be called inside the deferred recover, otherwise the stack is
// not accurate.
func errFromPanic(panicVal any) (stackAttr slog.Attr, err error) {
	stackAttr = slog.String("stack", string(debug.Stack()))
	err, ok := panicVal.(error)
	if !ok {
		err = errors.New(fmt.Sprint(panicVal))
	}
	return stackAttr, err
}
