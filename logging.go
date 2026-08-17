package herdgate

import (
	"log/slog"
	"time"
)

var _loggerAttr = slog.String("logger", "herdgate")

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
