// Package logging provides a [Logger] that shields callers from any panic or error in the [slog.Handler].
package logging

import (
	"context"
	"log/slog"
	"runtime"
	"time"
)

// Logger logs messages including a "logger" [slog.Attr], whose value is set by [New].
//
// Use [New] to create a new Logger. The zero value [Logger] leaves the attribute value unset and thus it should be avoided.
type Logger struct {
	nameAttr slog.Attr
}

// New returns a [Logger] with name as the "logger" [slog.Attr] value.
func New(name string) Logger {
	return Logger{nameAttr: slog.String("logger", name)}
}

// Error logs the message at [slog.LevelError], discarding any panic or error from the [slog.Handler].
func (l Logger) Error(ctx context.Context, msg string, attrs ...slog.Attr) {
	log(ctx, slog.LevelError, msg, append([]slog.Attr{l.nameAttr}, attrs...)...)
}

// Debug logs the message at [slog.LevelDebug], discarding any panic or error from the [slog.Handler].
func (l Logger) Debug(ctx context.Context, msg string, attrs ...slog.Attr) {
	log(ctx, slog.LevelDebug, msg, append([]slog.Attr{l.nameAttr}, attrs...)...)
}

func log(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	defer func() { _ = recover() }()

	logger := slog.Default()
	if !logger.Enabled(ctx, level) {
		return
	}
	var pc [1]uintptr
	runtime.Callers(3, pc[:])
	record := slog.NewRecord(time.Now(), level, msg, pc[0])
	record.AddAttrs(attrs...)
	_ = logger.Handler().Handle(ctx, record)
}
