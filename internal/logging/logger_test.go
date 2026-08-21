package logging

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

const _loggerName = "testLoggerName"

func TestLogger_NewLoggerAttribute(t *testing.T) {
	for name, fn := range logFuncs() {
		t.Run(name, func(t *testing.T) {
			var buffer bytes.Buffer
			handler := slog.NewTextHandler(&buffer, &slog.HandlerOptions{Level: slog.LevelDebug})
			setDefaultLogger(t, slog.New(handler))

			fn(context.Background(), "test message")

			assert.Contains(t, buffer.String(), `logger=testLoggerName`)
		})
	}
}

func TestLogger_LogCallerSource(t *testing.T) {
	for name, fn := range logFuncs() {
		t.Run(name, func(t *testing.T) {
			var buffer bytes.Buffer
			handler := slog.NewTextHandler(&buffer, &slog.HandlerOptions{AddSource: true, Level: slog.LevelDebug})
			setDefaultLogger(t, slog.New(handler))

			_, _, callerLine, _ := runtime.Caller(0)
			fn(context.Background(), "test message")

			assert.Contains(t, buffer.String(), fmt.Sprintf("logger_test.go:%d", callerLine+1))
		})
	}
}

func TestLogger_LogHandlerPanic(t *testing.T) {
	for name, fn := range logFuncs() {
		t.Run(name, func(t *testing.T) {
			baseHandler := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
			setDefaultLogger(t, slog.New(panicHandler{baseHandler}))

			assert.NotPanics(t, func() {
				fn(context.Background(), "test message")
			})
		})
	}
}

func TestLogger_LogLevelNotEnabled(t *testing.T) {
	const minimumLevel = slog.Level(math.MaxInt)

	for name, fn := range logFuncs() {
		t.Run(name, func(t *testing.T) {
			var buffer bytes.Buffer
			handler := slog.NewTextHandler(&buffer, &slog.HandlerOptions{Level: minimumLevel})
			setDefaultLogger(t, slog.New(handler))

			fn(context.Background(), "test message")

			assert.Empty(t, buffer.String(), "no output should be written when the level is not enabled")
		})
	}
}

type logFunc func(ctx context.Context, msg string, attrs ...slog.Attr)

func logFuncs() map[string]logFunc {
	logger := New(_loggerName)
	return map[string]logFunc{"Debug": logger.Debug, "Error": logger.Error}
}

// panicHandler wraps a [slog.Handler] and panics on [slog.Handler.Handle].
type panicHandler struct{ slog.Handler }

func (panicHandler) Handle(context.Context, slog.Record) error {
	panic(assert.AnError)
}

func setDefaultLogger(t *testing.T, logger *slog.Logger) {
	t.Helper()
	previous := slog.Default()
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(previous) })
}
