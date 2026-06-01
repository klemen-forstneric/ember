package ember

import "context"

// LoggerCtx is the context-aware structured logger used across spark.
// Implementations adapt arbitrary backends (slog, zap, zerolog, etc.) by
// satisfying this interface.
//
// kvs is a flat sequence of alternating keys and values. Keys must be
// strings; values may be any type the underlying backend can render.
// Mismatched pairs are the implementation's problem to surface.
//
// Error takes the error as a separate positional argument so backends can
// render stack traces or attach it as a typed field rather than a string.
type LoggerCtx interface {
	Debug(ctx context.Context, msg string, kvs ...interface{})
	Info(ctx context.Context, msg string, kvs ...interface{})
	Warn(ctx context.Context, msg string, kvs ...interface{})
	Error(ctx context.Context, msg string, err error, kvs ...interface{})
}

// NopLogger is a LoggerCtx that drops every call. Used as the zero-value
// default by middleware that takes an optional logger.
var NopLogger LoggerCtx = nopLogger{}

type nopLogger struct{}

func (nopLogger) Debug(context.Context, string, ...interface{})        {}
func (nopLogger) Info(context.Context, string, ...interface{})         {}
func (nopLogger) Warn(context.Context, string, ...interface{})         {}
func (nopLogger) Error(context.Context, string, error, ...interface{}) {}
