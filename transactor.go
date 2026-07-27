package ember

import "context"

// Transactor
type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
	// InTx reports whether ctx already carries an active transaction.
	InTx(ctx context.Context) bool
}

// NopTransactor runs fn inline and always reports no ambient transaction. Use
// it with a backend that has no multi-document transactions, where a Save is
// already atomic per entity or the caller accepts partial writes.
var NopTransactor Transactor = nopTransactor{}

type nopTransactor struct{}

func (nopTransactor) InTx(context.Context) bool { return false }

func (nopTransactor) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}
