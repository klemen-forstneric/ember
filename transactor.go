package ember

import "context"

// Transactor
type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
	// InTx reports whether ctx already carries an active transaction.
	InTx(ctx context.Context) bool
}
