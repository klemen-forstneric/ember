package ember

import "context"

// Transactor runs fn inside a transaction, passing a ctx bound to it. Reentrant
// implementations join a transaction already present on the ctx rather than
// nesting a new one.
type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}
