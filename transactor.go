package ember

import "context"

// Transactor
type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}
