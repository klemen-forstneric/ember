package ember

import "context"

// EntityStore
type EntityStore[E Entity] struct {
	loader *EntityLoader[E]
	saver  *EntitySaver
}

func NewEntityStore[E Entity](r EntityRepository, m EntityMarshaler[E], p *Publisher, tx Transactor, l LoggerCtx) *EntityStore[E] {
	b := Bind[E](r, m)

	return &EntityStore[E]{
		loader: NewEntityLoader(b),
		saver:  NewEntitySaver(p, tx, l, b),
	}
}

func (s *EntityStore[E]) Get(ctx context.Context, id string) (E, error) {
	return s.loader.Get(ctx, id)
}

func (s *EntityStore[E]) List(ctx context.Context, f Filter, sort Sort) ([]E, error) {
	return s.loader.List(ctx, f, sort)
}

func (s *EntityStore[E]) ListPage(ctx context.Context, f Filter, sort Sort, p Page) ([]E, error) {
	return s.loader.ListPage(ctx, f, sort, p)
}

// Save returning an error wrapping ErrDeliveryFailed means the write
// committed and only delivery failed — do not retry.
func (s *EntityStore[E]) Save(ctx context.Context, e E) error {
	return s.saver.Save(ctx, e)
}
