package ember

import "context"

// EntityStore is a self-contained single-type convenience: typed reads plus an
// internal saver that persists the entity and its events atomically.
type EntityStore[E Entity] struct {
	loader *EntityLoader[E]
	saver  *EntitySaver
}

func NewEntityStore[E Entity](r EntityRepository, m EntityMarshaler[E], ev *EventStore, tx Transactor) *EntityStore[E] {
	b := Bind[E](r, m)
	return &EntityStore[E]{loader: NewEntityLoader(b), saver: NewEntitySaver(ev, tx, b)}
}

func (s *EntityStore[E]) Get(ctx context.Context, id string) (E, error) {
	return s.loader.Get(ctx, id)
}

func (s *EntityStore[E]) List(ctx context.Context, f Filter, sort Sort) ([]E, error) {
	return s.loader.List(ctx, f, sort)
}

func (s *EntityStore[E]) Save(ctx context.Context, e E) error {
	return s.saver.Save(ctx, e)
}
