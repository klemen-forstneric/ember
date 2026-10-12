package ember

import "context"

// EntityLoader reads and queries entities of a single type.
type EntityLoader[E Entity] struct{ repository EntityRepository[E] }

func NewEntityLoader[E Entity](b Binding[E]) *EntityLoader[E] {
	return &EntityLoader[E]{repository: b.repo}
}

func (l *EntityLoader[E]) Get(ctx context.Context, id string) (E, error) {
	return l.repository.Get(ctx, id)
}

func (l *EntityLoader[E]) List(ctx context.Context, filter Filter, sort Sort) ([]E, error) {
	return l.ListPage(ctx, filter, sort, Unpaged())
}

func (l *EntityLoader[E]) ListPage(ctx context.Context, filter Filter, sort Sort, page Page) ([]E, error) {
	return l.repository.ListPage(ctx, filter, sort, page)
}
