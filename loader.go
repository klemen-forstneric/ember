package ember

import "context"

// EntityLoader reads entities of a single type. Domain repositories own queries.
type EntityLoader[E Entity] struct{ repository EntityRepository[E] }

func NewEntityLoader[E Entity](b Binding[E]) *EntityLoader[E] {
	return &EntityLoader[E]{repository: b.repo}
}

func (l *EntityLoader[E]) Get(ctx context.Context, id string) (E, error) {
	return l.repository.Get(ctx, id)
}
