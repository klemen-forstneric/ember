package ember

import "context"

// EntityLoader reads entities of a single type.
type EntityLoader[E Entity] struct {
	repository EntityRepository
	marshaler  EntityMarshaler[E]
}

func NewEntityLoader[E Entity](b Binding[E]) *EntityLoader[E] {
	return &EntityLoader[E]{repository: b.repo, marshaler: b.marshaler}
}

func (l *EntityLoader[E]) Get(ctx context.Context, id string) (E, error) {
	var empty E
	m, err := l.repository.Get(ctx, empty.Type(), id)
	if err != nil {
		return empty, err
	}
	return l.marshaler.Unmarshal(ctx, m)
}

func (l *EntityLoader[E]) List(ctx context.Context, f Filter, sort Sort) ([]E, error) {
	var empty E
	ms, err := l.repository.List(ctx, empty.Type(), f, sort)
	if err != nil {
		return nil, err
	}
	out := make([]E, 0, len(ms))
	for _, m := range ms {
		e, err := l.marshaler.Unmarshal(ctx, m)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}
