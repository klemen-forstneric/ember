package ember

import "context"

// Binding
type Binding[E Entity] struct {
	repo      EntityRepository
	marshaler EntityMarshaler[E]
}

func Bind[E Entity](r EntityRepository, m EntityMarshaler[E]) Binding[E] {
	return Binding[E]{repo: r, marshaler: m}
}

// binder
type binder interface {
	binding() binding
}

// binding
type binding struct {
	typ     string
	repo    EntityRepository
	marshal func(ctx context.Context, e Entity) (*MarshaledEntity, error)
}

func (b Binding[E]) binding() binding {
	var zero E

	return binding{
		typ:  zero.Type(),
		repo: b.repo,
		marshal: func(ctx context.Context, e Entity) (*MarshaledEntity, error) {
			return b.marshaler.Marshal(ctx, e.(E))
		},
	}
}
