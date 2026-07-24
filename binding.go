package ember

import "context"

// Binding declares how one entity type is persisted: its repository + marshaler.
// It is the single source both EntityLoader and EntitySaver are built from.
type Binding[E Entity] struct {
	repo      EntityRepository
	marshaler EntityMarshaler[E]
}

func Bind[E Entity](r EntityRepository, m EntityMarshaler[E]) Binding[E] {
	return Binding[E]{repo: r, marshaler: m}
}

// binding is the type-erased view the saver consumes.
type binding struct {
	typ     string
	repo    EntityRepository
	marshal func(ctx context.Context, e Entity) (*MarshaledEntity, error)
}

// binder is sealed: only ember's Binding[E] satisfies it.
type binder interface{ binding() binding }

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
