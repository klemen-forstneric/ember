package ember

import "context"

// Binding connects one entity type to its persistence adapter.
type Binding[E Entity] struct{ repo EntityRepository[E] }

func Bind[E Entity](r EntityRepository[E]) Binding[E] { return Binding[E]{repo: r} }

type binder interface{ binding() binding }
type binding struct {
	typ  string
	save func(context.Context, Entity, Version) error
}

func (b Binding[E]) binding() binding {
	var zero E
	return binding{
		typ: zero.Type(),
		save: func(ctx context.Context, e Entity, v Version) error {
			return b.repo.Save(ctx, e.(E), v)
		},
	}
}
