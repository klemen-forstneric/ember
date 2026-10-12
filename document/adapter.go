// Package document adapts serialized snapshot repositories to typed persistence.
package document

import (
	"context"
	"github.com/klemen-forstneric/ember"
)

// Adapter owns document serialization and optional generic queries. Like the
// entity itself, it must not be used to save the same entity concurrently.
type Adapter[E ember.Entity] struct {
	repo      ember.DocumentRepository
	marshaler ember.EntityMarshaler[E]
}

func NewAdapter[E ember.Entity](r ember.DocumentRepository, m ember.EntityMarshaler[E]) *Adapter[E] {
	return &Adapter[E]{repo: r, marshaler: m}
}
func (a *Adapter[E]) Get(ctx context.Context, id string) (E, error) {
	var zero E
	m, err := a.repo.Get(ctx, zero.Type(), id)
	if err != nil {
		return zero, err
	}
	return a.marshaler.Unmarshal(ctx, m)
}
func (a *Adapter[E]) Save(ctx context.Context, e E, v ember.Version) error {
	m, err := a.marshal(ctx, e, v)
	if err != nil {
		return err
	}
	// Metadata remains authoritative even for a marshaler that omits it.
	m.ID, m.Type, m.Version = e.ID(), e.Type(), v
	return a.repo.Save(ctx, m)
}
func (a *Adapter[E]) marshal(ctx context.Context, e E, v ember.Version) (*ember.MarshaledEntity, error) {
	previous := e.Version()
	e.SetVersion(v)
	defer e.SetVersion(previous)
	return a.marshaler.Marshal(ctx, e)
}
func (a *Adapter[E]) List(ctx context.Context, f ember.Filter, s ember.Sort) ([]E, error) {
	return a.ListPage(ctx, f, s, ember.Unpaged())
}
func (a *Adapter[E]) ListPage(ctx context.Context, f ember.Filter, s ember.Sort, p ember.Page) ([]E, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	var zero E
	ms, err := a.repo.List(ctx, zero.Type(), f, s, p)
	if err != nil {
		return nil, err
	}
	out := make([]E, 0, len(ms))
	for _, m := range ms {
		e, err := a.marshaler.Unmarshal(ctx, m)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}
