// Package document adapts serialized snapshot repositories to typed persistence.
package document

import (
	"context"
	"fmt"

	"github.com/klemen-forstneric/ember"
)

// Repository stores serialized snapshots. Wrap one with NewAdapter to bind it
// to an EntitySaver or to query it through a typed API.
type Repository interface {
	Save(ctx context.Context, m *ember.MarshaledEntity) error
	Get(ctx context.Context, typ, id string) (*ember.MarshaledEntity, error)
	List(ctx context.Context, typ string, f ember.Filter, s ember.Sort, p ember.Page) ([]*ember.MarshaledEntity, error)
}

// Adapter owns document serialization and optional generic queries. Like the
// entity itself, it must not be used to save the same entity concurrently.
type Adapter[E ember.Entity] struct {
	repo      Repository
	marshaler ember.EntityMarshaler[E]
}

func NewAdapter[E ember.Entity](r Repository, m ember.EntityMarshaler[E]) *Adapter[E] {
	return &Adapter[E]{repo: r, marshaler: m}
}

func (a *Adapter[E]) Get(ctx context.Context, id string) (E, error) {
	var zero E
	m, err := a.repo.Get(ctx, zero.Type(), id)
	if err != nil {
		return zero, err
	}
	return a.unmarshal(ctx, m)
}

func (a *Adapter[E]) unmarshal(ctx context.Context, m *ember.MarshaledEntity) (E, error) {
	e, err := a.marshaler.Unmarshal(ctx, m)
	if err != nil {
		return e, err
	}
	if e.ID() != m.ID {
		var zero E
		return zero, fmt.Errorf("document: marshaler changed identity %q to %q", m.ID, e.ID())
	}
	// Stored metadata is authoritative, as on Save.
	e.SetVersion(m.Version)
	return e, nil
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
		e, err := a.unmarshal(ctx, m)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}
