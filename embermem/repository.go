// Package embermem provides an in-memory ember.EntityRepository for use as a
// test double. It has filter, sort, and optimistic-version parity with the real
// backends so unit tests exercise the same semantics. It is NOT for production.
package embermem

import (
	"context"
	"sync"

	"github.com/klemen-forstneric/ember"
)

var _ ember.EntityRepository = (*EntityRepository)(nil)

type EntityRepository struct {
	mu   sync.Mutex
	docs map[string]*ember.MarshaledEntity // key: type/id
}

func New() *EntityRepository {
	return &EntityRepository{docs: map[string]*ember.MarshaledEntity{}}
}

func key(typ, id string) string { return typ + "/" + id }

// Save enforces optimistic concurrency the way the persistent backends do.
func (r *EntityRepository) Save(_ context.Context, m *ember.MarshaledEntity) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	k := key(m.Type, m.ID)
	if cur, ok := r.docs[k]; ok {
		if cur.Version.Value() != m.Version.Initial() {
			return ember.ErrVersionConflict
		}
	} else if m.Version.Initial() != 0 {
		return ember.ErrVersionConflict
	}
	stored := *m
	r.docs[k] = &stored
	return nil
}

func (r *EntityRepository) Get(_ context.Context, typ, id string) (*ember.MarshaledEntity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.docs[key(typ, id)]; ok {
		clone := *m
		return &clone, nil
	}
	return nil, ember.ErrEntityNotFound
}

func (r *EntityRepository) List(_ context.Context, typ string, f ember.Filter, s ember.Sort) ([]*ember.MarshaledEntity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []*ember.MarshaledEntity
	for _, m := range r.docs {
		if m.Type != typ {
			continue
		}
		ok, err := matches(f, m)
		if err != nil {
			return nil, err
		}
		if ok {
			clone := *m
			out = append(out, &clone)
		}
	}

	if err := applySort(out, s); err != nil {
		return nil, err
	}
	return out, nil
}
