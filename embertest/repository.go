package embertest

import (
	"bytes"
	"context"
	"sync"

	"github.com/klemen-forstneric/ember"
)

// clone returns a deep copy of a marshaled entity so stored state and returned
// results never alias the caller's Data slice.
func clone(m *ember.MarshaledEntity) *ember.MarshaledEntity {
	c := *m
	c.Data = bytes.Clone(m.Data)
	return &c
}

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
	// Normalize to NewVersion(Value()) to match real backends (e.g. Mongo stores
	// the uint64 value and reconstructs with NewVersion on read). This ensures
	// that an entity fetched via Get and then re-saved does not self-conflict.
	stored := clone(m)
	stored.Version = ember.NewVersion(m.Version.Value())
	r.docs[k] = stored
	return nil
}

func (r *EntityRepository) Get(_ context.Context, typ, id string) (*ember.MarshaledEntity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.docs[key(typ, id)]; ok {
		return clone(m), nil
	}
	return nil, ember.ErrEntityNotFound
}

func (r *EntityRepository) List(_ context.Context, typ string, f ember.Filter, s ember.Sort, p ember.Page) ([]*ember.MarshaledEntity, error) {
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
			out = append(out, clone(m))
		}
	}

	applySort(out, s, !p.IsZero())

	if !p.Cursor.IsZero() {
		seeked, err := seek(out, s, p.Cursor)
		if err != nil {
			return nil, err
		}
		out = seeked
	}
	if p.Offset > 0 {
		if p.Offset >= len(out) {
			return nil, nil
		}
		out = out[p.Offset:]
	}
	if p.Limit > 0 && len(out) > p.Limit {
		out = out[:p.Limit]
	}

	return out, nil
}
