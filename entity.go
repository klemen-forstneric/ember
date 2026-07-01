package ember

import (
	"context"
	"errors"
)

var (
	ErrEntityNotFound  = errors.New("ember: entity not found")
	ErrVersionConflict = errors.New("ember: entity version conflict")
)

// Entity
type Entity interface {
	ID() string
	Type() string
	Version() Version
	SetVersion(Version)
}

// EntityRoot
type EntityRoot struct {
	id      string
	version Version
}

func NewEntityRoot(id string) EntityRoot {
	return EntityRoot{id: id, version: NewVersion(0)}
}

func (r *EntityRoot) ID() string           { return r.id }
func (r *EntityRoot) Version() Version     { return r.version }
func (r *EntityRoot) SetVersion(v Version) { r.version = v }

// MarshaledEntity
type MarshaledEntity struct {
	ID      string
	Type    string
	Version Version
	Data    []byte
}

// EntityMarshaler
type EntityMarshaler[E Entity] interface {
	Marshal(ctx context.Context, e E) (*MarshaledEntity, error)
	Unmarshal(ctx context.Context, m *MarshaledEntity) (E, error)
}

// EntityRepository
type EntityRepository interface {
	Save(ctx context.Context, m *MarshaledEntity) error
	Get(ctx context.Context, typ, id string) (*MarshaledEntity, error)
	List(ctx context.Context, typ string, f Filter) ([]*MarshaledEntity, error)
}

// EntityStore
type EntityStore[E Entity] struct {
	repository EntityRepository
	marshaler  EntityMarshaler[E]
}

func NewEntityStore[E Entity](r EntityRepository, m EntityMarshaler[E]) *EntityStore[E] {
	return &EntityStore[E]{repository: r, marshaler: m}
}

func (s *EntityStore[E]) Get(ctx context.Context, id string) (E, error) {
	var empty E
	m, err := s.repository.Get(ctx, empty.Type(), id)
	if err != nil {
		return empty, err
	}

	return s.marshaler.Unmarshal(ctx, m)
}

func (s *EntityStore[E]) List(ctx context.Context, f Filter) ([]E, error) {
	var empty E
	ms, err := s.repository.List(ctx, empty.Type(), f)
	if err != nil {
		return nil, err
	}

	out := make([]E, 0, len(ms))
	for _, m := range ms {
		e, err := s.marshaler.Unmarshal(ctx, m)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}

	return out, nil
}

func (s *EntityStore[E]) Save(ctx context.Context, e E) error {
	next := e.Version().Inc()
	e.SetVersion(next)

	m, err := s.marshaler.Marshal(ctx, e)
	if err != nil {
		return err
	}

	if err := s.repository.Save(ctx, m); err != nil {
		return err
	}

	// Collapse the version so a subsequent Save of the same in-memory entity
	// filters on the just-persisted version rather than the original Initial().
	// Without this, repeated saves of one instance (e.g. a streamed message
	// persisted across create -> partial flushes -> complete) self-conflict,
	// because Version.Inc keeps the original Initial() across increments.
	e.SetVersion(NewVersion(next.Value()))
	return nil
}
