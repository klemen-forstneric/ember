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
	Load(ctx context.Context, typ, id string) (*MarshaledEntity, error)
}

// EntityStore
type EntityStore[E Entity] struct {
	repository EntityRepository
	marshaler  EntityMarshaler[E]
}

func NewEntityStore[E Entity](r EntityRepository, m EntityMarshaler[E]) *EntityStore[E] {
	return &EntityStore[E]{repository: r, marshaler: m}
}

func (s *EntityStore[E]) Load(ctx context.Context, id string) (E, error) {
	var empty E
	m, err := s.repository.Load(ctx, empty.Type(), id)
	if err != nil {
		return empty, err
	}

	return s.marshaler.Unmarshal(ctx, m)
}

func (s *EntityStore[E]) Save(ctx context.Context, e E) error {
	v := e.Version().Inc()
	e.SetVersion(v)

	m, err := s.marshaler.Marshal(ctx, e)
	if err != nil {
		return err
	}

	return s.repository.Save(ctx, m)
}
