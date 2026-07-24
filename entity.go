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
	events() *Events // unexported: only ember-rooted types satisfy Entity
}

// EntityRoot supplies identity, optimistic-concurrency version, and a domain-
// event buffer to an entity. None of these are serialized here — persistence is
// owned by a per-entity marshaler.
type EntityRoot struct {
	i string
	v Version
	e Events
}

func NewEntityRoot(id string) EntityRoot {
	return EntityRoot{i: id, v: NewVersion(0)}
}

func (r *EntityRoot) ID() string           { return r.i }
func (r *EntityRoot) Version() Version     { return r.v }
func (r *EntityRoot) SetVersion(v Version) { r.v = v }
func (r *EntityRoot) Emit(events ...Event) { r.e.Emit(events...) }
func (r *EntityRoot) events() *Events      { return &r.e }

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
	List(ctx context.Context, typ string, f Filter, s Sort) ([]*MarshaledEntity, error)
}
