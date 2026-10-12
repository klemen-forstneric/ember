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
// owned by the persistence adapter.
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

// EntityRepository loads, saves, and lists a concrete entity without prescribing
// its encoding. ListPage accepts Unpaged() for an unbounded query; filter and
// sort paths refer to the adapter's mapped fields.
// Save must use the transaction on ctx and leave entity state unchanged. Version
// carries the expected stored version in Initial and the new version in Value.
// A missing/stale update or duplicate identity returns ErrVersionConflict; Get
// returns ErrEntityNotFound when the identity is absent.
type EntityRepository[E Entity] interface {
	Get(context.Context, string) (E, error)
	Save(context.Context, E, Version) error
	ListPage(context.Context, Filter, Sort, Page) ([]E, error)
}

// DocumentRepository stores serialized snapshots. Use document.NewAdapter to
// bind one to an EntitySaver or to query it through a typed document API.
type DocumentRepository interface {
	Save(ctx context.Context, m *MarshaledEntity) error
	Get(ctx context.Context, typ, id string) (*MarshaledEntity, error)
	List(ctx context.Context, typ string, f Filter, s Sort, p Page) ([]*MarshaledEntity, error)
}
