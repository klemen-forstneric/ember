package postgres

import (
	"context"

	"github.com/klemen-forstneric/ember"
)

// Row contains authoritative persisted metadata and a service-defined SQL
// representation. R is a plain struct with db tags, independent of the entity.
// It must not contain the reserved id or version columns.
type Row[R any] struct {
	ID      string
	Version ember.Version
	Data    R
}

// RowMapper separates domain entities from their SQL representation. Marshal
// must not mutate the entity. Unmarshal must reconstruct the given identity
// without emitting domain events; the repository applies the persisted version.
// Both methods receive the caller's context and may return conversion errors.
type RowMapper[E ember.Entity, R any] interface {
	Marshal(context.Context, E) (R, error)
	Unmarshal(context.Context, Row[R]) (E, error)
}
