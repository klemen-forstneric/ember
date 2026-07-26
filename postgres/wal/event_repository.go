package wal

import (
	"context"

	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/postgres"
)

const emitQuery = `SELECT pg_logical_emit_message(true, $1, $2)`

// EventRepository writes events into the WAL as logical decoding messages.
// Save must run inside the caller's transaction: the emit is transactional, so
// a slot sees the message only if that transaction commits.
type EventRepository struct {
	db     *postgres.DB
	prefix string
}

func NewEventRepository(db *postgres.DB, prefix string) *EventRepository {
	return &EventRepository{db: db, prefix: prefix}
}

func (r *EventRepository) Save(ctx context.Context, envelopes []ember.EventEnvelope) error {
	for _, e := range envelopes {
		payload, err := encode(e)
		if err != nil {
			return err
		}
		if _, err := r.db.Conn(ctx).ExecContext(ctx, emitQuery, r.prefix, payload); err != nil {
			return err
		}
	}
	return nil
}
