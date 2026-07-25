package postgres

import (
	"context"
	"encoding/json"
	"time"

	sq "github.com/Masterminds/squirrel"

	"github.com/klemen-forstneric/ember"
)

// EventRepository stores events in an outbox table. Save is called by
// the Publisher (inside a transaction); ListUnpublished/MarkPublished are
// driven by a relay.
type EventRepository struct {
	db    *DB
	table string
}

func NewEventRepository(db *DB, table string) *EventRepository {
	return &EventRepository{db: db, table: table}
}

func (r *EventRepository) Save(ctx context.Context, envelopes []ember.EventEnvelope) error {
	if len(envelopes) == 0 {
		return nil
	}

	insert := psql.Insert(r.table).
		Columns("id", "entity_id", "type", "data", "metadata", "seq", "created_at", "published")

	for _, e := range envelopes {
		metadata, err := json.Marshal(e.Metadata)
		if err != nil {
			return err
		}

		insert = insert.Values(
			e.ID,
			e.EntityID,
			e.Event.Type,
			e.Event.Data,
			metadata,
			e.Timestamp.UnixNano(),
			e.Timestamp.UTC(),
			false,
		)
	}

	query, args, err := insert.ToSql()
	if err != nil {
		return err
	}
	_, err = r.db.Conn(ctx).ExecContext(ctx, query, args...)
	return err
}

func (r *EventRepository) ListUnpublished(ctx context.Context, limit int) ([]ember.EventEnvelope, error) {
	qb := psql.
		Select("id", "entity_id", "type", "data", "metadata", "created_at").
		From(r.table).
		Where(sq.Eq{"published": false}).
		OrderBy("seq ASC")

	if limit > 0 {
		qb = qb.Limit(uint64(limit))
	}

	query, args, err := qb.ToSql()
	if err != nil {
		return nil, err
	}
	rows, err := r.db.Conn(ctx).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var es []ember.EventEnvelope
	for rows.Next() {
		var (
			id, entityID, typ string
			data, metadata    []byte
			createdAt         time.Time
		)
		if err := rows.Scan(&id, &entityID, &typ, &data, &metadata, &createdAt); err != nil {
			return nil, err
		}
		md := ember.Metadata{}
		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &md); err != nil {
				return nil, err
			}
		}

		es = append(es, ember.EventEnvelope{
			ID:       id,
			EntityID: entityID,
			Event: &ember.MarshaledEvent{
				Type: typ,
				Data: data,
			},
			Metadata:  md,
			Timestamp: createdAt,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return es, nil
}

func (r *EventRepository) MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	query, args, err := psql.
		Update(r.table).
		Set("published", true).
		Set("published_at", time.Now().UTC()).
		Set("expires_at", expiresAt).
		Where(sq.Eq{"id": ids}).
		ToSql()
	if err != nil {
		return err
	}
	_, err = r.db.Conn(ctx).ExecContext(ctx, query, args...)
	return err
}
