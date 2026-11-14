package postgres

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"

	"github.com/klemen-forstneric/ember"
)

// EventRepository stores events in an outbox table. Save is called by
// the Publisher (inside a transaction); ListUnpublished/MarkPublished are
// driven by a relay.
type EventRepository struct {
	db         *DB
	table      string
	partitions int
}

// EventRepositoryOption
type EventRepositoryOption func(*EventRepository)

// WithPartitions splits the outbox into n partitions by entity, so up to n relays
// drain it in parallel. Changing n moves entities between partitions: change it
// only with an empty outbox.
func WithPartitions(n int) EventRepositoryOption {
	return func(r *EventRepository) { r.partitions = max(n, 1) }
}

func NewEventRepository(db *DB, table string, opts ...EventRepositoryOption) *EventRepository {
	r := &EventRepository{db: db, table: table, partitions: 1}
	for _, o := range opts {
		o(r)
	}
	return r
}

func (r *EventRepository) Partitions() int {
	return r.partitions
}

func (r *EventRepository) Save(ctx context.Context, envelopes []ember.EventEnvelope) error {
	if len(envelopes) == 0 {
		return nil
	}

	insert := psql.Insert(r.table).
		Columns("id", "entity_id", "ordering_key", "type", "data", "metadata", "version", "idx", "partition", "created_at", "published")

	for _, e := range envelopes {
		metadata, err := json.Marshal(e.Metadata)
		if err != nil {
			return err
		}

		insert = insert.Values(
			e.ID,
			e.EntityID,
			e.OrderingKey,
			e.Event.Type,
			e.Event.Data,
			metadata,
			int64(e.Version),
			e.Index,
			ember.Partition(e.EntityID, r.partitions),
			e.Timestamp,
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

func (r *EventRepository) ListUnpublished(ctx context.Context, partition, limit int) ([]ember.EventEnvelope, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%w: got %d", ember.ErrInvalidLimit, limit)
	}

	qb := psql.Select("id", "entity_id", "ordering_key", "type", "data", "metadata", "version", "idx", "created_at").
		From(r.table).
		Where("NOT published").
		Where(sq.Eq{"partition": partition}).
		OrderBy("entity_id", "version ASC", "idx ASC").
		Limit(uint64(limit))

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
			id, entityID, orderingKey, typ string
			data, metadata                 []byte
			version                        int64
			idx                            int
			createdAt                      time.Time
		)
		if err := rows.Scan(&id, &entityID, &orderingKey, &typ, &data, &metadata, &version, &idx, &createdAt); err != nil {
			return nil, err
		}
		md := ember.Metadata{}
		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &md); err != nil {
				return nil, err
			}
		}

		es = append(es, ember.EventEnvelope{
			ID:          id,
			EntityID:    entityID,
			OrderingKey: cmp.Or(orderingKey, entityID),
			Version:     uint64(version),
			Index:       idx,
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
