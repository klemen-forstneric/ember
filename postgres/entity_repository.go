package postgres

import (
	"context"
	"database/sql"

	sq "github.com/Masterminds/squirrel"

	"github.com/klemen-forstneric/ember"
)

// psql renders `?` placeholders as Postgres `$N`.
var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// EntityRepository
type EntityRepository struct {
	db    *sql.DB
	table string
}

func NewEntityRepository(db *sql.DB, table string) *EntityRepository {
	return &EntityRepository{db: db, table: table}
}

func (r *EntityRepository) Save(ctx context.Context, m *ember.MarshaledEntity) error {
	query, args, err := psql.
		Insert(r.table).
		Columns("id", "type", "version", "data").
		Values(m.ID, m.Type, m.Version.Value(), m.Data).
		Suffix(
			"ON CONFLICT (id) DO UPDATE SET version = ?, data = ? WHERE "+r.table+".version = ?",
			m.Version.Value(), m.Data, m.Version.Initial(),
		).
		ToSql()
	if err != nil {
		return err
	}

	res, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return ember.ErrVersionConflict
	}

	return nil
}

func (r *EntityRepository) Get(ctx context.Context, typ, id string) (*ember.MarshaledEntity, error) {
	query, args, err := psql.
		Select("version", "data").
		From(r.table).
		Where(sq.Eq{"type": typ, "id": id}).
		ToSql()
	if err != nil {
		return nil, err
	}

	var (
		version uint64
		data    []byte
	)
	row := r.db.QueryRowContext(ctx, query, args...)

	if err := row.Scan(&version, &data); err == sql.ErrNoRows {
		return nil, ember.ErrEntityNotFound
	} else if err != nil {
		return nil, err
	}

	return &ember.MarshaledEntity{
		ID:      id,
		Type:    typ,
		Version: ember.NewVersion(version),
		Data:    data,
	}, nil
}

func (r *EntityRepository) List(ctx context.Context, typ string, f ember.Filter) ([]*ember.MarshaledEntity, error) {
	pred, err := buildPredicate(f)
	if err != nil {
		return nil, err
	}

	qb := psql.Select("id", "version", "data").From(r.table).Where(sq.Eq{"type": typ})
	if pred != nil {
		qb = qb.Where(pred) // multiple Where clauses are AND-ed together
	}

	query, args, err := qb.ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*ember.MarshaledEntity
	for rows.Next() {
		var (
			id      string
			version uint64
			data    []byte
		)
		if err := rows.Scan(&id, &version, &data); err != nil {
			return nil, err
		}
		out = append(out, &ember.MarshaledEntity{
			ID:      id,
			Type:    typ,
			Version: ember.NewVersion(version),
			Data:    data,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
