package postgres

import (
	"context"
	"database/sql"

	sq "github.com/Masterminds/squirrel"

	"github.com/klemen-forstneric/ember"
)

// psql renders `?` placeholders as Postgres `$N`.
var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// document
type document struct {
	ID      string
	Type    string
	Version uint64
	Data    []byte
}

var documentColumns = []string{"id", "type", "version", "data"}

func (d *document) scan(row interface{ Scan(...any) error }) error {
	return row.Scan(&d.ID, &d.Type, &d.Version, &d.Data)
}

func (d document) NewMarshaledEntity() *ember.MarshaledEntity {
	return &ember.MarshaledEntity{
		ID:      d.ID,
		Type:    d.Type,
		Version: ember.NewVersion(d.Version),
		Data:    d.Data,
	}
}

// EntityRepository
type EntityRepository struct {
	db    *DB
	table string
}

func NewEntityRepository(db *DB, table string) *EntityRepository {
	return &EntityRepository{db: db, table: table}
}

func (r *EntityRepository) Save(ctx context.Context, m *ember.MarshaledEntity) error {
	query, args, err := psql.
		Insert(r.table).
		Columns(documentColumns...).
		Values(m.ID, m.Type, m.Version.Value(), m.Data).
		Suffix(
			"ON CONFLICT (id, type) DO UPDATE SET version = ?, data = ? WHERE "+r.table+".version = ?",
			m.Version.Value(), m.Data, m.Version.Initial(),
		).
		ToSql()
	if err != nil {
		return err
	}

	res, err := r.db.Conn(ctx).ExecContext(ctx, query, args...)
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
		Select(documentColumns...).
		From(r.table).
		Where(sq.Eq{"type": typ, "id": id}).
		ToSql()
	if err != nil {
		return nil, err
	}

	var d document
	if err := d.scan(r.db.Conn(ctx).QueryRowContext(ctx, query, args...)); err == sql.ErrNoRows {
		return nil, ember.ErrEntityNotFound
	} else if err != nil {
		return nil, err
	}

	return d.NewMarshaledEntity(), nil
}

func (r *EntityRepository) List(ctx context.Context, typ string, f ember.Filter, s ember.Sort, p ember.Page) ([]*ember.MarshaledEntity, error) {
	pred, err := buildPredicate(f)
	if err != nil {
		return nil, err
	}

	qb := psql.
		Select(documentColumns...).
		From(r.table).
		Where(sq.Eq{"type": typ})

	if pred != nil {
		qb = qb.Where(pred)
	}
	if !p.Cursor.IsZero() {
		seek, err := seekPredicate(s, p.Cursor)
		if err != nil {
			return nil, err
		}
		qb = qb.Where(seek)
	}
	if clauses := orderBy(s, !p.IsZero()); len(clauses) > 0 {
		qb = qb.OrderBy(clauses...)
	}
	if p.Limit > 0 {
		qb = qb.Limit(uint64(p.Limit))
	}
	if p.Offset > 0 {
		qb = qb.Offset(uint64(p.Offset))
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

	var out []*ember.MarshaledEntity
	for rows.Next() {
		var d document
		if err := d.scan(rows); err != nil {
			return nil, err
		}
		out = append(out, d.NewMarshaledEntity())
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
