package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/klemen-forstneric/ember"
)

// EntityRepository
type EntityRepository struct {
	db    *sql.DB
	table string
}

func NewEntityRepository(db *sql.DB, table string) *EntityRepository {
	return &EntityRepository{db: db, table: table}
}

func (r *EntityRepository) Save(ctx context.Context, m *ember.MarshaledEntity) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (id, type, version, data)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE
			SET version = $3, data = $4
			WHERE %s.version = $5`, r.table, r.table)

	res, err := r.db.ExecContext(ctx, query, m.ID, m.Type, m.Version.Value(), m.Data, m.Version.Initial())
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
	query := fmt.Sprintf(`
		SELECT version, data
		FROM %s
		WHERE type = $1 AND id = $2`, r.table)

	var (
		version uint64
		data    []byte
	)
	row := r.db.QueryRowContext(ctx, query, typ, id)

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
	where, args, err := buildWhere(f)
	if err != nil {
		return nil, err
	}

	typeIdx := len(args) + 1
	args = append(args, typ)

	var sb strings.Builder
	fmt.Fprintf(&sb, "SELECT id, version, data FROM %s WHERE type = $%d", r.table, typeIdx)
	if where != "" {
		fmt.Fprintf(&sb, " AND (%s)", where)
	}

	rows, err := r.db.QueryContext(ctx, sb.String(), args...)
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
