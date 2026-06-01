package postgres

import (
	"context"
	"database/sql"
	"fmt"

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

func (r *EntityRepository) Load(ctx context.Context, typ, id string) (*ember.MarshaledEntity, error) {
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
