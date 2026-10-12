package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func EnsureSchema(ctx context.Context, pool *sql.DB, entities, outbox string) error {
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id text NOT NULL,
			type text NOT NULL,
			version bigint NOT NULL,
			data jsonb NOT NULL,
			PRIMARY KEY (id, type)
		)`, entities),
	}
	for _, stmt := range stmts {
		if _, err := pool.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return EnsureOutbox(ctx, pool, outbox)
}

// EnsureOutbox provisions the shared outbox without a document entity table.
func EnsureOutbox(ctx context.Context, pool *sql.DB, outbox string) error {
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id text PRIMARY KEY,
			entity_id text NOT NULL,
			type text NOT NULL,
			data jsonb NOT NULL,
			metadata jsonb NOT NULL,
			version bigint NOT NULL,
			idx int NOT NULL,
			created_at timestamptz NOT NULL,
			published boolean NOT NULL DEFAULT false,
			published_at timestamptz,
			expires_at timestamptz
		)`, outbox),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_unpublished ON %s (entity_id, version, idx) WHERE NOT published`, outbox, outbox),
	}
	for _, stmt := range stmts {
		if _, err := pool.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func EnsureIndex(ctx context.Context, pool *sql.DB, table, name string, unique bool, paths ...string) error {
	if len(paths) == 0 {
		return errors.New("postgres: EnsureIndex needs at least one path")
	}
	cols := []string{"type"}
	for _, p := range paths {
		col, reserved := column(p)
		if !reserved {
			col = "(" + col + ")"
		}
		cols = append(cols, col)
	}
	kind := "INDEX"
	if unique {
		kind = "UNIQUE INDEX"
	}
	_, err := pool.ExecContext(ctx, fmt.Sprintf(`CREATE %s IF NOT EXISTS %s ON %s (%s)`, kind, name, table, strings.Join(cols, ", ")))
	return err
}
