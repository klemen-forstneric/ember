package postgres

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

// Each test owns a schema, never a shared service table. An explicit test DSN
// makes unavailable Postgres a failure instead of silently skipping coverage.
func nativeTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("EMBER_TEST_POSTGRES")
	explicit := dsn != ""
	if !explicit {
		dsn = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)
	admin := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = admin.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		if explicit {
			t.Fatalf("required Postgres unavailable: %v", err)
		}
		t.Skipf("Postgres unavailable: %v", err)
	}
	schema := "ember_native_test_" + uuid.NewString()[:8]
	_, err = admin.ExecContext(ctx, `CREATE SCHEMA `+schema)
	require.NoError(t, err)
	cfg.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = db.Close(); _, _ = admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`) })
	return db
}
