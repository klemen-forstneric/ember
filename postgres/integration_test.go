package postgres

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

func testConnString() string {
	if v := os.Getenv("EMBER_TEST_POSTGRES"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
}

func connectTestPostgres(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", testConnString())
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Skipf("postgres unavailable: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestListUnpublishedQueryParses(t *testing.T) {
	pool := connectTestPostgres(t)
	table := "ember_list_unpublished_parse_test"

	_, err := pool.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = pool.Exec(fmt.Sprintf(`CREATE TABLE %s (
		id text, entity_id text, type text, data jsonb, metadata jsonb,
		version bigint, idx int, created_at timestamptz, published boolean
	)`, table))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = pool.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table)) })

	query, _, err := listUnpublishedQuery(table, 10)
	require.NoError(t, err)

	stmt, err := pool.Prepare(query)
	require.NoError(t, err)
	require.NoError(t, stmt.Close())
}
