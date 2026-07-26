package wal

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember/postgres"
)

// testConnString is the base DSN for integration tests. Start a suitable
// server with:
//
//	docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:17 \
//	  -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10
func testConnString() string {
	if v := os.Getenv("EMBER_TEST_POSTGRES"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
}

// connectTestPostgres opens a pool and skips the test when no server with
// logical decoding is reachable.
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

	var walLevel string
	if err := db.QueryRow(`SHOW wal_level`).Scan(&walLevel); err != nil {
		_ = db.Close()
		t.Skipf("postgres unavailable: %v", err)
	}
	if walLevel != "logical" {
		_ = db.Close()
		t.Skipf("postgres wal_level is %q, need \"logical\"", walLevel)
	}

	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestEnsurePublicationIsIdempotent(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)
	name := "ember_ensure_pub_test"
	ctx := context.Background()

	t.Cleanup(func() {
		_, _ = pool.Exec(`DROP PUBLICATION IF EXISTS ` + name)
	})

	require.NoError(t, EnsurePublication(ctx, db, name))
	require.NoError(t, EnsurePublication(ctx, db, name), "second call must succeed")

	var count int
	require.NoError(t,
		pool.QueryRow(`SELECT count(*) FROM pg_publication WHERE pubname = $1`, name).Scan(&count))
	require.Equal(t, 1, count)

	// A publication with no tables is all pgoutput needs to deliver messages.
	require.NoError(t,
		pool.QueryRow(`SELECT count(*) FROM pg_publication_tables WHERE pubname = $1`, name).Scan(&count))
	require.Equal(t, 0, count)
}
