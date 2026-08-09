package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
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

func TestListUnpublishedAgainstPostgres(t *testing.T) {
	pool := connectTestPostgres(t)
	table := "ember_list_unpublished_integration_test"

	_, err := pool.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = pool.Exec(fmt.Sprintf(`CREATE TABLE %s (
		id text, entity_id text, type text, data jsonb, metadata jsonb,
		version bigint, idx int, created_at timestamptz, published boolean
	)`, table))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = pool.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table)) })

	ctx := context.Background()
	repo := NewEventRepository(NewDB(pool), table)
	ts := time.Unix(1_700_000_000, 0).UTC()
	require.NoError(t, repo.Save(ctx, []ember.EventEnvelope{
		versioned("b-1", "b", 1, 0, ts),
		versioned("a-2", "a", 2, 0, ts),
		versioned("a-1", "a", 1, 0, ts),
	}))

	got, err := repo.ListUnpublished(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"a-1", "a-2", "b-1"}, ids(got),
		"ordered by entity_id, then version within each entity")
	require.Equal(t, uint64(1), got[0].Version)
	require.Equal(t, "c-a-1", got[0].Metadata[ember.MetadataKey("corr")])
	require.Equal(t, []byte(`{"k": "v"}`), got[0].Event.Data)
}
