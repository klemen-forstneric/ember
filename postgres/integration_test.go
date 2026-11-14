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
	ctx := context.Background()
	require.NoError(t, EnsureOutbox(ctx, pool, table))
	t.Cleanup(func() { _, _ = pool.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table)) })

	repo := NewEventRepository(NewDB(pool), table)
	ts := time.Unix(1_700_000_000, 0).UTC()
	require.NoError(t, repo.Save(ctx, []ember.EventEnvelope{
		versioned("b-1", "b", 1, 0, ts),
		versioned("a-2", "a", 2, 0, ts),
		versioned("a-1", "a", 1, 0, ts),
	}))

	got, err := repo.ListUnpublished(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"a-1", "a-2", "b-1"}, ids(got),
		"ordered by entity_id, then version within each entity")
	require.Equal(t, uint64(1), got[0].Version)
	require.Equal(t, "c-a-1", got[0].Metadata[ember.MetadataKey("corr")])
	require.Equal(t, []byte(`{"k": "v"}`), got[0].Event.Data)
}

func TestPartitionsSplitPendingRowsAgainstPostgres(t *testing.T) {
	pool := connectTestPostgres(t)
	table := "ember_partition_integration_test"
	_, err := pool.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, EnsureOutbox(ctx, pool, table))
	t.Cleanup(func() { _, _ = pool.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table)) })

	repo := NewEventRepository(NewDB(pool), table, WithPartitions(4))
	ts := time.Unix(1_700_000_000, 0).UTC()
	var envs []ember.EventEnvelope
	for i := range 20 {
		envs = append(envs, versioned(fmt.Sprintf("e%d", i), fmt.Sprintf("entity-%d", i), 1, 0, ts))
	}
	require.NoError(t, repo.Save(ctx, envs))

	seen := map[string]int{}
	for p := range repo.Partitions() {
		got, err := repo.ListUnpublished(ctx, p, 100)
		require.NoError(t, err)
		for _, e := range got {
			require.Equal(t, p, ember.Partition(e.EntityID, 4), "%s listed in the wrong partition", e.EntityID)
			seen[e.ID]++
		}
	}
	require.Len(t, seen, 20, "every row is in exactly one partition")
}

func TestEnsureOutboxMigratesATableFromBeforePartitions(t *testing.T) {
	pool := connectTestPostgres(t)
	table := "ember_partition_migration_test"
	_, err := pool.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = pool.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table)) })
	// The outbox as created before partitions, holding one pending row.
	_, err = pool.Exec(fmt.Sprintf(`CREATE TABLE %s (
		id text PRIMARY KEY, entity_id text NOT NULL, type text NOT NULL, data jsonb NOT NULL,
		metadata jsonb NOT NULL, version bigint NOT NULL, idx int NOT NULL, created_at timestamptz NOT NULL,
		published boolean NOT NULL DEFAULT false, published_at timestamptz, expires_at timestamptz
	)`, table))
	require.NoError(t, err)
	_, err = pool.Exec(fmt.Sprintf(`INSERT INTO %s (id, entity_id, type, data, metadata, version, idx, created_at)
		VALUES ('legacy', 'A', 'Created', '{}', '{}', 1, 0, now())`, table))
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, EnsureOutbox(ctx, pool, table))
	repo := NewEventRepository(NewDB(pool), table, WithPartitions(4))

	got, err := repo.ListUnpublished(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"legacy"}, ids(got), "rows from before partitioning belong to partition 0")
	require.NoError(t, repo.Save(ctx, []ember.EventEnvelope{versioned("new", "B", 1, 0, time.Unix(1_700_000_000, 0).UTC())}))
}

func TestEnsureOutboxMigratesATableFromBeforeOrderingKeys(t *testing.T) {
	pool := connectTestPostgres(t)
	table := "ember_ordering_key_migration_test"
	_, err := pool.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = pool.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table)) })
	// The outbox as created before ordering keys, holding one pending row.
	_, err = pool.Exec(fmt.Sprintf(`CREATE TABLE %s (
		id text PRIMARY KEY, entity_id text NOT NULL, type text NOT NULL, data jsonb NOT NULL,
		metadata jsonb NOT NULL, version bigint NOT NULL, idx int NOT NULL, partition int NOT NULL DEFAULT 0,
		created_at timestamptz NOT NULL, published boolean NOT NULL DEFAULT false, published_at timestamptz,
		expires_at timestamptz
	)`, table))
	require.NoError(t, err)
	_, err = pool.Exec(fmt.Sprintf(`INSERT INTO %s (id, entity_id, type, data, metadata, version, idx, created_at)
		VALUES ('legacy', 'A', 'Created', '{}', '{}', 1, 0, now())`, table))
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, EnsureOutbox(ctx, pool, table))
	repo := NewEventRepository(NewDB(pool), table)
	ordered := versioned("new", "message-1", 1, 0, time.Unix(1_700_000_000, 0).UTC())
	ordered.OrderingKey = "chat-1"
	require.NoError(t, repo.Save(ctx, []ember.EventEnvelope{ordered}))

	got, err := repo.ListUnpublished(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"legacy", "new"}, ids(got))
	require.Equal(t, "A", got[0].OrderingKey, "rows from before ordering keys are ordered by entity")
	require.Equal(t, "chat-1", got[1].OrderingKey)
}
