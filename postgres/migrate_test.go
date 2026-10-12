package postgres

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestMigrateSerializesAndValidatesHistory(t *testing.T) {
	pool := nativeTestDB(t)
	source := fstest.MapFS{
		"001_create.sql": {Data: []byte(`CREATE TABLE widgets (id text PRIMARY KEY)`)},
		"002_extend.sql": {Data: []byte(`ALTER TABLE widgets ADD COLUMN label text`)},
	}
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- Migrate(ctx, pool, "test_migrations", source) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var n int
	require.NoError(t, pool.QueryRow(`SELECT count(*) FROM test_migrations`).Scan(&n))
	require.Equal(t, 2, n)
	changed := fstest.MapFS{"001_create.sql": {Data: []byte(`SELECT 1`)}, "002_extend.sql": source["002_extend.sql"]}
	require.ErrorContains(t, Migrate(ctx, pool, "test_migrations", changed), "checksum changed")
	older := fstest.MapFS{"001_create.sql": source["001_create.sql"]}
	require.ErrorContains(t, Migrate(ctx, pool, "test_migrations", older), "newer")
	_, err := pool.Exec(`DELETE FROM test_migrations WHERE version=1`)
	require.NoError(t, err)
	require.ErrorContains(t, Migrate(ctx, pool, "test_migrations", source), "gap")
}

func TestMigrateRollsBackDDLAndLedger(t *testing.T) {
	pool := nativeTestDB(t)
	source := fstest.MapFS{
		"001_create.sql": {Data: []byte(`CREATE TABLE widgets (id text PRIMARY KEY)`)},
		"002_broken.sql": {Data: []byte(`ALTER TABLE missing ADD COLUMN label text`)},
	}
	require.Error(t, Migrate(context.Background(), pool, "test_migrations", source))
	for _, name := range []string{"widgets", "test_migrations"} {
		var table sql.NullString
		require.NoError(t, pool.QueryRow(`SELECT to_regclass($1)::text`, name).Scan(&table))
		require.False(t, table.Valid)
	}
}

func TestMigrateRejectsInvalidConfigurationBeforeOpeningTransaction(t *testing.T) {
	for _, source := range []fstest.MapFS{
		{}, {"002_gap.sql": {Data: []byte(`SELECT 1`)}},
		{"1_short.sql": {Data: []byte(`SELECT 1`)}},
		{"001_a.sql": {Data: []byte(`SELECT 1`)}, "001_b.sql": {Data: []byte(`SELECT 1`)}},
	} {
		require.Error(t, Migrate(context.Background(), nil, "test_migrations", source))
	}
	require.Error(t, Migrate(context.Background(), nil, "invalid;table", fstest.MapFS{}))
}
