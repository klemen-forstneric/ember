package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
)

func ensureTestTables(t *testing.T, pool *sql.DB, prefix string) (entities, outbox string) {
	t.Helper()
	entities, outbox = prefix+"_entities", prefix+"_outbox"
	t.Cleanup(func() {
		_, _ = pool.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s, %s`, entities, outbox))
	})
	return entities, outbox
}

func TestEnsureSchemaIsIdempotentAndBacksTheRepositories(t *testing.T) {
	pool := connectTestPostgres(t)
	ctx := context.Background()
	entities, outbox := ensureTestTables(t, pool, "ember_ensure_schema")

	require.NoError(t, EnsureSchema(ctx, pool, entities, outbox))
	require.NoError(t, EnsureSchema(ctx, pool, entities, outbox))

	db := NewDB(pool)
	repo := NewEntityRepository(db, entities)
	doc := &ember.MarshaledEntity{ID: "e-1", Type: "thing", Version: ember.NewVersion(0).Inc(), Data: []byte(`{"name":"a"}`)}
	require.NoError(t, repo.Save(ctx, doc))

	got, err := repo.Get(ctx, "thing", "e-1")
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"a"}`, string(got.Data))

	stale := &ember.MarshaledEntity{ID: "e-1", Type: "thing", Version: ember.NewVersion(0).Inc(), Data: []byte(`{"name":"b"}`)}
	require.ErrorIs(t, repo.Save(ctx, stale), ember.ErrVersionConflict)

	events := NewEventRepository(db, outbox)
	require.NoError(t, events.Save(ctx, []ember.EventEnvelope{{
		ID: "ev-1", EntityID: "e-1", Version: 1,
		Event:     &ember.MarshaledEvent{Type: "ThingHappened", Data: []byte(`{}`)},
		Metadata:  ember.Metadata{},
		Timestamp: time.Now().UTC(),
	}}))
	unpublished, err := events.ListUnpublished(ctx, 10)
	require.NoError(t, err)
	require.Len(t, unpublished, 1)
	require.NoError(t, events.MarkPublished(ctx, []string{"ev-1"}, time.Now().Add(time.Hour)))
	unpublished, err = events.ListUnpublished(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, unpublished)
}

func TestEnsureIndexServesTheQueryListGenerates(t *testing.T) {
	pool := connectTestPostgres(t)
	ctx := context.Background()
	entities, outbox := ensureTestTables(t, pool, "ember_ensure_index")
	require.NoError(t, EnsureSchema(ctx, pool, entities, outbox))
	require.NoError(t, EnsureIndex(ctx, pool, entities, entities+"_email", true, "email"))
	require.NoError(t, EnsureIndex(ctx, pool, entities, entities+"_email", true, "email"))

	pred, err := buildPredicate(ember.Eq("email", "a@b.test"))
	require.NoError(t, err)
	query, args, err := psql.Select("id").From(entities).Where(sq.Eq{"type": "identity"}).Where(pred).ToSql()
	require.NoError(t, err)

	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.ExecContext(ctx, `SET enable_seqscan = off`)
	require.NoError(t, err)

	rows, err := conn.QueryContext(ctx, "EXPLAIN "+query, args...)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var plan strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan.WriteString(line + "\n")
	}
	require.NoError(t, rows.Err())
	require.Contains(t, plan.String(), entities+"_email")
}

func TestEnsureIndexEnforcesUniqueness(t *testing.T) {
	pool := connectTestPostgres(t)
	ctx := context.Background()
	entities, outbox := ensureTestTables(t, pool, "ember_ensure_unique")
	require.NoError(t, EnsureSchema(ctx, pool, entities, outbox))
	require.NoError(t, EnsureIndex(ctx, pool, entities, entities+"_email", true, "email"))

	repo := NewEntityRepository(NewDB(pool), entities)
	require.NoError(t, repo.Save(ctx, &ember.MarshaledEntity{ID: "i-1", Type: "identity", Version: ember.NewVersion(0).Inc(), Data: []byte(`{"email":"a@b.test"}`)}))
	require.Error(t, repo.Save(ctx, &ember.MarshaledEntity{ID: "i-2", Type: "identity", Version: ember.NewVersion(0).Inc(), Data: []byte(`{"email":"a@b.test"}`)}))
	require.NoError(t, repo.Save(ctx, &ember.MarshaledEntity{ID: "r-1", Type: "revocation", Version: ember.NewVersion(0).Inc(), Data: []byte(`{"email":"a@b.test"}`)}))
}

func TestEnsureIndexRequiresAPath(t *testing.T) {
	require.Error(t, EnsureIndex(context.Background(), nil, "t", "t_idx", false))
}

func TestDocumentVersionedUpdateCannotInsertMissingEntity(t *testing.T) {
	pool := connectTestPostgres(t)
	ctx := context.Background()
	entities, outbox := ensureTestTables(t, pool, "ember_missing_update")
	require.NoError(t, EnsureSchema(ctx, pool, entities, outbox))
	r := NewEntityRepository(NewDB(pool), entities)
	m := &ember.MarshaledEntity{ID: "missing", Type: "item", Version: ember.NewVersion(3).Inc(), Data: []byte(`{}`)}
	require.ErrorIs(t, r.Save(ctx, m), ember.ErrVersionConflict)
	_, err := r.Get(ctx, "item", "missing")
	require.ErrorIs(t, err, ember.ErrEntityNotFound)
	m.Version = ember.NewVersion(0).Inc()
	require.NoError(t, r.Save(ctx, m))
	require.ErrorIs(t, r.Save(ctx, m), ember.ErrVersionConflict)
	m.Version = ember.NewVersion(1).Inc()
	require.NoError(t, r.Save(ctx, m))
	require.ErrorIs(t, r.Save(ctx, m), ember.ErrVersionConflict)
}
