package wal

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/postgres"
)

// testConnString is the base DSN for integration tests. Start a suitable
// server with:
//
//	docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:17 \
//	  -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10 \
//	  -c wal_sender_timeout=5s
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
		_, _ = pool.Exec(`DROP PUBLICATION IF EXISTS ` + pgQuoteIdent(name))
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

// replConnString adds replication=database to the base DSN.
func replConnString() string {
	sep := "&"
	if !strings.Contains(testConnString(), "?") {
		sep = "?"
	}
	return testConnString() + sep + "replication=database"
}

// collectingSink records every batch it is handed.
type collectingSink struct {
	mu      sync.Mutex
	batches [][]ember.EventEnvelope
}

func (s *collectingSink) Publish(_ context.Context, envelopes []ember.EventEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, append([]ember.EventEnvelope(nil), envelopes...))
	return nil
}

func (s *collectingSink) all() []ember.EventEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ember.EventEnvelope
	for _, b := range s.batches {
		out = append(out, b...)
	}
	return out
}

// setupWAL provisions a uniquely named slot and publication and returns a
// config plus a cleanup that drops both.
func setupWAL(t *testing.T, pool *sql.DB, service string) RelayConfig {
	t.Helper()
	cfg := DefaultRelayConfig(service)
	cfg.KeepAliveInterval = 200 * time.Millisecond
	cfg.AcquireInterval = 100 * time.Millisecond
	cfg.MaxRetryBackoff = time.Second

	ctx := context.Background()
	require.NoError(t, EnsurePublication(ctx, postgres.NewDB(pool), cfg.PublicationName))

	t.Cleanup(func() {
		_, _ = pool.Exec(`SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1`, cfg.SlotName)
		_, _ = pool.Exec(`DROP PUBLICATION IF EXISTS ` + pgQuoteIdent(cfg.PublicationName))
	})
	return cfg
}

// startRelay starts a relay and returns a stop func. It does not wait for
// readiness — callers decide what "ready" means for the role they are starting.
func startRelay(t *testing.T, cfg RelayConfig, sink ember.Sink, l ember.LoggerCtx) func() {
	t.Helper()
	r, err := NewRelay(cfg, replConnString(), sink, l)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			_ = r.Close()
			<-done
		})
	}
}

// runRelay starts a relay and blocks until it holds cfg.SlotName, so callers
// never race the relay's own slot creation. Only valid for a relay expected to
// win the slot: an already-active slot satisfies this gate no matter which
// relay holds it.
func runRelay(t *testing.T, pool *sql.DB, cfg RelayConfig, sink ember.Sink) func() {
	t.Helper()
	stop := startRelay(t, cfg, sink, ember.NopLogger)
	require.True(t, eventually(t, 10*time.Second, func() bool { return slotActive(t, pool, cfg.SlotName) }),
		"relay never acquired slot %s", cfg.SlotName)
	return stop
}

func slotActive(t *testing.T, pool *sql.DB, slot string) bool {
	t.Helper()
	var active bool
	err := pool.QueryRow(`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&active)
	return err == nil && active
}

// eventually polls cond until it holds or the deadline passes.
func eventually(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

func TestCommittedEventsReachTheSink(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)
	cfg := setupWAL(t, pool, "e2e_commit")
	repo := NewEventRepository(db, cfg.MessagePrefix)

	sink := &collectingSink{}
	stop := runRelay(t, pool, cfg, sink)
	defer stop()

	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repo.Save(ctx, []ember.EventEnvelope{env("e1"), env("e2")})
	}))

	require.True(t, eventually(t, 10*time.Second, func() bool { return len(sink.all()) == 2 }),
		"expected 2 events, got %d", len(sink.all()))

	got := sink.all()
	require.Equal(t, "e1", got[0].ID)
	require.Equal(t, "e2", got[1].ID)
	require.Equal(t, "c-e1", got[0].Metadata[ember.MetadataKey("correlation_id")])
}

// transactional := true means a rolled back write emits nothing.
func TestRolledBackEventsNeverReachTheSink(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)
	cfg := setupWAL(t, pool, "e2e_rollback")
	repo := NewEventRepository(db, cfg.MessagePrefix)

	sink := &collectingSink{}
	stop := runRelay(t, pool, cfg, sink)
	defer stop()

	wantErr := errors.New("domain failure")
	err := db.WithinTx(context.Background(), func(ctx context.Context) error {
		if err := repo.Save(ctx, []ember.EventEnvelope{env("rolled-back")}); err != nil {
			return err
		}
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)

	// Commit a marker afterwards so we know the relay was streaming.
	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repo.Save(ctx, []ember.EventEnvelope{env("marker")})
	}))

	require.True(t, eventually(t, 10*time.Second, func() bool { return len(sink.all()) == 1 }))
	require.Equal(t, "marker", sink.all()[0].ID)
}

// The regression test for pg-logrepl's ident.XLogPos start position.
func TestEventsWrittenWhileRelayIsDownAreDelivered(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)
	cfg := setupWAL(t, pool, "e2e_resume")
	repo := NewEventRepository(db, cfg.MessagePrefix)

	first := &collectingSink{}
	stop := runRelay(t, pool, cfg, first)
	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repo.Save(ctx, []ember.EventEnvelope{env("before")})
	}))
	require.True(t, eventually(t, 10*time.Second, func() bool { return len(first.all()) == 1 }))
	stop()

	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repo.Save(ctx, []ember.EventEnvelope{env("while-down")})
	}))

	second := &collectingSink{}
	stop2 := runRelay(t, pool, cfg, second)
	defer stop2()

	require.True(t, eventually(t, 15*time.Second, func() bool {
		for _, e := range second.all() {
			if e.ID == "while-down" {
				return true
			}
		}
		return false
	}), "event written while the relay was down must be delivered on restart")
}

// Commits under a foreign prefix still advance the cursor: several services
// sharing one database must not stall on each other's traffic.
func TestForeignPrefixCommitsAdvanceTheCursor(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)
	cfg := setupWAL(t, pool, "e2e_cursor")
	repo := NewEventRepository(db, cfg.MessagePrefix)

	sink := &collectingSink{}
	stop := runRelay(t, pool, cfg, sink)
	defer stop()

	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repo.Save(ctx, []ember.EventEnvelope{env("e1")})
	}))
	require.True(t, eventually(t, 10*time.Second, func() bool { return len(sink.all()) == 1 }))

	lag := func() int64 {
		var v int64
		require.NoError(t, pool.QueryRow(
			`SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
			 FROM pg_replication_slots WHERE slot_name = $1`, cfg.SlotName).Scan(&v))
		return v
	}

	// Generate committed messages under a prefix this relay does not care about.
	for i := 0; i < 20; i++ {
		_, err := pool.Exec(`SELECT pg_logical_emit_message(true, 'someone_else', 'x')`)
		require.NoError(t, err)
	}

	require.True(t, eventually(t, 15*time.Second, func() bool { return lag() < 10_000 }),
		"slot lag stayed at %d; the cursor is not advancing past foreign-prefix commits", lag())
}

// The regression test for the keepalive cursor advance: WAL that never
// reaches the decoder as a transaction (a plain write to a table outside the
// publication, which pgoutput suppresses as an empty commit) must still be
// passed by the slot, driven only by the server's keepalive ServerWALEnd.
func TestServerKeepaliveAdvancesTheCursor(t *testing.T) {
	pool := connectTestPostgres(t)
	cfg := setupWAL(t, pool, "e2e_keepalive")

	_, err := pool.Exec(`CREATE TABLE e2e_keepalive_scratch (id serial primary key, payload text)`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = pool.Exec(`DROP TABLE IF EXISTS e2e_keepalive_scratch`) })

	sink := &collectingSink{}
	stop := runRelay(t, pool, cfg, sink)
	defer stop()

	lag := func() int64 {
		var v int64
		require.NoError(t, pool.QueryRow(
			`SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
			 FROM pg_replication_slots WHERE slot_name = $1`, cfg.SlotName).Scan(&v))
		return v
	}

	// Generate enough WAL, on a table the publication does not include, that a
	// commit-driven advance could not plausibly explain a lag drop: pgoutput
	// emits nothing for these transactions at all, so only a keepalive can move
	// the cursor past them.
	for i := 0; i < 500; i++ {
		_, err := pool.Exec(`INSERT INTO e2e_keepalive_scratch(payload) VALUES ($1)`, strings.Repeat("x", 2000))
		require.NoError(t, err)
	}
	require.Greater(t, lag(), int64(10_000), "test setup did not generate enough WAL to be meaningful")

	require.True(t, eventually(t, 15*time.Second, func() bool { return lag() < 10_000 }),
		"slot lag stayed at %d; the server keepalive is not advancing the cursor", lag())
}

// Two services on one database keep independent cursors and see only their own
// events.
func TestTwoRelaysWithDistinctSlotsStayIndependent(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)

	cfgA := setupWAL(t, pool, "e2e_svc_a")
	cfgB := setupWAL(t, pool, "e2e_svc_b")

	sinkA, sinkB := &collectingSink{}, &collectingSink{}
	stopA := runRelay(t, pool, cfgA, sinkA)
	defer stopA()
	stopB := runRelay(t, pool, cfgB, sinkB)
	defer stopB()

	repoA := NewEventRepository(db, cfgA.MessagePrefix)
	repoB := NewEventRepository(db, cfgB.MessagePrefix)

	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repoA.Save(ctx, []ember.EventEnvelope{env("a1")})
	}))
	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repoB.Save(ctx, []ember.EventEnvelope{env("b1")})
	}))

	require.True(t, eventually(t, 15*time.Second, func() bool {
		return len(sinkA.all()) == 1 && len(sinkB.all()) == 1
	}))
	require.Equal(t, "a1", sinkA.all()[0].ID)
	require.Equal(t, "b1", sinkB.all()[0].ID)
}

// A second relay on the same slot stands by rather than double-publishing, and
// takes over once the leader lets the slot go.
func TestSecondRelayStandsByAndTakesOver(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)
	cfg := setupWAL(t, pool, "e2e_standby")
	repo := NewEventRepository(db, cfg.MessagePrefix)

	save := func(id string) {
		t.Helper()
		require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
			return repo.Save(ctx, []ember.EventEnvelope{env(id)})
		}))
	}
	delivered := func(s *collectingSink, id string) bool {
		for _, e := range s.all() {
			if e.ID == id {
				return true
			}
		}
		return false
	}

	leader, standby := &collectingSink{}, &collectingSink{}
	stopLeader := runRelay(t, pool, cfg, leader)
	defer stopLeader()

	// runRelay's gate is useless here: the slot is already active because the
	// leader holds it, so it would pass even if this relay never dialed. Wait for
	// the standby's own 55006 log instead.
	standbyLog := newRecordingLogger()
	stopStandby := startRelay(t, cfg, standby, standbyLog)
	defer stopStandby()

	require.True(t, eventually(t, 10*time.Second, func() bool {
		return slices.Contains(standbyLog.at("debug"), "WAL slot held by another replica; standing by")
	}), "the standby never dialed and got 55006; errors=%v", standbyLog.at("error"))

	save("only-once")
	require.True(t, eventually(t, 10*time.Second, func() bool { return delivered(leader, "only-once") }))
	time.Sleep(time.Second)
	require.Empty(t, standby.all(), "a standby must not publish")

	stopLeader()

	save("after-failover")
	require.True(t, eventually(t, 20*time.Second, func() bool { return delivered(standby, "after-failover") }),
		"the standby never took over the slot; errors=%v", standbyLog.at("error"))
	require.False(t, delivered(standby, "only-once"),
		"takeover must resume from the confirmed position, not replay what the leader published")
}
