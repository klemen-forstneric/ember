# Postgres Transactor + EventRepository — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** A reentrant `postgres.Transactor`, a transaction-aware `postgres.EntityRepository`, and a new `postgres.EventRepository` (outbox parity with mongo), so `EntitySaver` persists an entity and its events atomically on postgres.

**Architecture:** The transaction rides `ctx` under an unexported key. `WithinTx` joins an existing `*sql.Tx` (reentrant) or begins one and commits/rolls back. Repositories run through a `querierFrom(ctx, db)` helper that returns the ctx `*sql.Tx` when present, else `r.db` — so writes inside `WithinTx` join the transaction. Production code stays driver-agnostic (`database/sql`); tests use `go-sqlmock`.

**Tech Stack:** Go, `database/sql`, `github.com/Masterminds/squirrel` (query builder, already a dep), `github.com/DATA-DOG/go-sqlmock` (test-only, added here), testify.

## Global Constraints

- Module `github.com/klemen-forstneric/ember`; run `go` from the ember repo root. Package is `ember/postgres`.
- Production code imports NO sql driver (`ember/postgres` stays driver-agnostic — the consumer registers one). `go-sqlmock` is used only in `_test.go`.
- White-box tests (`package postgres`); testify assertions; match the existing `postgres/filter_test.go` style. Existing tests assert generated SQL, not real execution.
- Comments terse; only where non-obvious.
- `gofmt -l .` MUST be empty before each commit.
- Run `go build ./...` and `go test ./postgres/`. Do NOT run `go test ./...` (other subpackages need live infra and hang).
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Spec: `docs/superpowers/specs/2026-07-24-postgres-transactor-design.md`.

## File Structure

- `postgres/transactor.go` (new) — `Transactor`, `NewTransactor`, ctx helpers (`txKey`/`ctxWithTx`/`txFromCtx`), the `querier` interface + `querierFrom`.
- `postgres/entity_repository.go` (modify) — route `Save`/`Get`/`List` through `querierFrom(ctx, r.db)`.
- `postgres/event_repository.go` (new) — `EventRepository` + `NewEventRepository` + `Save`/`ListUnpublished`/`MarkPublished`.
- Tests: `postgres/transactor_test.go`, extend `postgres/entity_repository_test.go` (create if absent), `postgres/event_repository_test.go`.

---

### Task 1: `Transactor` + tx-aware `EntityRepository`

**Files:**
- Create: `postgres/transactor.go`, `postgres/transactor_test.go`
- Modify: `postgres/entity_repository.go`
- Create: `postgres/entity_repository_test.go`
- Modify: `go.mod`/`go.sum` (add `go-sqlmock`, test-only)

**Interfaces:**
- Produces:
  - `Transactor` + `NewTransactor(db *sql.DB) *Transactor`; `WithinTx(ctx, func(ctx) error) error` (satisfies `ember.Transactor`).
  - unexported `querier` interface (`ExecContext`/`QueryContext`/`QueryRowContext`) and `querierFrom(ctx context.Context, db *sql.DB) querier`.
  - unexported `ctxWithTx(ctx, *sql.Tx) context.Context`, `txFromCtx(ctx) *sql.Tx`.

- [ ] **Step 1: Add the test-only sqlmock dependency**

Run: `go get github.com/DATA-DOG/go-sqlmock@v1.5.2`
Expected: `go.mod` gains the require line. (It only becomes test-only once it's imported solely from `_test.go` — that happens in Step 2.)

- [ ] **Step 2: Write the failing Transactor tests**

Create `postgres/transactor_test.go`:

```go
package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestWithinTxCommitsOnSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tr := NewTransactor(db)
	err = tr.WithinTx(context.Background(), func(ctx context.Context) error {
		_, err := querierFrom(ctx, db).ExecContext(ctx, "INSERT INTO t VALUES (1)")
		return err
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxRollsBackOnError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	boom := errors.New("boom")
	mock.ExpectBegin()
	mock.ExpectRollback()

	tr := NewTransactor(db)
	err = tr.WithinTx(context.Background(), func(ctx context.Context) error { return boom })

	require.ErrorIs(t, err, boom)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxReentrantJoinsExistingTx(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// One begin (ours), the fn's exec, one commit (ours) — the reentrant
	// WithinTx must NOT begin a second transaction.
	mock.ExpectBegin()
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	outer, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	ctx := ctxWithTx(context.Background(), outer)

	tr := NewTransactor(db)
	ran := false
	err = tr.WithinTx(ctx, func(ctx context.Context) error {
		ran = true
		require.NotNil(t, txFromCtx(ctx), "fn keeps the existing tx on ctx")
		_, e := querierFrom(ctx, db).ExecContext(ctx, "INSERT INTO t VALUES (1)")
		return e
	})
	require.NoError(t, err)
	require.True(t, ran)
	require.NoError(t, outer.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxBeginError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	boom := errors.New("begin boom")
	mock.ExpectBegin().WillReturnError(boom)

	tr := NewTransactor(db)
	err = tr.WithinTx(context.Background(), func(ctx context.Context) error { return nil })

	require.ErrorIs(t, err, boom)
	require.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./postgres/ -run TestWithinTx -v`
Expected: FAIL — `undefined: NewTransactor` / `querierFrom` / `ctxWithTx` / `txFromCtx`.

- [ ] **Step 4: Implement `postgres/transactor.go`**

```go
package postgres

import (
	"context"
	"database/sql"
)

type txKey struct{}

func ctxWithTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

func txFromCtx(ctx context.Context) *sql.Tx {
	tx, _ := ctx.Value(txKey{}).(*sql.Tx)
	return tx
}

// querier is the subset of *sql.DB / *sql.Tx the repositories use, so a write
// can run on whichever is active on the ctx.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func querierFrom(ctx context.Context, db *sql.DB) querier {
	if tx := txFromCtx(ctx); tx != nil {
		return tx
	}
	return db
}

// Transactor runs work inside a database/sql transaction. Reentrant: if the ctx
// already carries a *sql.Tx it joins that transaction rather than beginning a
// nested one (database/sql has no nested transactions).
type Transactor struct {
	db *sql.DB
}

func NewTransactor(db *sql.DB) *Transactor {
	return &Transactor{db: db}
}

func (t *Transactor) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if txFromCtx(ctx) != nil {
		return fn(ctx)
	}

	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(ctxWithTx(ctx, tx)); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
```

- [ ] **Step 5: Run the Transactor tests**

Run: `go test ./postgres/ -run TestWithinTx -v`
Expected: PASS (all four).

- [ ] **Step 6: Route `EntityRepository` through the ctx querier**

In `postgres/entity_repository.go`, replace the three execution handles (SQL/args unchanged):

- In `Save`: `res, err := r.db.ExecContext(ctx, query, args...)` → `res, err := querierFrom(ctx, r.db).ExecContext(ctx, query, args...)`
- In `Get`: `row := r.db.QueryRowContext(ctx, query, args...)` → `row := querierFrom(ctx, r.db).QueryRowContext(ctx, query, args...)`
- In `List`: `rows, err := r.db.QueryContext(ctx, query, args...)` → `rows, err := querierFrom(ctx, r.db).QueryContext(ctx, query, args...)`

- [ ] **Step 7: Write the failing EntityRepository tx-routing test**

Create `postgres/entity_repository_test.go`:

```go
package postgres

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
)

// A Save invoked inside WithinTx must execute on the transaction (between Begin
// and Commit), not directly on the db.
func TestEntitySaveJoinsTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO entities").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	repo := NewEntityRepository(db, "entities")
	tr := NewTransactor(db)
	err = tr.WithinTx(context.Background(), func(ctx context.Context) error {
		return repo.Save(ctx, &ember.MarshaledEntity{ID: "1", Type: "order", Version: ember.NewVersion(1), Data: []byte(`{}`)})
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// A version conflict (0 rows affected) surfaces as ember.ErrVersionConflict.
func TestEntitySaveVersionConflict(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("INSERT INTO entities").WillReturnResult(sqlmock.NewResult(0, 0))

	repo := NewEntityRepository(db, "entities")
	err = repo.Save(context.Background(), &ember.MarshaledEntity{ID: "1", Type: "order", Version: ember.NewVersion(2), Data: []byte(`{}`)})

	require.ErrorIs(t, err, ember.ErrVersionConflict)
	require.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 8: Run the EntityRepository tests**

Run: `go test ./postgres/ -run TestEntity -v`
Expected: PASS. (Step 6's routing makes `TestEntitySaveJoinsTransaction` pass; the standalone conflict test exercises `r.db` directly.)

- [ ] **Step 9: Full build + package test + gofmt, then commit**

Run: `go build ./... && go test ./postgres/ && gofmt -l .`
Expected: build clean, postgres tests pass, gofmt empty. Confirm `go-sqlmock` is only imported from `_test.go` (`grep -rl DATA-DOG postgres/*.go` shows only `_test.go` files).

```bash
git add postgres/transactor.go postgres/transactor_test.go postgres/entity_repository.go postgres/entity_repository_test.go go.mod go.sum
git commit -m "feat(postgres): reentrant Transactor + tx-aware EntityRepository

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `postgres.EventRepository`

**Files:**
- Create: `postgres/event_repository.go`, `postgres/event_repository_test.go`

**Interfaces:**
- Consumes: `querierFrom`/`txFromCtx` (Task 1), `ember.EventEnvelope`, `ember.MarshaledEvent`, `ember.Metadata`, `psql` (the `$N` squirrel builder already in `entity_repository.go`).
- Produces:
  - `EventRepository` + `NewEventRepository(db *sql.DB, table string) *EventRepository`.
  - `Save(ctx, []ember.EventEnvelope) error` (satisfies `ember.EventRepository`).
  - `ListUnpublished(ctx, limit int) ([]ember.EventEnvelope, error)`.
  - `MarkPublished(ctx, ids []string, expiresAt time.Time) error`.

- [ ] **Step 1: Write the failing tests**

Create `postgres/event_repository_test.go`:

```go
package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
)

func env(id string, ts time.Time) ember.EventEnvelope {
	return ember.EventEnvelope{
		ID:        id,
		EntityID:  "A",
		Event:     &ember.MarshaledEvent{Type: "Created", Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{ember.MetadataKey("corr"): "c-" + id},
		Timestamp: ts,
	}
}

func TestEventSaveInsertsUnpublished(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("INSERT INTO events").WillReturnResult(sqlmock.NewResult(0, 2))

	repo := NewEventRepository(db, "events")
	err = repo.Save(context.Background(), []ember.EventEnvelope{
		env("e1", time.Unix(1, 0).UTC()),
		env("e2", time.Unix(2, 0).UTC()),
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEventSaveEmptyIsNoop(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	// No expectations: Save with no envelopes must issue no query.

	repo := NewEventRepository(db, "events")
	require.NoError(t, repo.Save(context.Background(), nil))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEventListUnpublishedMapsRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ts := time.Unix(1_700_000_000, 0).UTC()
	rows := sqlmock.NewRows([]string{"id", "entity_id", "type", "data", "metadata", "created_at"}).
		AddRow("e1", "A", "Created", []byte(`{"k":"v"}`), []byte(`{"corr":"c-e1"}`), ts)
	mock.ExpectQuery("SELECT .* FROM events").WillReturnRows(rows)

	repo := NewEventRepository(db, "events")
	got, err := repo.ListUnpublished(context.Background(), 10)

	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "e1", got[0].ID)
	require.Equal(t, "A", got[0].EntityID)
	require.Equal(t, "Created", got[0].Event.Type)
	require.JSONEq(t, `{"k":"v"}`, string(got[0].Event.Data))
	require.Equal(t, ts, got[0].Timestamp)
	require.Equal(t, "c-e1", got[0].Metadata[ember.MetadataKey("corr")])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEventMarkPublished(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("UPDATE events").WillReturnResult(sqlmock.NewResult(0, 2))

	repo := NewEventRepository(db, "events")
	err = repo.MarkPublished(context.Background(), []string{"e1", "e2"}, time.Unix(9, 0).UTC())

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEventMarkPublishedEmptyIsNoop(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	repo := NewEventRepository(db, "events")
	require.NoError(t, repo.MarkPublished(context.Background(), nil, time.Now()))
	require.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./postgres/ -run TestEvent -v`
Expected: FAIL — `undefined: NewEventRepository`.

- [ ] **Step 3: Implement `postgres/event_repository.go`**

```go
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	sq "github.com/Masterminds/squirrel"

	"github.com/klemen-forstneric/ember"
)

// EventRepository stores events in an outbox table. Save is called by
// ember.EventStore (inside a transaction); ListUnpublished/MarkPublished are
// driven by a relay.
type EventRepository struct {
	db    *sql.DB
	table string
}

func NewEventRepository(db *sql.DB, table string) *EventRepository {
	return &EventRepository{db: db, table: table}
}

func (r *EventRepository) Save(ctx context.Context, envelopes []ember.EventEnvelope) error {
	if len(envelopes) == 0 {
		return nil
	}

	insert := psql.Insert(r.table).
		Columns("id", "entity_id", "type", "data", "metadata", "seq", "created_at", "published")
	for _, e := range envelopes {
		metadata, err := json.Marshal(e.Metadata)
		if err != nil {
			return err
		}
		insert = insert.Values(
			e.ID, e.EntityID, e.Event.Type, e.Event.Data, metadata,
			e.Timestamp.UnixNano(), e.Timestamp.UTC(), false,
		)
	}

	query, args, err := insert.ToSql()
	if err != nil {
		return err
	}
	_, err = querierFrom(ctx, r.db).ExecContext(ctx, query, args...)
	return err
}

func (r *EventRepository) ListUnpublished(ctx context.Context, limit int) ([]ember.EventEnvelope, error) {
	qb := psql.
		Select("id", "entity_id", "type", "data", "metadata", "created_at").
		From(r.table).
		Where(sq.Eq{"published": false}).
		OrderBy("seq ASC")
	if limit > 0 {
		qb = qb.Limit(uint64(limit))
	}

	query, args, err := qb.ToSql()
	if err != nil {
		return nil, err
	}
	rows, err := querierFrom(ctx, r.db).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ember.EventEnvelope
	for rows.Next() {
		var (
			id, entityID, typ string
			data, metadata    []byte
			createdAt         time.Time
		)
		if err := rows.Scan(&id, &entityID, &typ, &data, &metadata, &createdAt); err != nil {
			return nil, err
		}
		md := ember.Metadata{}
		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &md); err != nil {
				return nil, err
			}
		}
		out = append(out, ember.EventEnvelope{
			ID:        id,
			EntityID:  entityID,
			Event:     &ember.MarshaledEvent{Type: typ, Data: data},
			Metadata:  md,
			Timestamp: createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *EventRepository) MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	query, args, err := psql.
		Update(r.table).
		Set("published", true).
		Set("published_at", time.Now().UTC()).
		Set("expires_at", expiresAt).
		Where(sq.Eq{"id": ids}).
		ToSql()
	if err != nil {
		return err
	}
	_, err = querierFrom(ctx, r.db).ExecContext(ctx, query, args...)
	return err
}
```

Note: `sq.Eq{"id": ids}` with a slice renders `id IN ($1,$2,...)` — the portable equivalent of the spec's `id = ANY(...)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./postgres/ -run TestEvent -v`
Expected: PASS (all five). If a query-matcher regex fails, adjust the expected pattern (sqlmock's default matcher is regexp) — do not weaken an assertion to hide a real mismatch.

- [ ] **Step 5: Full build + package test + gofmt, then commit**

Run: `go build ./... && go test ./postgres/ && gofmt -l .`
Expected: build clean, postgres tests pass, gofmt empty.

```bash
git add postgres/event_repository.go postgres/event_repository_test.go
git commit -m "feat(postgres): add EventRepository (outbox parity with mongo)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

## Self-Review

**Spec coverage:** Transactor reentrant WithinTx (§1) → Task 1. querier/querierFrom + tx-aware EntityRepository (§2) → Task 1. EventRepository Save/ListUnpublished/MarkPublished with jsonb + seq ordering (§3) → Task 2. Error handling (rollback on fn error, version conflict, begin error) → Task 1 tests. Testing via go-sqlmock, test-only → both tasks. Out-of-scope items (relay, TTL, dynamo) correctly excluded.

**Placeholder scan:** none — every step has complete code and exact commands.

**Type consistency:** `NewTransactor(db)`, `WithinTx(ctx, fn)`, `querierFrom(ctx, db)`, `ctxWithTx`/`txFromCtx` defined Task 1, consumed Task 2. `NewEventRepository(db, table)`, `Save`/`ListUnpublished`/`MarkPublished` signatures match the spec and `ember.EventRepository`. `psql` (the `$N` builder) is reused from `entity_repository.go`, not redefined. Columns in `Save`'s INSERT match the `ListUnpublished` SELECT and the spec's table.
