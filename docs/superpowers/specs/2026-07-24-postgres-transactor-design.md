# Postgres Transactor + EventRepository

**Date:** 2026-07-24
**Scope:** `ember/postgres` only. Adds a `Transactor` implementation, makes the entity repository transaction-aware, and adds a postgres `EventRepository` (outbox parity with mongo).

## Problem

`ember.Transactor` (the unit-of-work boundary) has one implementation: `mongo`. Its reentrant `WithinTx` works because the mongo v2 driver propagates the session through `ctx`, so repository writes auto-join the transaction with no repo changes.

Postgres has no such ambient-transaction-on-context: `postgres.EntityRepository` always runs queries on `r.db` (`*sql.DB`), so a `WithinTx` would have no effect on it. And there is no `postgres.EventRepository` at all (the outbox is mongo-only), so `EntitySaver`'s event-persistence path can't run on a postgres-backed store.

Goal: give postgres a working `Transactor` (so `EntitySaver.Save` — one entity or several — is atomic on pg) and a postgres `EventRepository` with full outbox parity, so entity + events commit in one transaction.

## Non-goals

- No relay/delivery for the pg outbox. `mongo.Notifier` is the only relay; generalizing it to a backend-agnostic `Relay` that can drain the pg outbox is the already-planned separate follow-up. The pg `EventRepository` here is relay-ready (implements `ListUnpublished`/`MarkPublished`) but nothing drains it yet.
- No `expires_at` TTL cleanup. Postgres has no native TTL; `MarkPublished` records `expires_at`, but reaping expired published rows is a follow-up (a cleanup job or partitioning).
- No dynamo `Transactor` (parked — dynamo has no interactive transaction; needs a `TransactWriteItems` collector, a separate design).
- No production driver dependency: `ember/postgres` stays driver-agnostic (`database/sql`); the consumer registers the driver.

## Design

### 1. `postgres.Transactor`

Mirrors `mongo.Transactor`, reentrant. The transaction rides `ctx` under an unexported key.

```go
// transactor.go
type txKey struct{}

func ctxWithTx(ctx context.Context, tx *sql.Tx) context.Context { return context.WithValue(ctx, txKey{}, tx) }
func txFromCtx(ctx context.Context) *sql.Tx {
    tx, _ := ctx.Value(txKey{}).(*sql.Tx)
    return tx
}

type Transactor struct{ db *sql.DB }

func NewTransactor(db *sql.DB) *Transactor { return &Transactor{db: db} }

func (t *Transactor) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
    if txFromCtx(ctx) != nil {
        return fn(ctx) // reentrant: already inside a tx, join it
    }
    tx, err := t.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    if err := fn(ctxWithTx(ctx, tx)); err != nil {
        _ = tx.Rollback() // best-effort; return the original error
        return err
    }
    return tx.Commit()
}
```

Reentrancy: a `WithinTx` nested inside another (or under spark's `Atomic` once that also puts a `*sql.Tx` on the ctx via this key) joins the existing tx rather than opening a nested one (`database/sql` does not support nested transactions).

### 2. Transaction-aware repositories

`*sql.DB` and `*sql.Tx` both satisfy the same execution methods, so a tiny interface + accessor lets a repo run on whichever is active:

```go
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
```

`EntityRepository.Save`/`Get`/`List` change only their execution handle: `r.db.ExecContext(...)` → `querierFrom(ctx, r.db).ExecContext(...)` (and the `QueryRowContext`/`QueryContext` equivalents). SQL, args, and error handling (`ErrVersionConflict` on 0 rows, `ErrEntityNotFound` on `sql.ErrNoRows`) are unchanged. Standalone calls use `r.db`; calls inside `WithinTx` join the tx.

### 3. `postgres.EventRepository`

Full parity with `mongo.EventRepository`: `Save` (satisfies `ember.EventRepository`, called by `EventStore` inside the tx) plus `ListUnpublished`/`MarkPublished` (for a future relay). Tx-aware via the same `querierFrom(ctx, r.db)`.

```go
type EventRepository struct {
    db    *sql.DB
    table string
}

func NewEventRepository(db *sql.DB, table string) *EventRepository
```

Table columns (caller owns DDL):

| column        | type          | notes                                   |
|---------------|---------------|-----------------------------------------|
| `id`          | text primary key | envelope ID                          |
| `entity_id`   | text          | source entity                           |
| `type`        | text          | event type                              |
| `data`        | jsonb         | marshaled event payload                 |
| `metadata`    | jsonb         | `ember.Metadata`                        |
| `seq`         | bigint        | `Timestamp.UnixNano()`, ordering key    |
| `created_at`  | timestamptz   | envelope timestamp                      |
| `published`   | boolean       | default false                           |
| `published_at`| timestamptz null |                                      |
| `expires_at`  | timestamptz null |                                      |

- `Save(ctx, envelopes)`: no-op on empty; one multi-row `INSERT` (`published=false`), through the ctx querier so it joins the entity's tx. `data`/`metadata` marshaled to JSON for the jsonb columns.
- `ListUnpublished(ctx, limit)`: `SELECT ... WHERE published = false ORDER BY seq ASC` (apply `LIMIT` when `limit > 0`); scan back into `[]ember.EventEnvelope` (`Event.Type`/`Data` from `type`/`data`, `Metadata` from `metadata`, `Timestamp` from `created_at`).
- `MarkPublished(ctx, ids, expiresAt)`: no-op on empty; `UPDATE ... SET published = true, published_at = now(), expires_at = $ WHERE id = ANY($ids)`.

## Data flow (pg-backed EntitySaver)

```
saver.Save(ctx, order)   // order emitted events
  tx.WithinTx(ctx):       // postgres.Transactor: BeginTx, tx on ctx
    EntityStore.persist -> EntityRepository.Save -> querierFrom(ctx) == tx -> INSERT ... ON CONFLICT (in tx)
    EventStore.Save     -> EventRepository.Save  -> querierFrom(ctx) == tx -> INSERT events (in tx)
  Commit
  order.SetVersion(...); order.events().Clear()   // post-commit (EntitySaver)
[future pg relay] polls ListUnpublished -> deliver -> MarkPublished
```

## Error handling

- `fn` error inside `WithinTx` → `tx.Rollback()`, original error returned (rollback error is swallowed; the fn error is the actionable one).
- `BeginTx`/`Commit` error → returned as-is.
- Version conflict: `EntityRepository.Save` still returns `ember.ErrVersionConflict` (0 rows affected via the `ON CONFLICT ... WHERE version` guard); inside a tx it propagates out of `fn` → rollback.
- Reentrant path: `fn` runs on the existing tx; commit/rollback stays with the outermost `WithinTx`.

## Testing

`github.com/DATA-DOG/go-sqlmock` as a **test-only** dependency (production code stays driver-agnostic). This matches the package's existing convention (`filter_test.go` asserts generated SQL rather than executing it): sqlmock matches expected query text/args and drives results, but does not execute against real postgres.

- **Transactor** (`transactor_test.go`):
  - success: `ExpectBegin` → the `fn`'s `ExpectExec` → `ExpectCommit`; assert `ExpectationsWereMet`.
  - fn error: `ExpectBegin` → exec → `ExpectRollback`; the fn's error is returned.
  - reentrant: a ctx already carrying a `*sql.Tx` (from a first `WithinTx`) runs `fn` with **no** second `ExpectBegin`.
  - `BeginTx` error surfaces.
- **EntityRepository tx-routing** (`entity_repository_test.go`, extend): a `Save` invoked inside `WithinTx` issues its `Exec` within the begun tx (sqlmock records it between Begin and Commit); a standalone `Save` issues `Exec` with no Begin. Version-conflict: result with `RowsAffected() == 0` → `ErrVersionConflict`.
- **EventRepository** (`event_repository_test.go`):
  - `Save`: expected `INSERT` with the right columns/args for N envelopes; empty slice is a no-op (no query).
  - `ListUnpublished`: expected `SELECT ... WHERE published = false ORDER BY seq ASC` (+ `LIMIT` when set); feed mock rows and assert they map back to `EventEnvelope` (type/data/metadata/timestamp).
  - `MarkPublished`: expected `UPDATE` setting published/published_at/expires_at for the given ids; empty ids is a no-op.

## Files

- `postgres/transactor.go` (new) — `Transactor`, `NewTransactor`, `txKey`/`ctxWithTx`/`txFromCtx`.
- `postgres/entity_repository.go` (modify) — `querier` interface + `querierFrom`; route `Save`/`Get`/`List` through it.
- `postgres/event_repository.go` (new) — `EventRepository`, `NewEventRepository`, `Save`/`ListUnpublished`/`MarkPublished`.
- Tests alongside each; `go.mod`/`go.sum` gain `DATA-DOG/go-sqlmock` (test-only).
