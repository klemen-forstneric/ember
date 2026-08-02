# ember: make the entity key type-scoped

**Date:** 2026-08-02
**Repo:** ember
**Status:** approved, in progress

## Problem

`mongo.EntityRepository` disagrees with itself about what identifies an entity.

`Get` and `List` take a `typ` and filter on it (`entity_repository.go:53-56`, `:93`).
`Save` does not — it upserts on `{_id, version}` alone:

```go
filter := bson.D{
	{Key: "_id", Value: m.ID},
	{Key: "version", Value: m.Version.Initial()},
}
```

So the store's read model is keyed by `(type, id)` and its write model by `id`.
Since `_id` carries a mandatory unique index, two entities of different types
that share an id cannot coexist in one collection — the second `Save` upserts,
misses on version, falls through to an insert, and dies on E11000, which
`Save` reports as `ErrVersionConflict`.

`postgres.EntityRepository` has the same split: `ON CONFLICT (id)` on write
(`entity_repository.go:31`) against `Where(sq.Eq{"type": typ, "id": id})` on read
(`:60`).

`embertest.Repository` does not: it keys by `typ + "/" + id`
(`repository.go:30`). The in-memory fake has always had the right model, which is
exactly why no test could see the bug.

### How it surfaced

auth-service is the first service to deliberately give one entity the same id as
another: its `Revocation`'s entity id **is** the identity id, so a revocation is
a primary-key get and one-per-identity holds structurally. Its
`/password-reset/confirm` saves `Identity` and `Revocation` in one transaction,
which aborts every time. The provider password change has already committed by
then, so the password changes while the revocation — the record that kills
outstanding refresh tokens — is never written.

The eight other services put several entity types in one collection too
(`subscription-service`: `Subscription` + `BillingAttempt`; `order-service`:
`Order` + `Offer` + `Product`). They have never hit this because their ids are
independently generated UUIDs and never collide. Their data is not corrupt; they
are relying on luck for a guarantee the store should provide.

## Decision

Identity moves off `_id` onto an explicit, type-scoped compound key.

- Documents gain an `entity_id` field carrying the entity's id.
- A **unique index on `(type, entity_id)`** enforces the real constraint.
- `_id` becomes a meaningless surrogate — Mongo generates an ObjectId on insert.

Rejected: a composite `_id` of `type/id`. Semantically equivalent, but `_id` is
immutable, so migrating existing documents means delete-and-reinsert for every
entity in nine live databases. Backfilling `entity_id` is an in-place `$set`.

Rejected: one collection per entity type. It works around the library's
inconsistency rather than fixing it, and leaves the next service to reuse an id
across types hitting the same wall.

## Mongo changes

Document shape:

```
{ _id: ObjectId(...), entity_id: "<id>", type: "<type>", version: <uint64>, data: {...} }
```

- **`Save`** — filter `{type, entity_id, version: initial}`; replacement carries
  `type`, `entity_id`, `version`, `data`. Upsert. `_id` is absent from the filter,
  so an insert gets a generated ObjectId. A duplicate-key error now means a
  genuine version conflict on the same `(type, entity_id)`, which is what
  `ErrVersionConflict` already claims.
- **`Get`** — filter `{type, entity_id}`.
- **`List`** — filter on `type` as today; decode `entity_id` rather than `_id`.

### The index is not optional

Before this change the optimistic lock rode on Mongo's `_id` index: mandatory,
auto-created, impossible to forget. After it, the lock rides on the unique
`(type, entity_id)` index. Trace a stale write: the filter misses on version,
the upsert falls through to an insert, and only that index turns the insert into
the E11000 that becomes `ErrVersionConflict`. **Without the index the insert
succeeds** — two documents for one logical entity, `Get` returning whichever
Mongo picks, and no error ever. That is a quieter and worse failure than the bug
being fixed.

There is a matching ordering hazard: a unique index treats a missing field as
null, so building it before the backfill collides every un-migrated document
against every other.

So construction, not convention, guarantees both:

```go
func NewEntityRepository(ctx context.Context, c *mongo.Collection) (*EntityRepository, error)
```

It calls `EnsureEntities` itself. A repository cannot exist without its index,
the two steps cannot be inverted, and a failure surfaces at startup rather than
as silent divergence later. Index creation is idempotent and runs outside any
transaction. `EnsureEntities` stays exported for callers that provision
collections separately.

### Migration

`EnsureEntities(ctx, *mongo.Collection)` — called by the constructor, and
exported for standalone use. Idempotent, no deletes:

1. `UpdateMany({entity_id: {$exists: false}}, [{$set: {entity_id: "$_id"}}])` —
   an aggregation-pipeline update, so `entity_id` takes the existing `_id`'s
   value. Old documents keep their string `_id`; new ones get ObjectIds. Mixed
   `_id` types in one collection are fine.
2. Create the unique index on `{type: 1, entity_id: 1}`.

Backfilled `entity_id` values inherit `_id`'s per-collection uniqueness, so the
index can never fail to build on existing data.

Each service picks the migration up when it next deploys, independently and in
any order, because the constructor runs it. The ten callers change from
`NewEntityRepository(c)` to `NewEntityRepository(ctx, c)` with an error check —
the compiler finds every one.

## Postgres changes

Already has separate `id` and `type` columns, so only the write path is wrong:

- `ON CONFLICT (id)` → `ON CONFLICT (id, type)`.
- The table needs a unique constraint on `(id, type)` rather than on `id`.

Postgres is dormant (test-only, go-sqlmock), so this is a correctness fix with no
live data behind it.

## embertest

Already correct. Confirm its semantics match — `key(typ, id)` is the same
constraint this change gives Mongo — and leave it alone otherwise.

## Verification

- ember's own suites, including the mongo integration tests.
- auth-service: `/password-reset/confirm` must return 204 and write both
  documents, with the `Revocation` visible in Mongo. This is the case that was
  failing; it is the acceptance test.
- One sibling service (subscription-service, which binds two entity types)
  boots, migrates, and round-trips an entity.
