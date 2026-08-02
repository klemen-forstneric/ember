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

The nine other services put several entity types in one collection too
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
entity in ten live databases. Backfilling `entity_id` is an in-place `$set`.

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

The backfill is restricted to string `_id`s. Copying an ObjectId `_id` would set
`entity_id` to an ObjectId, which no `Get` — comparing a string — could ever
match: the document would be permanently invisible while occupying a unique-index
slot that can never collide. Left alone, it either stays merely invisible or
collides loudly at index build. Loud beats silent.

Backfilled `entity_id` values inherit `_id`'s per-collection uniqueness, so the
index can never fail to build on existing data.

Each service picks the migration up when it next deploys, independently and in
any order, because the constructor runs it. The ten callers change from
`NewEntityRepository(c)` to `NewEntityRepository(ctx, c)` with an error check —
the compiler finds every one:

    auth, chat, conversation, entitlement, image,
    order, payment, subscription, user, wallet

## Postgres changes

Already has separate `id` and `type` columns, so only the write path is wrong:

- `ON CONFLICT (id)` → `ON CONFLICT (id, type)`.
- The table needs a unique constraint on `(id, type)` rather than on `id`.

Postgres is dormant (test-only, go-sqlmock), so this is a correctness fix with no
live data behind it.

### Required DDL before Postgres is activated

This change **fails closed**. `ON CONFLICT (id, type)` needs a unique index on
exactly those two columns; the old `ON CONFLICT (id)` happened to match the `id`
primary key a caller would naturally create, so the requirement was invisible.
Without it every `Save` errors with *"there is no unique or exclusion constraint
matching the ON CONFLICT specification"* — not on some rows, on all of them.

ember ships no DDL and no migration framework; the caller owns the table. Whoever
activates Postgres must create:

```sql
CREATE TABLE entities (
    id      text   NOT NULL,
    type    text   NOT NULL,
    version bigint NOT NULL,
    data    jsonb  NOT NULL,
    PRIMARY KEY (id, type)
);
```

`PRIMARY KEY (id, type)` satisfies the conflict target. So does a separate
`UNIQUE (id, type)` alongside a different primary key, and so does an index
declared in the opposite order, `(type, id)` — verified against postgres:17.
Postgres matches the conflict target by column *set*, so declaration order is
irrelevant, but the set must match exactly. A unique constraint on `id` alone
does not match and raises the error above on the first `Save`.

No test asserts this. `postgres/entity_repository_test.go` matches only
`"INSERT INTO entities"` through go-sqlmock, which cannot know what constraints
exist; asserting the generated SQL string would test the query builder, not
behaviour. The guarantee has to come from the DDL above.

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

## Deployment constraint: do not roll this out gradually

Each service must be **stopped and restarted, not rolled**, across the ember
bump. There must be no window in which both ember versions write the same
collection. On compose this is the default; on any orchestrator it means a
recreate strategy for that one release.

The reason is that old ember's `Save` filters on `{_id, version}` and replaces
whole documents. What that does during an overlap window depends on which
document it lands on, and the two cases are not equally bad.

**Case 1 — a pre-existing entity (string `_id`). Self-healing.** Old ember's
filter matches, so it replaces in place; its replacement carries no `entity_id`,
so the field is stripped. But the string `_id` survives the replacement, and the
next `EnsureEntities` re-derives `entity_id` from it. The document is invisible
to `Get` until the next startup, then recovers on its own. Bad, not fatal.

**Case 2 — an entity created by new ember during the window (ObjectId `_id`).
Fatal.** Old ember's filter `{_id: "<string id>", version: n}` cannot match a
document whose `_id` is an ObjectId. So it upserts, and on a replacement
upsert-insert Mongo *does* carry `_id` across from the filter (the one equality
field it carries — see the corrections below). The result is a second document
with `_id: "<string id>"` and no `entity_id`. The twin exists immediately, and it
does not collide at insert time because the unique index sees its missing
`entity_id` as null.

The next `EnsureEntities` then backfills that twin's `entity_id` from its string
`_id` — producing a genuine duplicate of the real document's `(type, entity_id)`.
The index build fails:

```
(DuplicateKey) Index build failed ... dup key: { type: "order", entity_id: "c" }
```

Because the constructor builds the index, **the service cannot start**, and it
cannot start again until someone reconciles the duplicates by hand in Mongo.
That is the true worst case: not a stale read, an unstartable service needing
manual data surgery. Both cases were reproduced against mongo:7.

A partial index (`partialFilterExpression: {entity_id: {$exists: true}}`) would
survive the window, but only by trading a loud failure for silent divergence: a
stripped document looks absent, so the next save inserts a second one. A hard
stop is the honest option.

## Verified during implementation, contradicting this plan

- **`ReplaceOne` upsert-insert does NOT carry the filter's equality fields into
  the inserted document.** The plan claimed it did. It does not: a replacement
  document replaces wholesale, and the "build a base document from the equality
  clauses" rule applies to update-*operator* updates only. Correctness rests
  entirely on the `document` struct carrying `type`, `entity_id`, `version` and
  `data`. Do not "simplify" the replacement to lean on the filter — every insert
  would lose its key fields.
- **`_id` is the one exception.** Mongo does carry `_id` from the filter into a
  replacement upsert-insert. That is why our filter omits it (so inserts get a
  generated ObjectId) and why old ember's filter, which includes it, manufactures
  the string-`_id` twin described in the deployment constraint above.
- **`mongo/filter.go` mapped the filter path `"id"` onto `_id`.** After this
  change that silently matches nothing, and `Asc("id")`/`Desc("id")` sort by
  insertion-time ObjectId. Now mapped to `entity_id`, matching how `embertest`
  resolves the same path to `m.ID`.
- **The backfill must skip non-string `_id`s.** Copying an ObjectId into
  `entity_id` orphans the document permanently and silently; see the migration
  section.
