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
entity in the affected database. Backfilling `entity_id` is an in-place `$set`.

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

### Why the backfill leaves `_id` alone — and how to resolve it afterwards

The backfill leaves `_id` as it finds it, so migrated documents keep their string
`_id` while documents written afterwards get an ObjectId. That mixed shape is
correct but untidy, and it is resolved out of band by `NormalizeDocumentIDs`
(below), not by the backfill.

Two reasons the backfill cannot do it:

`_id` is immutable. Mongo rejects any update that alters it (*"the (immutable)
field '_id' was found to have been altered"*), in both a pipeline update and a
plain `$set`; verified against mongo:7. Changing it requires deleting the document
and reinserting it, which no update pipeline can express.

`EnsureEntities` runs on every boot of all ten services, so it must stay a cheap
no-op once migrated. A scan-delete-reinsert over the whole entities collection is
the opposite of that, and it needs transactions, which index creation must not be
inside. So it belongs in a standalone operator-invoked run, not on a boot path.

Nothing depends on `_id` either way. After this change the entity path never reads
or writes it: `Save` filters on `{type, entity_id, version}`, `Get` on
`{type, entity_id}`, `List` on `type`, and `filter.go` maps the `"id"` path to
`entity_id`. The `document` struct carries no `_id` field at all, which is why a
replacement preserves it on update and lets Mongo generate one on insert. So
normalising is a cleanup, not a correctness fix — nothing breaks if it is never
run.

**Superseded:** an earlier revision of this section also argued that normalising
would worsen the rolling-deploy hazard, because old ember filters on
`{_id: "<string id>", version}` and would match nothing once every `_id` is an
ObjectId, twinning every overlapping write. That argument is void: this plan
already mandates a hard stop-start with no old/new overlap (see "Deployment
constraint"), and the rollout happens in a low-traffic maintenance window with
nothing consuming old data. There is no overlapping old-ember instance for the
hazard to apply to.

### `NormalizeDocumentIDs` — operator-invoked cleanup

```go
func NormalizeDocumentIDs(ctx context.Context, client *mongo.Client, c *mongo.Collection) (int, error)
```

Replaces the string `_id` with a generated ObjectId on every document whose
`entity_id` **equals** its `_id`, preserving `type`, `version`, `data` and every
other field byte for byte. Returns the number converted.

- **Not on the boot path.** Neither `NewEntityRepository` nor `EnsureEntities`
  calls it. Run it deliberately, with the service stopped.
- **Only converts what `EnsureEntities` produced.** The backfill sets
  `entity_id = $_id`, so `_id == entity_id` is exactly that set — nothing less.
  Anything else is left alone, which is what keeps it off the outbox: an outbox
  entry has a string `_id` *and* an `entity_id`, but they differ, and an event's id
  lives **only** in `_id`. Rewriting one destroys the event: the relay then cannot
  decode its own backlog (*"decoding an object ID into a string is not
  supported"*), `MarkPublished` can never match, and the backlog freezes with
  `expires_at` never set. Equality excludes it by construction rather than by the
  operator being careful with collection names. Deliberately not `$expr`, which
  DocumentDB 5.0 does not support.
- **Delete-then-insert, one transaction per document.** Insert-then-delete cannot
  work: the unique `(type, entity_id)` index rejects the copy while the original
  is still present (verified — E11000 inside the transaction). Delete-then-insert
  without a transaction loses the document if the reinsert fails (verified: the
  count goes to zero). Both operations commit together or neither does.
- **Takes a `*mongo.Client`** so the session requirement is visible at the call
  site, mirroring `NewTransactor(client)`. Not strictly required — the client is
  reachable as `c.Database().Client()` — so this is an explicitness choice, not a
  technical one.
- **Resumable.** It selects on `{_id: {$type: "string"}}`, so an interrupted run is
  resumed simply by running it again; a rolled-back document is still selected, a
  committed one is not. Interrupting it is safe.
- **Idempotent.** A second run on a normalised collection converts zero.
- **The returned count is a lower bound.** It under-reports if a commit is retried
  and excludes the in-flight document when it returns an error.

## Operator runbook: normalising `_id`

### Prerequisite — the service must already run post-change ember

**Do not normalise a service that is still deployed on pre-change ember.** The
string `_id` is what the old code keys on; removing it while the old code is live
twins every write and the eventual upgrade then fails its index build — Case 2
above, service will not start.

At the time of writing, five services are still pinned to `578393179ea2` and still
call the one-arg `NewEntityRepository`: **chat, conversation, entitlement, image,
user**. Five are on the post-change ember: auth, order, payment, subscription,
wallet. Only the latter group is eligible.

The operator snippet below runs `EnsureEntities` first. On an eligible service that
is a genuine no-op. On one of the five it would *be* the migration, and normalise
would then delete the `_id`s the still-deployed old code depends on. So the
prerequisite is a hard gate, not a nicety: **check the service's `go.mod` pin and
that the deployed build takes `(ctx, collection)` before running this.**

### It is a one-way door

After normalising, rolling the ember bump back re-creates Case 2 for *every*
entity in the collection, not merely those written during a window: old ember
matches nothing, so every write twins. The "Superseded" note above is right that
there is no *concurrent* overlap to worry about, but it is wrong to conclude the
hazard is void — it survives as this prerequisite and as a rollback constraint.
Normalise only once the ember bump is one you are prepared not to revert.

### Before the window

- **Take a snapshot** of each database. Nothing here is designed to be undone.
- **Confirm the deployment supports transactions** — a replica set or mongos. On a
  standalone it fails closed with *"(IllegalOperation) Transaction numbers are only
  allowed on a replica set member or mongos"* before converting anything, so it is
  safe, but discover that beforehand rather than during the window.
- **On DocumentDB, connect with `retryWrites=false`.**
- **Size it.** Measured 2.0 ms/document against a local single-node replica set, so
  100k entities ≈ 3m22s; expect materially worse across availability zones. One
  transaction per document is the cost of not being able to lose one. It resumes,
  so an over-running job can be stopped and continued.

### Where the code lives

Not ten hand-edited `main.go` files. One small one-off tool taking `--uri`, `--db`
and `--collection`, run once per service, is the right shape — it keeps the
prerequisite check and the logging in one place and leaves no edits behind in the
services.

```go
import (
    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"

    embermongo "github.com/klemen-forstneric/ember/mongo"
)

client, err := mongo.Connect(options.Client().ApplyURI(uri))
if err != nil {
    return err
}
defer client.Disconnect(ctx)

col := client.Database(db).Collection(collection) // the entities collection

// A no-op on an eligible service; see the prerequisite above.
if err := embermongo.EnsureEntities(ctx, col); err != nil {
    return err
}
n, err := embermongo.NormalizeDocumentIDs(ctx, client, col)
if err != nil {
    return err
}
log.Printf("normalized %d entity documents", n)
```

Note the two `mongo` packages: `mongo` is the driver, `embermongo` is ember's — the
alias the ten services already use.

Order across services does not matter, and a service that is never normalised
keeps working: the mixed `_id` shape is inert.

## Who actually needs this

**One service is in production: `conversation-service`.** The other nine are
local or dev, so their collections can be dropped and recreated rather than
migrated. Earlier revisions of this plan described the change as touching "ten
live databases", which overstated the stakes and made the delete-and-reinsert
question look riskier than it is.

**conversation-service does not need this fix.** It binds four entity types to
one `entities` collection — Conversation, Message, Memory, Backlog — which is the
shape the bug affects, but only when two types deliberately share an id. All four
take ids from `skuuid.IDer`, so they never collide. auth-service is the only
service that deliberately reuses an id across types (`Revocation`'s id **is** its
`Identity`'s id), and it is the reason this fix exists.

It also cannot take the bump yet: it is pinned to `578393179ea2` and has four
two-arg `NewEntityStore(r, marshaler)` call sites, all of which break on the new
ember. Same for `chat`, `entitlement`, `image` and `user`.

So production stays untouched. conversation-service migrates when it adopts
ember's unit of work — tracked separately, and it has to change those four call
sites for that anyway. At that point the pin bump, the `NewEntityStore` migration
and `EnsureEntities` land together in one reviewed change, and `EnsureEntities`
runs automatically via the constructor, so the backfill and the index come for
free. Only `NormalizeDocumentIDs` would be a deliberate extra step, and it stays
optional — the mixed `_id` shape is inert.

Running the backfill or the normalisation against any of those five **while the
old code is still deployed** produces the unstartable-service case in "Deployment
constraint": old ember filters on `{_id: "<string id>", version}`, matches nothing
after normalisation, and twins on every write.
