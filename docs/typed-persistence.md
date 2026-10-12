# Typed persistence

Ember binds entities to `EntityRepository[E]`. A repository loads an entity by ID
and saves it using an explicit optimistic-concurrency version. The repository
owns its encoding and schema; it does not publish events or advance the entity's
in-memory version.

```go
binding := ember.Bind[*Identity](identityRepository)
loader := ember.NewEntityLoader(binding)
saver := ember.NewEntitySaver(publisher, db, logger, binding)
```

`Save(ctx, entity, version)` receives the expected persisted version in
`version.Initial()` and the proposed version in `version.Value()`. A new entity
has expected version zero. Inserts must reject existing IDs; updates must match
the expected version and must not create missing entities. Return
`ember.ErrVersionConflict` for these conflicts and `ember.ErrEntityNotFound` for
missing reads. Domain repositories can translate those errors at their boundary.

Adapters must make each save atomic and join the backend transaction carried on
`ctx`. Native Postgres adapters use `postgres.DB.Conn(ctx)`, including for reads.
Use one database/transaction wrapper for all bindings and the outbox in a saver.
A cross-database save is not an atomic operation. A single entity without events
retains the existing direct-save fast path; an adapter that writes several rows
must make that individual operation atomic itself.

`EntitySaver` owns version advancement, event staging, and event-buffer clearing.
With `AtLeastOnce`, entity writes and outbox events commit together. If the saver
joins a caller-owned transaction, its in-memory state is finalized before the
outer commit; discard/reload affected entities after an outer rollback.
`BestEffort` retains its existing caller-owned-transaction limitations.

## Migrating document callers

The former non-generic `ember.EntityRepository` is now
`ember.DocumentRepository`. Existing Mongo, JSONB, and `embertest` snapshot
repositories implement that interface. Wrap a repository and marshaler once:

```go
import "github.com/klemen-forstneric/ember/document"

adapter := document.NewAdapter[*Order](mongoRepository, orderMarshaler)
binding := ember.Bind(adapter)
loader := ember.NewEntityLoader(binding)
saver := ember.NewEntitySaver(publisher, transactor, logger, binding)

orders, err := adapter.ListPage(ctx,
    ember.Eq("user_id", userID), ember.AscLex("id"), ember.Limit(50))
```

`Bind(repository, marshaler)` becomes `Bind(document.NewAdapter(repository,
marshaler))`. `NewEntityStore` now takes a typed repository followed by publisher,
transactor, and logger. `EntityLoader` and `EntityStore` provide `Get` (and the
store also `Save`); move their former `List`/`ListPage` calls to the document adapter.
Document revision migrations and marshaler behavior remain inside that adapter.
Mongo and JSONB document saves now reject missing updates with a nonzero expected
version instead of inserting a new snapshot.
It temporarily supplies the proposed version to legacy marshalers, restores the
entity even on error/panic, and stores authoritative ID/type/version metadata.

## Native Postgres adoption

Use Ember's shared `postgres.Repository[E]` with a separate SQL row type. For
ordinary field mappings, `postgres.StructMapper[E, R]` handles conversion and
entity reconstruction. The service declares the storage shape; Ember owns SQL
CRUD, scanning, concurrency checks, and migration execution. Custom
`postgres.RowMapper[E, R]` implementations remain available for other shapes.

```go
// Domain entity: ordinary Go state and behavior.
type Identity struct {
    ember.EntityRoot
    Email  string
    UserID string
}

// Storage representation, in the mapper file.
type identityRow struct {
    Email  string `db:"email"`
    UserID string `db:"user_id,nullzero"`
}

type identityMapper = postgres.StructMapper[*Identity, identityRow]

repo, err := postgres.NewRepository(db, "identities", &identityMapper{})
// Handle err during startup, then bind the repository to the saver.
binding := ember.Bind(repo)
matches, err := repo.ListPage(ctx, ember.Eq("email", email),
    ember.Unsorted(), ember.Limit(2))
```

The constructor infers E and R from the mapper, validates R's SQL mapping, and
calls the mapper's `Validate() error` method if it provides one. `StructMapper`
compiles its field mapping once and is safe to share across goroutines. Always
pass a pointer to it; it contains an initialization guard and must not be copied.

The shared struct mapper matches exported fields by Go name. Put an
`entity:"FieldName"` tag on a row field to override its target; for example,
`Address string` tagged with `db:"email" entity:"Email"` maps to the entity's
`Email` field. Matching named scalar types are converted automatically (such as
`Method` to/from `string`). Numeric kinds must match, preventing narrowing or
number/string conversions. Matching nullable pointers are copied into fresh
allocations, so entity and row values do not share mutable pointer fields.
Pointer/value mismatches require explicit conversion in a custom mapper.

For `StructMapper`, E must point to a struct directly embedding `EntityRoot`.
It reconstructs that root from ID/version metadata without invoking a domain
constructor or emitting events. Extra entity fields are left at their zero
values on reconstruction. Domain entities need no mapping tags. R is a plain
row struct; it does not embed `EntityRoot` or implement `Entity`. Every row field
requires `db:"column"` or `db:"-"` to explicitly exclude it.

For private fields, derived values, custom constructors, or incompatible shapes,
implement the existing `RowMapper[E, R]` interface with `Marshal` and `Unmarshal`.
The repository itself places no field-layout requirements on the domain entity.

The table needs `id text PRIMARY KEY`, `version bigint NOT NULL`, and the tagged
columns. ID/version are reserved repository metadata, outside R. On writes,
Ember obtains the ID from the entity and the version from the saver. On reads,
`postgres.Row[R]` provides ID, persisted Version, and Data to the mapper. Ember
rejects nil entities and changed IDs returned by the mapper, then applies the
persisted version. The mapper must reconstruct entities without emitting events.
Both conversion methods receive the caller's context and can return errors;
marshal errors prevent the write, and unmarshal errors propagate through Get/List.

Column and table names must be simple lowercase SQL identifiers; select schemas
through the connection's search path. Supported row field types are strings,
booleans, integers, floats, named scalar types, `time.Time`, and pointers to these.
Unsupported nested row objects and collections fail at startup. A mapper can
convert richer domain values into supported row fields; more specialized storage
can still implement `EntityRepository[E]` explicitly.

Pointers preserve NULL. Row tags also support `nullzero`, which stores a zero
value as NULL and loads NULL as the Go zero value. Auth uses `nullzero` on its
row type for optional user/wallet IDs. Timestamps use UTC microseconds.
Ember does not mutate the entity during writes; mappers must follow the same
contract. Entity version advancement and event clearing remain the saver's job.

Document revision migrations remain in document marshalers. For typed SQL,
service-owned SQL migrations evolve the stored schema/data, while row mappers
own domain reconstruction and representation conversions. Neither belongs on
the domain entity.

The shared repository provides `Get`, `Save`, `List`, and `ListPage`. Filters and
sort paths name mapped SQL columns, plus `id` and `version`; unknown paths fail.
Native column types determine comparison and sorting, including timestamps;
`Sort.Ordering` does not cast a column. Ordinary B-tree indexes therefore match
queries directly. Paged queries append `id` as the tiebreaker. Keyset pagination
supports nullable columns using Postgres's default null ordering (last ascending,
first descending); a nil cursor value represents NULL. For fast filtered ordered
lists, include equality columns, the sort column, and `id` in the matching index.

Use `postgres.Migrate(ctx, pool, "auth_schema_migrations", schema.Migrations)`
with an embedded `fs.FS` whose root contains `001_name.sql`, `002_name.sql`, etc.
Ember serializes concurrent migration calls, verifies immutable file checksums,
rejects ledger gaps/newer schema versions, and commits pending DDL and ledger
records atomically. SQL requiring execution outside a transaction needs separate
migration tooling. Services retain ownership of table definitions and indexes.
No schema diffing or automatic destructive alterations happen at startup.

Use `postgres.EnsureOutbox` to provision the outbox independently of entity
tables. `EnsureSchema` and `NewEntityRepository` remain the legacy JSONB document
API for callers choosing snapshot storage. `NewRepository(db, table, mapper)` is the typed-table
API.

Auth-service is the first native adopter. Other services remain on their pinned
Ember versions. Validate Ember and auth together in a temporary Go workspace;
release the Ember revision before updating auth's module pin. Do not commit a
local filesystem `replace`. Command-result codecs and idempotency middleware are
independent of the entity persistence format and retain their existing semantics.
