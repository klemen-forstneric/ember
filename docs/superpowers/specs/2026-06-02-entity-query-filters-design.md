# EntityStore field queries via a backend-neutral filter AST

**Date:** 2026-06-02
**Status:** Approved design

## Problem

`EntityStore[E]` can only retrieve entities by ID (`Load`). Services need to query
by other attributes — e.g. "all orders where `status = open` and `total > 4200`".

The difficulty is the abstraction boundary. To an `EntityRepository`, an entity is an
opaque blob: `MarshaledEntity{ID, Type, Version, Data []byte}`. Only the marshaler
(generic over `E`) understands the domain fields inside `Data`. Meanwhile the target
backends speak radically different query dialects — Postgres `WHERE`, Mongo BSON
filters, DynamoDB `FilterExpression`.

We need a way to (1) let the repository filter on entity attributes without
understanding the blob format, and (2) let callers express filters in a
backend-neutral way that each repository translates into its native dialect.

## Scope

**In scope:** field-level filtering returning matching entities, with comparison
operators (`=`, `!=`, `>`, `>=`, `<`, `<=`), membership (`IN`), existence, and
boolean composition (`AND`/`OR`/`NOT`). Backends: Postgres, Mongo, DynamoDB.

**Out of scope (deferred):** sorting, pagination, total counts, projections,
aggregation. The API is shaped so these slot in later additively (see Extensibility).

## Key decisions

1. **Blob introspection, not duplicated attributes.** The repository filters directly
   into the single stored copy of the data, rather than maintaining a separate set of
   queryable columns/fields. This avoids storing indexed attributes twice. It works
   because the blobs are already structured on the target backends: Mongo stores `data`
   as a nested document, Postgres stores it as `jsonb`, Dynamo as a nested map.

   Consequence accepted: filters reference the entity's **serialized field paths**
   (e.g. `"status"`, `"address.city"`), not Go field names — a mild leak of the
   serialization shape into the query layer. Truly opaque (non-introspectable) stores
   cannot participate; this is fine because entity stores are persistent databases, not
   caches like Redis.

2. **Sealed `Filter` AST (composable predicate values).** Callers build a tree of
   typed predicate values via constructor functions. The `Filter` interface has an
   unexported method, so backends cannot add node types — they can only *translate* the
   closed set, which lets each repository do an exhaustive type switch with no silent
   gaps.

3. **`List` lives on the base `EntityRepository`.** Querying is not an optional
   capability interface — all realistic entity backends (Postgres, Mongo, Dynamo) are
   queryable databases. `Get` (by ID) is kept as its own method, distinct from `List`,
   because by-ID is a *privileged key-access path* in every backend (Dynamo `GetItem`,
   PK index in Postgres/Mongo) versus the scan/filter path that `List` represents.

4. **Naming: `Get` / `List` / `Save`.** `Load` is renamed to `Get`. `Save` is unchanged
   (upsert with optimistic version check). `List` is the new filtered read.

## API

### Filter AST (`filter.go`, package `ember`)

```go
// Filter is a sealed sum type: only ember-defined nodes satisfy it.
type Filter interface {
    isFilter()
}

type Operator int

const (
    OpEq Operator = iota
    OpNe
    OpGt
    OpGte
    OpLt
    OpLte
)

// Leaf predicates. Path is a serialized field path: a reserved metadata path
// ("id", "type", "version") or a dot-delimited path into the data blob
// ("address.city").
type Comparison struct {
    Path  string
    Op    Operator
    Value any
}
type Membership struct { // IN
    Path   string
    Values []any
}
type Existence struct {
    Path   string
    Exists bool
}

// Composites
type Conjunction struct { Filters []Filter } // AND
type Disjunction struct { Filters []Filter } // OR
type Negation    struct { Filter Filter }    // NOT
```

Constructors form the ergonomic surface:

```go
ember.Eq(path string, v any) Filter
ember.Ne(path string, v any) Filter
ember.Gt(path string, v any) Filter
ember.Gte(path string, v any) Filter
ember.Lt(path string, v any) Filter
ember.Lte(path string, v any) Filter
ember.In(path string, vs ...any) Filter
ember.Exists(path string, exists bool) Filter
ember.And(fs ...Filter) Filter
ember.Or(fs ...Filter) Filter
ember.Not(f Filter) Filter
```

Example:

```go
f := ember.And(
    ember.Eq("status", "open"),
    ember.Gt("total", 4200),
    ember.Or(ember.Eq("region", "EU"), ember.Eq("region", "UK")),
)
orders, err := store.List(ctx, f)
```

### Repository interface (`entity.go`)

```go
type EntityRepository interface {
    Save(ctx context.Context, m *MarshaledEntity) error
    Get(ctx context.Context, typ, id string) (*MarshaledEntity, error)
    List(ctx context.Context, typ string, f Filter) ([]*MarshaledEntity, error)
}
```

`Load` is renamed to `Get`. `List` is added.

### EntityStore methods

```go
func (s *EntityStore[E]) Get(ctx context.Context, id string) (E, error)
func (s *EntityStore[E]) Save(ctx context.Context, e E) error
func (s *EntityStore[E]) List(ctx context.Context, f Filter) ([]E, error)
```

`List` scopes to the entity type (`empty.Type()`), calls the repository, then hydrates
each `*MarshaledEntity` through the existing marshaler:

```go
func (s *EntityStore[E]) List(ctx context.Context, f Filter) ([]E, error) {
    var empty E
    ms, err := s.repository.List(ctx, empty.Type(), f)
    if err != nil {
        return nil, err
    }
    out := make([]E, 0, len(ms))
    for _, m := range ms {
        e, err := s.marshaler.Unmarshal(ctx, m)
        if err != nil {
            return nil, err
        }
        out = append(out, e)
    }
    return out, nil
}
```

## Backend translation

### Path namespace

Each backend splits `Path` into two worlds:

- **Reserved metadata paths** — `id`, `type`, `version` → the top-level
  columns/fields the store already maintains.
- **Everything else** → a path into the `data` blob; dot segments address nested
  fields.

| | reserved `id` | reserved `version` | data path `address.city` |
|---|---|---|---|
| **Postgres** (jsonb `data`) | `id` column | `version` column | `data#>>'{address,city}'` |
| **Mongo** | `_id` | `version` | `data.address.city` |
| **Dynamo** | partition key | `version` attr | `data.address.city` |

`List` is already scoped to one `typ`, so the `type` reserved path is rarely needed but
available.

### Translation strategy

Each repository walks the sealed `Filter` via an exhaustive type switch and emits native
query fragments. Because the AST is closed, the compiler forces every node type to be
handled.

- **Postgres** → `WHERE` clause with bind params. `Gt` on a data path →
  `(data#>>'{total}')::numeric > $1`; `Membership` → `… = ANY($1)`;
  `Conjunction`/`Disjunction` → `AND`/`OR` groups; `Negation` → `NOT (...)`;
  `Existence` → `data ? 'key'` / `IS NULL`. Requires the `data` column typed as
  `jsonb`.
- **Mongo** → `bson.D`. `Eq` → `{path: v}`; `Gt` → `{path: {$gt: v}}`;
  `In` → `{path: {$in: […]}}`; `And`/`Or` → `$and`/`$or`; `Not` → `$not`/`$nor`;
  `Existence` → `{path: {$exists: b}}`.
- **Dynamo** → `FilterExpression` string plus `ExpressionAttributeNames`/`Values`, run
  as a `Scan` (or a `Query` when the filter pins the partition key). Comparisons, `IN`,
  `attribute_exists`, `AND`/`OR`/`NOT` map directly.

### Unsupported operators

Even among real databases, capability differs (an operator on a given path, or a value
type a backend cannot bind). The repository returns a wrapped sentinel:

```go
var ErrUnsupportedFilter = errors.New("ember: unsupported filter")
// e.g. fmt.Errorf("%w: operator %q on path %q", ErrUnsupportedFilter, op, path)
```

Callers get a typed, checkable failure rather than a malformed query.

### Smaller rules

- **Match-all:** `List(ctx, nil)` — a `nil` `Filter` — lists every entity of the type.
  No special constructor.
- **Value types:** filter values are restricted to a documented set — strings,
  ints/floats, bools, and `time.Time` — so each backend has a well-defined binding.
  Anything else → `ErrUnsupportedFilter`.

## Extensibility (deferred work)

`List` is shaped to accept variadic query options later without breaking callers or the
interface:

```go
List(ctx context.Context, typ string, f Filter, opts ...QueryOption) (...)
```

so `ember.SortBy(...)`, `ember.Limit(n)`, `ember.After(cursor)` slot in additively. Not
built now.

## Affected files

- `entity.go` — rename `Load` → `Get`; add `List` to `EntityRepository`; add
  `EntityStore.List`.
- `filter.go` (new) — `Filter` sum type, constructors, `Operator`,
  `ErrUnsupportedFilter`.
- `postgres/entity_repository.go` — rename `Load` → `Get`; implement `List` (jsonb
  translation); `data` column must be `jsonb`.
- `mongo/entity_repository.go` — rename `Load` → `Get`; implement `List` (BSON
  translation).
- `dynamo/entity_repository.go` (new, if/when a Dynamo backend is added) — `Get`
  via `GetItem`, `List` via `Scan`/`Query` + `FilterExpression`.
