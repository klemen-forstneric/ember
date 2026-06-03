# DynamoDB `EntityRepository` — Design

**Date:** 2026-06-03
**Status:** Approved

## Goal

Add a DynamoDB-backed implementation of `ember.EntityRepository`, mirroring the
existing `mongo/` and `postgres/` packages. It must satisfy the same interface
and preserve the same observable semantics — including optimistic-concurrency
behavior and the ability to filter on nested `data` paths.

## Interface to satisfy

From `entity.go:50-54`:

```go
type EntityRepository interface {
    Save(ctx context.Context, m *MarshaledEntity) error
    Get(ctx context.Context, typ, id string) (*MarshaledEntity, error)
    List(ctx context.Context, typ string, f Filter) ([]*MarshaledEntity, error)
}
```

## Package layout

New `dynamo/` package (named `dynamo`, not `dynamodb`, to match `mongo`),
sibling to `mongo/` and `postgres/`:

```
dynamo/
  entity_repository.go   // EntityRepository: NewEntityRepository, Save, Get, List
  filter.go              // buildFilter: ember.Filter -> expression.ConditionBuilder
  filter_test.go         // unit tests for filter translation + interface assertion
```

## Dependencies

`github.com/aws/aws-sdk-go-v2` core is already a direct dependency. Add:

- `github.com/aws/aws-sdk-go-v2/service/dynamodb`
- `github.com/aws/aws-sdk-go-v2/service/dynamodb/types`
- `github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression`

The caller constructs and owns the `*dynamodb.Client` (matching how `mongo`
takes a `*mongo.Collection` and `postgres` takes a `*sql.DB`).

## Constructor & type

```go
type EntityRepository struct {
    client *dynamodb.Client
    table  string
}

func NewEntityRepository(client *dynamodb.Client, table string) *EntityRepository
```

## Table schema (caller-owned)

Fixed key schema. The caller creates the table; the repository assumes:

| attribute | DynamoDB type | role           | source                        |
|-----------|---------------|----------------|-------------------------------|
| `type`    | S             | partition key  | `MarshaledEntity.Type`        |
| `id`      | S             | sort key       | `MarshaledEntity.ID`          |
| `version` | N             | attribute      | `MarshaledEntity.Version.Value()` |
| `data`    | M (map)       | attribute      | `json.Unmarshal(m.Data)` → AttributeValue map |

This (PK=`type`, SK=`id`) makes `Get` a `GetItem` and `List` an efficient
`Query` on the partition key, rather than a table `Scan`.

### `data` storage — document map (full filter parity)

`MarshaledEntity.Data` is `[]byte` of JSON. On `Save` we decode it into a Go
value and convert it to a DynamoDB document map (`M`) so that
`FilterExpression` can reach into nested paths (parity with Mongo's nested BSON
and Postgres's JSONB). On `Get`/`List` we convert the map back and
`json.Marshal` it to repopulate `MarshaledEntity.Data`.

**Number precision.** Decode with a `json.Decoder` configured via
`UseNumber()`, so JSON numbers become `json.Number` (an exact decimal string)
rather than `float64`. Store them in DynamoDB's native numeric type (`N`), which
holds up to 38 significant digits — covering the full `int64`/`uint64` range.
This preserves large integers (e.g. snowflake IDs, counters) exactly; the naive
`interface{}` decode would silently corrupt integers above 2^53.

Known limitation (accepted): object key ordering is not preserved across the
round-trip. This is semantically irrelevant — the `EntityMarshaler` matches by
field name — and matches the existing backends (Postgres JSONB and Mongo
ExtJSON both reorder keys).

## Methods

### Save — single conditional `PutItem`

Matches the Mongo/Postgres model exactly: **no branch on insert-vs-update**.
A single atomic conditional write keyed on the *expected initial version*.

Condition:

```go
cond := expression.AttributeNotExists(expression.Name("type")).
    Or(expression.Name("version").Equal(expression.Value(m.Version.Initial())))
```

- Item absent → `attribute_not_exists` passes → insert (new entity).
- Item present with `version == Initial()` → second clause passes → overwrite
  (normal update; also the `Initial()==0` overwrite case, which Mongo's
  `ReplaceOne` filter and Postgres's `ON CONFLICT … WHERE version = Initial()`
  both also allow).
- Item present with a different version → both clauses fail →
  `ConditionalCheckFailedException` → return `ember.ErrVersionConflict`.

`PutItem` writes the full item (`type`, `id`, `version = Value()`, `data`),
replacing the prior item — equivalent to Mongo's `ReplaceOne` and Postgres's
`DO UPDATE SET version = …, data = …`.

Error mapping: detect `ConditionalCheckFailedException` via
`errors.As` against `*types.ConditionalCheckFailedException` →
`ember.ErrVersionConflict`. Other errors propagate as-is.

### Get — `GetItem(type, id)`

Empty `Item` in the response → `ember.ErrEntityNotFound`. Otherwise decode
`version`, convert the `data` map back to JSON bytes, and return a
`*MarshaledEntity` with `Version: ember.NewVersion(version)`.

### List — `Query` on partition key

`Query` with `KeyConditionExpression` on `type = :typ`. If the filter is
non-nil, attach the `buildFilter` result as the `FilterExpression` (combined in
the same `expression.Builder`). Paginate by following `LastEvaluatedKey` until
empty. Each returned item's `data` map is converted back to JSON bytes. Returns
`[]*MarshaledEntity` (nil slice when no matches, matching the others).

## Filter translation (`buildFilter`)

Recursive walk over the sealed `Filter` sum type (`filter.go` in the root
package), producing an `expression.ConditionBuilder`:

- **Path mapping:** reserved paths `type` / `id` / `version` → top-level
  attribute names; any other path → nested document path under `data`
  (e.g. `address.city` → `expression.Name("data.address.city")`).
- **Operators:** `OpEq`/`OpNe`/`OpGt`/`OpGte`/`OpLt`/`OpLte` →
  `Equal`/`NotEqual`/`GreaterThan`/`GreaterThanEqual`/`LessThan`/`LessThanEqual`.
- **Membership** (`In`) → `expression` `In`.
- **Existence** (`Exists(path, true/false)`) → `AttributeExists` /
  `AttributeNotExists`.
- **Boolean** (`And`/`Or`/`Not`) → `And` / `Or` / `Not`.
- **Time normalization:** `time.Time` values normalized to RFC3339Nano strings,
  matching Mongo/Postgres.
- **Unsupported value types** → `ember.ErrUnsupportedFilter`.
- Reserved-word collisions are handled automatically by the `expression`
  builder via `ExpressionAttributeNames`.

## Error handling

Reuses the shared sentinels from the root package: `ErrEntityNotFound`,
`ErrVersionConflict`, `ErrUnsupportedFilter`.

## Testing

Unit-only, like `mongo/filter_test.go` and `postgres/filter_test.go` — no live
DynamoDB and no containers. Table-driven tests over `buildFilter`, asserting the
built expression's condition string plus its `ExpressionAttributeNames` and
`ExpressionAttributeValues`:

- each operator and node type (comparison, membership, existence, conjunction,
  disjunction, negation);
- reserved-path vs nested-`data`-path mapping;
- `time.Time` normalization to RFC3339Nano;
- unsupported value type → `ErrUnsupportedFilter`;
- nil/empty filter behavior;
- compile-time assertion: `var _ ember.EntityRepository = (*EntityRepository)(nil)`.

The `data` JSON↔AttributeValue-map conversion helpers are pure functions and
are unit-tested directly (no DynamoDB needed), including a round-trip case for
an integer above 2^53 to confirm `UseNumber()` + `N` storage preserves it.

## Out of scope

- Table creation / provisioning (caller-owned, as with the other backends).
- Integration tests against real DynamoDB / DynamoDB Local.
- Global secondary indexes or alternative access patterns beyond Get/List.
