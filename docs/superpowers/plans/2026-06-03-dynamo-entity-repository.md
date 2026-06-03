# DynamoDB EntityRepository Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `dynamo` package implementing `ember.EntityRepository` (Save/Get/List) backed by Amazon DynamoDB, mirroring the existing `mongo` and `postgres` packages.

**Architecture:** A single-table repository keyed by partition `type` + sort `id`. `Save` is one conditional `PutItem` (optimistic concurrency on `version`); `Get` is `GetItem`; `List` is a `Query` on the partition key with the ember `Filter` translated to a `FilterExpression`. Entity JSON `data` is stored as a DynamoDB document map via hand-rolled converters that preserve integer precision (`json.Number` ↔ `N`).

**Tech Stack:** Go, `aws-sdk-go-v2` (`service/dynamodb`, `service/dynamodb/types`, `feature/dynamodb/expression`). No `attributevalue` package (would lose integer precision). Tests are unit-only — no live DynamoDB, no containers — matching the sibling packages.

**Spec:** `docs/superpowers/specs/2026-06-03-dynamo-entity-repository-design.md`

---

## File Structure

- `dynamo/entity_repository.go` — `EntityRepository` struct, `NewEntityRepository`, `Save`, `Get`, `List`, `itemToEntity` helper.
- `dynamo/data.go` — JSON↔AttributeValue converters: `marshalData`, `unmarshalData`, `toAttributeValue`, `fromAttributeValue`.
- `dynamo/filter.go` — `buildFilter` + node translation helpers.
- `dynamo/data_test.go` — converter round-trip unit tests (incl. large-integer precision).
- `dynamo/filter_test.go` — filter translation table tests + `var _ ember.EntityRepository` compile assertion.

Reference, do not modify: `entity.go` (interface, `MarshaledEntity`, error sentinels), `filter.go` (the `Filter` sum type and null/missing contract), `version.go`, and the sibling `mongo/` / `postgres/` packages for style.

---

## Task 1: Scaffold package, dependencies, and interface assertion

**Files:**
- Create: `dynamo/entity_repository.go`
- Create: `dynamo/filter_test.go`
- Modify: `go.mod`, `go.sum` (via `go get`)

- [ ] **Step 1: Add the AWS SDK dependencies**

Run:
```bash
cd /Users/klemen/projects/ember
go get github.com/aws/aws-sdk-go-v2/service/dynamodb
go get github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression
```
Expected: `go.mod` gains `github.com/aws/aws-sdk-go-v2/service/dynamodb` and `.../feature/dynamodb/expression` as direct requirements. `service/dynamodb/types` is part of the `service/dynamodb` module (no separate `go get`). `github.com/aws/aws-sdk-go-v2` core is already present.

- [ ] **Step 2: Write the failing interface assertion test**

Create `dynamo/filter_test.go`:
```go
package dynamo

import (
	"github.com/klemen-forstneric/ember"
)

// Compile-time assertion that the repository satisfies the interface.
var _ ember.EntityRepository = (*EntityRepository)(nil)
```

- [ ] **Step 3: Run it to verify it fails (does not compile)**

Run: `go build ./dynamo/`
Expected: FAIL — `undefined: EntityRepository`.

- [ ] **Step 4: Write the struct, constructor, and method stubs**

Create `dynamo/entity_repository.go`:
```go
package dynamo

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/klemen-forstneric/ember"
)

// EntityRepository persists MarshaledEntity values in a single DynamoDB table
// keyed by partition attribute "type" and sort attribute "id".
type EntityRepository struct {
	client *dynamodb.Client
	table  string
}

// NewEntityRepository returns a repository backed by the given client and table.
// The caller owns table creation: partition key "type" (S), sort key "id" (S).
func NewEntityRepository(client *dynamodb.Client, table string) *EntityRepository {
	return &EntityRepository{client: client, table: table}
}

func (r *EntityRepository) Save(ctx context.Context, m *ember.MarshaledEntity) error {
	return errors.New("not implemented")
}

func (r *EntityRepository) Get(ctx context.Context, typ, id string) (*ember.MarshaledEntity, error) {
	return nil, errors.New("not implemented")
}

func (r *EntityRepository) List(ctx context.Context, typ string, f ember.Filter) ([]*ember.MarshaledEntity, error) {
	return nil, errors.New("not implemented")
}
```

- [ ] **Step 5: Run build to verify it passes**

Run: `go build ./dynamo/ && go vet ./dynamo/`
Expected: PASS (no output).

- [ ] **Step 6: Commit**

```bash
git add dynamo/entity_repository.go dynamo/filter_test.go go.mod go.sum
git commit -m "feat(dynamo): scaffold EntityRepository and add aws sdk deps"
```

---

## Task 2: Data converters (JSON ↔ AttributeValue) with precision

**Files:**
- Create: `dynamo/data.go`
- Create: `dynamo/data_test.go`

The converters are pure functions. `marshalData` decodes JSON with
`Decoder.UseNumber()` so numbers become `json.Number` (exact), then recursively
builds an AttributeValue map. `unmarshalData` reverses it, mapping `N` back to
`json.Number` so `json.Marshal` re-emits the exact numeric literal. This is the
heart of the integer-precision and filter-parity goals.

- [ ] **Step 1: Write the failing converter tests**

Create `dynamo/data_test.go`:
```go
package dynamo

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestMarshalData(t *testing.T) {
	in := []byte(`{"status":"open","total":100,"active":true,"missing":null,"addr":{"city":"NYC"},"tags":["a","b"]}`)
	got, err := marshalData(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[string]types.AttributeValue{
		"status":  &types.AttributeValueMemberS{Value: "open"},
		"total":   &types.AttributeValueMemberN{Value: "100"},
		"active":  &types.AttributeValueMemberBOOL{Value: true},
		"missing": &types.AttributeValueMemberNULL{Value: true},
		"addr": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"city": &types.AttributeValueMemberS{Value: "NYC"},
		}},
		"tags": &types.AttributeValueMemberL{Value: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "a"},
			&types.AttributeValueMemberS{Value: "b"},
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestMarshalDataEmpty(t *testing.T) {
	got, err := marshalData(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %#v, want empty map", got)
	}
}

// Round-trip must preserve an integer larger than 2^53, which a float64 decode
// would corrupt.
func TestDataRoundTripLargeInteger(t *testing.T) {
	in := []byte(`{"id":9007199254740993}`)
	av, err := marshalData(in)
	if err != nil {
		t.Fatalf("marshalData: %v", err)
	}
	n, ok := av["id"].(*types.AttributeValueMemberN)
	if !ok || n.Value != "9007199254740993" {
		t.Fatalf("got %#v, want N 9007199254740993", av["id"])
	}
	out, err := unmarshalData(av)
	if err != nil {
		t.Fatalf("unmarshalData: %v", err)
	}
	if string(out) != `{"id":9007199254740993}` {
		t.Errorf("round-trip got %s, want {\"id\":9007199254740993}", out)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./dynamo/ -run 'TestMarshalData|TestDataRoundTrip' -v`
Expected: FAIL — `undefined: marshalData` / `undefined: unmarshalData`.

- [ ] **Step 3: Implement the converters**

Create `dynamo/data.go`:
```go
package dynamo

import (
	"bytes"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// marshalData decodes the entity's JSON data into a DynamoDB document map.
// Numbers are decoded with UseNumber so large integers are not rounded through
// float64; they are stored in the native numeric type (N).
func marshalData(b []byte) (map[string]types.AttributeValue, error) {
	if len(b) == 0 {
		return map[string]types.AttributeValue{}, nil
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return nil, err
	}
	out := make(map[string]types.AttributeValue, len(raw))
	for k, v := range raw {
		out[k] = toAttributeValue(v)
	}
	return out, nil
}

func toAttributeValue(v any) types.AttributeValue {
	switch x := v.(type) {
	case nil:
		return &types.AttributeValueMemberNULL{Value: true}
	case bool:
		return &types.AttributeValueMemberBOOL{Value: x}
	case string:
		return &types.AttributeValueMemberS{Value: x}
	case json.Number:
		return &types.AttributeValueMemberN{Value: x.String()}
	case map[string]any:
		m := make(map[string]types.AttributeValue, len(x))
		for k, e := range x {
			m[k] = toAttributeValue(e)
		}
		return &types.AttributeValueMemberM{Value: m}
	case []any:
		l := make([]types.AttributeValue, len(x))
		for i, e := range x {
			l[i] = toAttributeValue(e)
		}
		return &types.AttributeValueMemberL{Value: l}
	default:
		// With UseNumber, json decoding yields only the cases above; treat any
		// unexpected value as null rather than panicking.
		return &types.AttributeValueMemberNULL{Value: true}
	}
}

// unmarshalData converts a DynamoDB document map back into JSON bytes. Numeric
// attributes become json.Number so json.Marshal re-emits the exact literal.
func unmarshalData(m map[string]types.AttributeValue) ([]byte, error) {
	raw := make(map[string]any, len(m))
	for k, v := range m {
		raw[k] = fromAttributeValue(v)
	}
	return json.Marshal(raw)
}

func fromAttributeValue(v types.AttributeValue) any {
	switch x := v.(type) {
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberBOOL:
		return x.Value
	case *types.AttributeValueMemberS:
		return x.Value
	case *types.AttributeValueMemberN:
		return json.Number(x.Value)
	case *types.AttributeValueMemberM:
		m := make(map[string]any, len(x.Value))
		for k, e := range x.Value {
			m[k] = fromAttributeValue(e)
		}
		return m
	case *types.AttributeValueMemberL:
		l := make([]any, len(x.Value))
		for i, e := range x.Value {
			l[i] = fromAttributeValue(e)
		}
		return l
	default:
		// Only the types produced by marshalData are ever read back.
		return nil
	}
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./dynamo/ -run 'TestMarshalData|TestDataRoundTrip' -v`
Expected: PASS for `TestMarshalData`, `TestMarshalDataEmpty`, `TestDataRoundTripLargeInteger`.

- [ ] **Step 5: Commit**

```bash
git add dynamo/data.go dynamo/data_test.go
git commit -m "feat(dynamo): JSON<->AttributeValue converters with integer precision"
```

---

## Task 3: Filter translation (`buildFilter`)

**Files:**
- Create: `dynamo/filter.go`
- Modify: `dynamo/filter_test.go`

`buildFilter` translates the sealed `Filter` sum type into an
`expression.ConditionBuilder` for use as a `FilterExpression`, honoring the
two-valued null/missing contract in root `filter.go:24-30`. Tests build each
filter into a full expression and assert on `expr.Filter()` (string),
`expr.Names()`, and `expr.Values()` — all deterministic for a given filter.

> Note on expected strings: `expr.Filter()` formatting (spacing, parentheses)
> is defined by the SDK's `expression` package. The expected strings below match
> its documented style. If a red run shows a cosmetic mismatch (e.g. a space
> before `(`), update the expected string to the exact emitted text — the
> `Names`/`Values` assertions remain the semantic check.

- [ ] **Step 1: Write the failing filter tests**

Replace the contents of `dynamo/filter_test.go` with:
```go
package dynamo

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/klemen-forstneric/ember"
)

// build is a test helper: it translates a non-nil filter and renders it as a
// standalone FilterExpression, returning the expression string, names, values.
func build(t *testing.T, f ember.Filter) (string, map[string]string, map[string]types.AttributeValue) {
	t.Helper()
	cb, ok, err := buildFilter(f)
	if err != nil {
		t.Fatalf("buildFilter: %v", err)
	}
	if !ok {
		t.Fatalf("buildFilter: expected a filter, got none")
	}
	expr, err := expression.NewBuilder().WithFilter(cb).Build()
	if err != nil {
		t.Fatalf("build expression: %v", err)
	}
	return aws.ToString(expr.Filter()), expr.Names(), expr.Values()
}

func nN(s string) types.AttributeValue { return &types.AttributeValueMemberN{Value: s} }
func sS(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }

func TestBuildFilterNilMatchesAll(t *testing.T) {
	cb, ok, err := buildFilter(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Errorf("nil filter should report no filter, got %#v", cb)
	}
}

func TestBuildFilterComparison(t *testing.T) {
	tests := []struct {
		name      string
		filter    ember.Filter
		wantExpr  string
		wantNames map[string]string
		wantVals  map[string]types.AttributeValue
	}{
		{
			"eq data path",
			ember.Eq("status", "open"),
			"#0.#1 = :0",
			map[string]string{"#0": "data", "#1": "status"},
			map[string]types.AttributeValue{":0": sS("open")},
		},
		{
			"nested data path",
			ember.Eq("address.city", "NYC"),
			"#0.#1.#2 = :0",
			map[string]string{"#0": "data", "#1": "address", "#2": "city"},
			map[string]types.AttributeValue{":0": sS("NYC")},
		},
		{
			"reserved version gt",
			ember.Gt("version", 5),
			"#0 > :0",
			map[string]string{"#0": "version"},
			map[string]types.AttributeValue{":0": nN("5")},
		},
		{
			"gte",
			ember.Gte("total", 100),
			"#0.#1 >= :0",
			map[string]string{"#0": "data", "#1": "total"},
			map[string]types.AttributeValue{":0": nN("100")},
		},
		{
			"lt",
			ember.Lt("total", 100),
			"#0.#1 < :0",
			map[string]string{"#0": "data", "#1": "total"},
			map[string]types.AttributeValue{":0": nN("100")},
		},
		{
			"lte",
			ember.Lte("total", 100),
			"#0.#1 <= :0",
			map[string]string{"#0": "data", "#1": "total"},
			map[string]types.AttributeValue{":0": nN("100")},
		},
		{
			"time normalized",
			ember.Eq("createdAt", time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)),
			"#0.#1 = :0",
			map[string]string{"#0": "data", "#1": "createdAt"},
			map[string]types.AttributeValue{":0": sS("2024-01-02T03:04:05Z")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotExpr, gotNames, gotVals := build(t, tt.filter)
			if gotExpr != tt.wantExpr {
				t.Errorf("expr: got %q, want %q", gotExpr, tt.wantExpr)
			}
			if !reflect.DeepEqual(gotNames, tt.wantNames) {
				t.Errorf("names: got %#v, want %#v", gotNames, tt.wantNames)
			}
			if !reflect.DeepEqual(gotVals, tt.wantVals) {
				t.Errorf("values: got %#v, want %#v", gotVals, tt.wantVals)
			}
		})
	}
}

func TestBuildFilterMembership(t *testing.T) {
	gotExpr, gotNames, gotVals := build(t, ember.In("region", "EU", "UK"))
	if gotExpr != "#0.#1 IN (:0, :1)" {
		t.Errorf("expr: got %q", gotExpr)
	}
	if !reflect.DeepEqual(gotNames, map[string]string{"#0": "data", "#1": "region"}) {
		t.Errorf("names: got %#v", gotNames)
	}
	if !reflect.DeepEqual(gotVals, map[string]types.AttributeValue{":0": sS("EU"), ":1": sS("UK")}) {
		t.Errorf("values: got %#v", gotVals)
	}
}

func TestBuildFilterEmptyMembershipMatchesNone(t *testing.T) {
	// Empty IN is always-false: attribute_not_exists(version).
	gotExpr, gotNames, _ := build(t, ember.In("region"))
	if gotExpr != "attribute_not_exists (#0)" {
		t.Errorf("expr: got %q", gotExpr)
	}
	if !reflect.DeepEqual(gotNames, map[string]string{"#0": "version"}) {
		t.Errorf("names: got %#v", gotNames)
	}
}

func TestBuildFilterNeGuarded(t *testing.T) {
	// Ne on a data path is the two-valued complement: present, non-null, and !=.
	gotExpr, gotNames, gotVals := build(t, ember.Ne("status", "open"))
	want := "(attribute_exists (#0.#1)) AND (NOT (attribute_type (#0.#1, :0))) AND (#0.#1 <> :1)"
	if gotExpr != want {
		t.Errorf("expr: got %q, want %q", gotExpr, want)
	}
	if !reflect.DeepEqual(gotNames, map[string]string{"#0": "data", "#1": "status"}) {
		t.Errorf("names: got %#v", gotNames)
	}
	if !reflect.DeepEqual(gotVals, map[string]types.AttributeValue{":0": sS("NULL"), ":1": sS("open")}) {
		t.Errorf("values: got %#v", gotVals)
	}
}

func TestBuildFilterNeReservedVersionUnguarded(t *testing.T) {
	gotExpr, gotNames, gotVals := build(t, ember.Ne("version", 3))
	if gotExpr != "#0 <> :0" {
		t.Errorf("expr: got %q", gotExpr)
	}
	if !reflect.DeepEqual(gotNames, map[string]string{"#0": "version"}) {
		t.Errorf("names: got %#v", gotNames)
	}
	if !reflect.DeepEqual(gotVals, map[string]types.AttributeValue{":0": nN("3")}) {
		t.Errorf("values: got %#v", gotVals)
	}
}

func TestBuildFilterExistence(t *testing.T) {
	t.Run("true on data path", func(t *testing.T) {
		gotExpr, gotNames, gotVals := build(t, ember.Exists("status", true))
		want := "(attribute_exists (#0.#1)) AND (NOT (attribute_type (#0.#1, :0)))"
		if gotExpr != want {
			t.Errorf("expr: got %q, want %q", gotExpr, want)
		}
		if !reflect.DeepEqual(gotNames, map[string]string{"#0": "data", "#1": "status"}) {
			t.Errorf("names: got %#v", gotNames)
		}
		if !reflect.DeepEqual(gotVals, map[string]types.AttributeValue{":0": sS("NULL")}) {
			t.Errorf("values: got %#v", gotVals)
		}
	})
	t.Run("false on data path", func(t *testing.T) {
		gotExpr, _, gotVals := build(t, ember.Exists("status", false))
		want := "(attribute_not_exists (#0.#1)) OR (attribute_type (#0.#1, :0))"
		if gotExpr != want {
			t.Errorf("expr: got %q, want %q", gotExpr, want)
		}
		if !reflect.DeepEqual(gotVals, map[string]types.AttributeValue{":0": sS("NULL")}) {
			t.Errorf("values: got %#v", gotVals)
		}
	})
}

func TestBuildFilterBoolean(t *testing.T) {
	t.Run("and", func(t *testing.T) {
		gotExpr, _, _ := build(t, ember.And(ember.Eq("a", "1"), ember.Eq("b", "2")))
		if gotExpr != "(#0.#1 = :0) AND (#0.#2 = :1)" {
			t.Errorf("expr: got %q", gotExpr)
		}
	})
	t.Run("or", func(t *testing.T) {
		gotExpr, _, _ := build(t, ember.Or(ember.Eq("a", "1"), ember.Eq("b", "2")))
		if gotExpr != "(#0.#1 = :0) OR (#0.#2 = :1)" {
			t.Errorf("expr: got %q", gotExpr)
		}
	})
	t.Run("not", func(t *testing.T) {
		gotExpr, _, _ := build(t, ember.Not(ember.Eq("status", "open")))
		if gotExpr != "NOT (#0.#1 = :0)" {
			t.Errorf("expr: got %q", gotExpr)
		}
	})
	t.Run("single-child and collapses", func(t *testing.T) {
		gotExpr, _, _ := build(t, ember.And(ember.Eq("a", "1")))
		if gotExpr != "#0.#1 = :0" {
			t.Errorf("expr: got %q", gotExpr)
		}
	})
	t.Run("empty and matches all", func(t *testing.T) {
		gotExpr, gotNames, _ := build(t, ember.And())
		if gotExpr != "attribute_exists (#0)" || !reflect.DeepEqual(gotNames, map[string]string{"#0": "version"}) {
			t.Errorf("expr: got %q names %#v", gotExpr, gotNames)
		}
	})
	t.Run("empty or matches none", func(t *testing.T) {
		gotExpr, gotNames, _ := build(t, ember.Or())
		if gotExpr != "attribute_not_exists (#0)" || !reflect.DeepEqual(gotNames, map[string]string{"#0": "version"}) {
			t.Errorf("expr: got %q names %#v", gotExpr, gotNames)
		}
	})
}

func TestBuildFilterUnsupportedValue(t *testing.T) {
	_, _, err := buildFilter(ember.Eq("status", []string{"nope"}))
	if !errors.Is(err, ember.ErrUnsupportedFilter) {
		t.Errorf("got %v, want ErrUnsupportedFilter", err)
	}
}

func TestBuildFilterRejectsKeyAttribute(t *testing.T) {
	for _, path := range []string{"id", "type"} {
		_, _, err := buildFilter(ember.Eq(path, "x"))
		if !errors.Is(err, ember.ErrUnsupportedFilter) {
			t.Errorf("path %q: got %v, want ErrUnsupportedFilter", path, err)
		}
	}
}

// Compile-time assertion that the repository satisfies the interface.
var _ ember.EntityRepository = (*EntityRepository)(nil)
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./dynamo/ -run TestBuildFilter -v`
Expected: FAIL — `undefined: buildFilter`.

- [ ] **Step 3: Implement the filter translation**

Create `dynamo/filter.go`:
```go
package dynamo

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"

	"github.com/klemen-forstneric/ember"
)

// buildFilter translates a Filter into a DynamoDB FilterExpression condition.
// A nil filter returns ok=false, signalling the caller to omit the filter
// (match all). A non-nil filter returns ok=true with the condition.
func buildFilter(f ember.Filter) (expression.ConditionBuilder, bool, error) {
	if f == nil {
		return expression.ConditionBuilder{}, false, nil
	}
	cb, err := node(f)
	if err != nil {
		return expression.ConditionBuilder{}, false, err
	}
	return cb, true, nil
}

func node(f ember.Filter) (expression.ConditionBuilder, error) {
	switch n := f.(type) {
	case ember.Comparison:
		return comparison(n)
	case ember.Membership:
		return membership(n)
	case ember.Existence:
		return existence(n)
	case ember.Conjunction:
		return composite(n.Filters, true)
	case ember.Disjunction:
		return composite(n.Filters, false)
	case ember.Negation:
		inner, err := node(n.Filter)
		if err != nil {
			return expression.ConditionBuilder{}, err
		}
		return expression.Not(inner), nil
	default:
		return expression.ConditionBuilder{}, fmt.Errorf("%w: unknown node %T", ember.ErrUnsupportedFilter, f)
	}
}

func comparison(c ember.Comparison) (expression.ConditionBuilder, error) {
	name, reserved, err := attrName(c.Path)
	if err != nil {
		return expression.ConditionBuilder{}, err
	}
	v, err := normalizeValue(c.Value)
	if err != nil {
		return expression.ConditionBuilder{}, err
	}
	val := expression.Value(v)
	switch c.Op {
	case ember.OpEq:
		return name.Equal(val), nil
	case ember.OpNe:
		// Two-valued complement of Eq: present, non-null, and != v. A missing or
		// NULL-typed attribute must not match. Reserved paths are always present
		// and non-null, so they need no guard.
		if reserved {
			return name.NotEqual(val), nil
		}
		return expression.And(
			name.AttributeExists(),
			expression.Not(name.AttributeType(expression.Null)),
			name.NotEqual(val),
		), nil
	case ember.OpGt:
		return name.GreaterThan(val), nil
	case ember.OpGte:
		return name.GreaterThanEqual(val), nil
	case ember.OpLt:
		return name.LessThan(val), nil
	case ember.OpLte:
		return name.LessThanEqual(val), nil
	default:
		return expression.ConditionBuilder{}, fmt.Errorf("%w: operator %d", ember.ErrUnsupportedFilter, c.Op)
	}
}

func membership(m ember.Membership) (expression.ConditionBuilder, error) {
	name, _, err := attrName(m.Path)
	if err != nil {
		return expression.ConditionBuilder{}, err
	}
	if len(m.Values) == 0 {
		return alwaysFalse(), nil // empty IN matches nothing
	}
	ops := make([]expression.OperandBuilder, 0, len(m.Values))
	for _, raw := range m.Values {
		v, err := normalizeValue(raw)
		if err != nil {
			return expression.ConditionBuilder{}, err
		}
		ops = append(ops, expression.Value(v))
	}
	// A missing or NULL-typed attribute matches no string/number operand, so IN
	// is naturally the two-valued positive predicate; no guard needed.
	return name.In(ops[0], ops[1:]...), nil
}

func existence(e ember.Existence) (expression.ConditionBuilder, error) {
	name, reserved, err := attrName(e.Path)
	if err != nil {
		return expression.ConditionBuilder{}, err
	}
	if reserved {
		// version is always present and non-null.
		if e.Exists {
			return name.AttributeExists(), nil
		}
		return name.AttributeNotExists(), nil
	}
	if e.Exists {
		// present and non-null
		return expression.And(name.AttributeExists(), expression.Not(name.AttributeType(expression.Null))), nil
	}
	// absent or null
	return expression.Or(name.AttributeNotExists(), name.AttributeType(expression.Null)), nil
}

func composite(fs []ember.Filter, and bool) (expression.ConditionBuilder, error) {
	if len(fs) == 0 {
		if and {
			return alwaysTrue(), nil // empty AND matches all
		}
		return alwaysFalse(), nil // empty OR matches none
	}
	parts := make([]expression.ConditionBuilder, 0, len(fs))
	for _, f := range fs {
		p, err := node(f)
		if err != nil {
			return expression.ConditionBuilder{}, err
		}
		parts = append(parts, p)
	}
	if len(parts) == 1 {
		return parts[0], nil // And/Or require >=2 operands; a single child is itself
	}
	if and {
		return expression.And(parts[0], parts[1], parts[2:]...), nil
	}
	return expression.Or(parts[0], parts[1], parts[2:]...), nil
}

// attrName maps a filter path to a DynamoDB attribute name and reports whether
// it is a reserved (always-present, non-null) attribute. version is a plain
// top-level attribute; any other path addresses a nested attribute under data.
// id and type are key attributes and are rejected: DynamoDB forbids key
// attributes in a FilterExpression.
//
// Path segments must not contain "." (interpreted as nested separators by the
// expression package).
func attrName(path string) (name expression.NameBuilder, reserved bool, err error) {
	switch path {
	case "id", "type":
		return expression.NameBuilder{}, false,
			fmt.Errorf("%w: cannot filter on key attribute %q", ember.ErrUnsupportedFilter, path)
	case "version":
		return expression.Name("version"), true, nil
	default:
		return expression.Name("data." + path), false, nil
	}
}

// alwaysTrue / alwaysFalse encode logical identities. version is always present
// (every item carries it) and is not a key attribute, so it is safe to reference
// in a FilterExpression.
func alwaysTrue() expression.ConditionBuilder  { return expression.Name("version").AttributeExists() }
func alwaysFalse() expression.ConditionBuilder { return expression.Name("version").AttributeNotExists() }

// normalizeValue validates a filter value and converts time.Time to RFC3339Nano
// text, matching how time is serialized into the stored JSON data.
func normalizeValue(v any) (any, error) {
	switch x := v.(type) {
	case string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return x, nil
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), nil
	default:
		return nil, fmt.Errorf("%w: value type %T", ember.ErrUnsupportedFilter, v)
	}
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./dynamo/ -run TestBuildFilter -v`
Expected: PASS for all `TestBuildFilter*` cases. If any `expr` string mismatches on cosmetic formatting only, update the expected string in the test to the exact emitted text (see the note at the top of this task), then re-run to green.

- [ ] **Step 5: Commit**

```bash
git add dynamo/filter.go dynamo/filter_test.go
git commit -m "feat(dynamo): translate ember filters to DynamoDB FilterExpression"
```

---

## Task 4: Implement Save, Get, List

**Files:**
- Modify: `dynamo/entity_repository.go`

These methods are thin glue over the converters and filter translator and talk
to DynamoDB, so — like the `mongo` and `postgres` repositories — they are not
unit-tested (no live DynamoDB / containers). Correctness rests on the
interface assertion, the unit-tested helpers, `go build`, and `go vet`.

- [ ] **Step 1: Replace the stub file with the full implementation**

Replace the contents of `dynamo/entity_repository.go` with:
```go
package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/klemen-forstneric/ember"
)

// EntityRepository persists MarshaledEntity values in a single DynamoDB table
// keyed by partition attribute "type" and sort attribute "id".
type EntityRepository struct {
	client *dynamodb.Client
	table  string
}

// NewEntityRepository returns a repository backed by the given client and table.
// The caller owns table creation: partition key "type" (S), sort key "id" (S).
func NewEntityRepository(client *dynamodb.Client, table string) *EntityRepository {
	return &EntityRepository{client: client, table: table}
}

// Save writes the entity with a single conditional PutItem. The write succeeds
// when the item does not yet exist, or when its stored version equals the
// entity's initial (expected) version; otherwise it is a version conflict.
func (r *EntityRepository) Save(ctx context.Context, m *ember.MarshaledEntity) error {
	data, err := marshalData(m.Data)
	if err != nil {
		return err
	}

	item := map[string]types.AttributeValue{
		"type":    &types.AttributeValueMemberS{Value: m.Type},
		"id":      &types.AttributeValueMemberS{Value: m.ID},
		"version": &types.AttributeValueMemberN{Value: strconv.FormatUint(m.Version.Value(), 10)},
		"data":    &types.AttributeValueMemberM{Value: data},
	}

	cond := expression.AttributeNotExists(expression.Name("type")).
		Or(expression.Name("version").Equal(expression.Value(m.Version.Initial())))
	expr, err := expression.NewBuilder().WithCondition(cond).Build()
	if err != nil {
		return err
	}

	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(r.table),
		Item:                      item,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})

	var conflict *types.ConditionalCheckFailedException
	if errors.As(err, &conflict) {
		return ember.ErrVersionConflict
	}
	return err
}

func (r *EntityRepository) Get(ctx context.Context, typ, id string) (*ember.MarshaledEntity, error) {
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.table),
		Key: map[string]types.AttributeValue{
			"type": &types.AttributeValueMemberS{Value: typ},
			"id":   &types.AttributeValueMemberS{Value: id},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, ember.ErrEntityNotFound
	}
	return itemToEntity(out.Item)
}

func (r *EntityRepository) List(ctx context.Context, typ string, f ember.Filter) ([]*ember.MarshaledEntity, error) {
	filter, hasFilter, err := buildFilter(f)
	if err != nil {
		return nil, err
	}

	builder := expression.NewBuilder().
		WithKeyCondition(expression.Key("type").Equal(expression.Value(typ)))
	if hasFilter {
		builder = builder.WithFilter(filter)
	}
	expr, err := builder.Build()
	if err != nil {
		return nil, err
	}

	paginator := dynamodb.NewQueryPaginator(r.client, &dynamodb.QueryInput{
		TableName:                 aws.String(r.table),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(), // nil when there is no filter
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})

	var out []*ember.MarshaledEntity
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			e, err := itemToEntity(item)
			if err != nil {
				return nil, err
			}
			out = append(out, e)
		}
	}
	return out, nil
}

// itemToEntity decodes a stored item into a MarshaledEntity.
func itemToEntity(item map[string]types.AttributeValue) (*ember.MarshaledEntity, error) {
	id, ok := item["id"].(*types.AttributeValueMemberS)
	if !ok {
		return nil, fmt.Errorf("dynamo: item missing string id")
	}
	typ, ok := item["type"].(*types.AttributeValueMemberS)
	if !ok {
		return nil, fmt.Errorf("dynamo: item missing string type")
	}
	verAttr, ok := item["version"].(*types.AttributeValueMemberN)
	if !ok {
		return nil, fmt.Errorf("dynamo: item missing numeric version")
	}
	ver, err := strconv.ParseUint(verAttr.Value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("dynamo: invalid version %q: %w", verAttr.Value, err)
	}

	dataMap := map[string]types.AttributeValue{}
	if d, ok := item["data"].(*types.AttributeValueMemberM); ok {
		dataMap = d.Value
	}
	data, err := unmarshalData(dataMap)
	if err != nil {
		return nil, err
	}

	return &ember.MarshaledEntity{
		ID:      id.Value,
		Type:    typ.Value,
		Version: ember.NewVersion(ver),
		Data:    data,
	}, nil
}
```

- [ ] **Step 2: Build and vet**

Run: `go build ./dynamo/ && go vet ./dynamo/`
Expected: PASS (no output).

- [ ] **Step 3: Run the full package test suite**

Run: `go test ./dynamo/ -v`
Expected: PASS — all converter and filter tests, plus the interface assertion compiles.

- [ ] **Step 4: Commit**

```bash
git add dynamo/entity_repository.go
git commit -m "feat(dynamo): implement Save, Get, List"
```

---

## Task 5: Final verification

**Files:** none (verification only)

- [ ] **Step 1: Format check**

Run: `gofmt -l dynamo/`
Expected: no output (all files formatted). If any file is listed, run `gofmt -w dynamo/` and re-check.

- [ ] **Step 2: Full repo build, vet, and test**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: PASS across the module — the new `dynamo` package and all existing packages.

- [ ] **Step 3: Tidy modules**

Run: `go mod tidy`
Expected: `go.mod`/`go.sum` settle with `service/dynamodb` and `feature/dynamodb/expression` as direct deps and no spurious changes.

- [ ] **Step 4: Commit any tidy/format changes**

```bash
git add -A
git commit -m "chore(dynamo): gofmt and go mod tidy" || echo "nothing to commit"
```
