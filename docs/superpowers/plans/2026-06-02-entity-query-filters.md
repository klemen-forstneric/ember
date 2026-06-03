# EntityStore Field Queries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add backend-neutral field querying to `EntityStore` via a sealed `Filter` AST that each repository translates into its native query dialect.

**Architecture:** Callers build a closed `Filter` tree (`ember.Eq`, `ember.And`, …). `EntityStore.List` scopes by type and delegates to `EntityRepository.List`, hydrating results through the existing marshaler. Each backend translates the AST with a pure function (independently unit-testable) and runs it against the store. By-id reads (`Load` → renamed `Get`) stay a distinct privileged path. Filters reference serialized field paths; `id`/`type`/`version` are reserved metadata paths.

**Tech Stack:** Go 1.26, standard library `testing` (table-driven), `database/sql` (Postgres), `go.mongodb.org/mongo-driver/v2` (Mongo).

**Spec:** `docs/superpowers/specs/2026-06-02-entity-query-filters-design.md`

**Out of scope (deferred):** sorting, pagination, counts; the DynamoDB backend (no package exists yet); live-DB integration tests for the repository wiring (no test-DB infra in the repo today — wiring is guarded by compile-time interface assertions instead).

**Convention:** Every commit message ends with a second `-m` footer line:
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

## File Structure

- **Create** `filter.go` (package `ember`) — `Filter` sum type, `Operator`, constructors, `ErrUnsupportedFilter`.
- **Create** `filter_test.go` (package `ember`) — constructor unit tests.
- **Modify** `entity.go` — rename `Load`→`Get` on `EntityRepository` and `EntityStore`; add `List` to both.
- **Create** `entity_test.go` (package `ember`) — `EntityStore.List`/`Get` tests with fakes.
- **Create** `postgres/filter.go` — pure `buildWhere` translator (AST → SQL `WHERE` + args).
- **Create** `postgres/filter_test.go` — translator unit tests.
- **Modify** `postgres/entity_repository.go` — rename `Load`→`Get`; add `List`.
- **Create** `mongo/filter.go` — pure `buildFilter` translator (AST → `bson.D`).
- **Create** `mongo/filter_test.go` — translator unit tests.
- **Modify** `mongo/entity_repository.go` — rename `Load`→`Get`; add `List`.

---

## Task 1: Filter AST and constructors

**Files:**
- Create: `filter.go`
- Test: `filter_test.go`

- [ ] **Step 1: Write the failing test**

Create `filter_test.go`:

```go
package ember

import (
	"reflect"
	"testing"
)

func TestConstructors(t *testing.T) {
	tests := []struct {
		name string
		got  Filter
		want Filter
	}{
		{"Eq", Eq("status", "open"), Comparison{Path: "status", Op: OpEq, Value: "open"}},
		{"Ne", Ne("status", "open"), Comparison{Path: "status", Op: OpNe, Value: "open"}},
		{"Gt", Gt("total", 100), Comparison{Path: "total", Op: OpGt, Value: 100}},
		{"Gte", Gte("total", 100), Comparison{Path: "total", Op: OpGte, Value: 100}},
		{"Lt", Lt("total", 100), Comparison{Path: "total", Op: OpLt, Value: 100}},
		{"Lte", Lte("total", 100), Comparison{Path: "total", Op: OpLte, Value: 100}},
		{"In", In("region", "EU", "UK"), Membership{Path: "region", Values: []any{"EU", "UK"}}},
		{"Exists", Exists("status", true), Existence{Path: "status", Exists: true}},
		{"Not", Not(Eq("a", 1)), Negation{Filter: Comparison{Path: "a", Op: OpEq, Value: 1}}},
		{
			"And",
			And(Eq("a", 1), Eq("b", 2)),
			Conjunction{Filters: []Filter{
				Comparison{Path: "a", Op: OpEq, Value: 1},
				Comparison{Path: "b", Op: OpEq, Value: 2},
			}},
		},
		{
			"Or",
			Or(Eq("a", 1), Eq("b", 2)),
			Disjunction{Filters: []Filter{
				Comparison{Path: "a", Op: OpEq, Value: 1},
				Comparison{Path: "b", Op: OpEq, Value: 2},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.got, tt.want) {
				t.Errorf("got %#v, want %#v", tt.got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./ -run TestConstructors`
Expected: FAIL — `undefined: Eq`, `undefined: Comparison`, etc.

- [ ] **Step 3: Write the implementation**

Create `filter.go`:

```go
package ember

import "errors"

// ErrUnsupportedFilter is returned by a repository when it cannot translate a
// filter node, operator, or value type into its native query dialect.
var ErrUnsupportedFilter = errors.New("ember: unsupported filter")

// Operator is a comparison operator used by Comparison.
type Operator int

const (
	OpEq Operator = iota
	OpNe
	OpGt
	OpGte
	OpLt
	OpLte
)

// Filter is a sealed sum type. Only ember-defined nodes satisfy it, so
// repositories can translate the closed set exhaustively.
type Filter interface {
	isFilter()
}

// Comparison matches a single path against a value with an operator.
type Comparison struct {
	Path  string
	Op    Operator
	Value any
}

// Membership matches when the path's value is one of Values (IN).
type Membership struct {
	Path   string
	Values []any
}

// Existence matches on whether the path is present (and non-null).
type Existence struct {
	Path   string
	Exists bool
}

// Conjunction is a logical AND over its children.
type Conjunction struct {
	Filters []Filter
}

// Disjunction is a logical OR over its children.
type Disjunction struct {
	Filters []Filter
}

// Negation is a logical NOT of its child.
type Negation struct {
	Filter Filter
}

func (Comparison) isFilter()  {}
func (Membership) isFilter()  {}
func (Existence) isFilter()   {}
func (Conjunction) isFilter() {}
func (Disjunction) isFilter() {}
func (Negation) isFilter()    {}

// Eq matches path == v.
func Eq(path string, v any) Filter { return Comparison{Path: path, Op: OpEq, Value: v} }

// Ne matches path != v.
func Ne(path string, v any) Filter { return Comparison{Path: path, Op: OpNe, Value: v} }

// Gt matches path > v.
func Gt(path string, v any) Filter { return Comparison{Path: path, Op: OpGt, Value: v} }

// Gte matches path >= v.
func Gte(path string, v any) Filter { return Comparison{Path: path, Op: OpGte, Value: v} }

// Lt matches path < v.
func Lt(path string, v any) Filter { return Comparison{Path: path, Op: OpLt, Value: v} }

// Lte matches path <= v.
func Lte(path string, v any) Filter { return Comparison{Path: path, Op: OpLte, Value: v} }

// In matches when the path's value is one of vs.
func In(path string, vs ...any) Filter { return Membership{Path: path, Values: vs} }

// Exists matches on whether the path is present (exists=true) or absent (exists=false).
func Exists(path string, exists bool) Filter { return Existence{Path: path, Exists: exists} }

// And combines filters with logical AND.
func And(fs ...Filter) Filter { return Conjunction{Filters: fs} }

// Or combines filters with logical OR.
func Or(fs ...Filter) Filter { return Disjunction{Filters: fs} }

// Not negates a filter.
func Not(f Filter) Filter { return Negation{Filter: f} }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./ -run TestConstructors`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add filter.go filter_test.go
git commit -m "feat: add backend-neutral Filter AST for entity queries" \
  -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Rename Load→Get and add List on EntityStore

**Files:**
- Modify: `entity.go:50-53` (interface), `entity.go:65-73` (`Load` method)
- Test: `entity_test.go`

- [ ] **Step 1: Write the failing test**

Create `entity_test.go`:

```go
package ember

import (
	"context"
	"errors"
	"testing"
)

// fakeEntity is a minimal Entity for store tests.
type fakeEntity struct {
	EntityRoot
	Name string
}

func newFakeEntity(id string) *fakeEntity {
	return &fakeEntity{EntityRoot: NewEntityRoot(id)}
}

func (e *fakeEntity) Type() string { return "fake" }

// fakeMarshaler hydrates fakeEntity from a MarshaledEntity (ID + Version only).
type fakeMarshaler struct{}

func (fakeMarshaler) Marshal(_ context.Context, e *fakeEntity) (*MarshaledEntity, error) {
	return &MarshaledEntity{ID: e.ID(), Type: e.Type(), Version: e.Version(), Data: []byte(e.Name)}, nil
}

func (fakeMarshaler) Unmarshal(_ context.Context, m *MarshaledEntity) (*fakeEntity, error) {
	e := newFakeEntity(m.ID)
	e.Name = string(m.Data)
	e.SetVersion(m.Version)
	return e, nil
}

// fakeRepo records calls and returns canned results.
type fakeRepo struct {
	getResult  *MarshaledEntity
	getErr     error
	listResult []*MarshaledEntity
	listErr    error
	gotType    string
	gotFilter  Filter
}

func (r *fakeRepo) Save(_ context.Context, _ *MarshaledEntity) error { return nil }

func (r *fakeRepo) Get(_ context.Context, typ, _ string) (*MarshaledEntity, error) {
	r.gotType = typ
	return r.getResult, r.getErr
}

func (r *fakeRepo) List(_ context.Context, typ string, f Filter) ([]*MarshaledEntity, error) {
	r.gotType = typ
	r.gotFilter = f
	return r.listResult, r.listErr
}

func TestEntityStoreList(t *testing.T) {
	repo := &fakeRepo{listResult: []*MarshaledEntity{
		{ID: "1", Type: "fake", Version: NewVersion(3), Data: []byte("alice")},
		{ID: "2", Type: "fake", Version: NewVersion(4), Data: []byte("bob")},
	}}
	store := NewEntityStore[*fakeEntity](repo, fakeMarshaler{})

	f := Eq("name", "alice")
	got, err := store.List(context.Background(), f)
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if repo.gotType != "fake" {
		t.Errorf("repo received type %q, want %q", repo.gotType, "fake")
	}
	if repo.gotFilter != f {
		t.Errorf("repo received filter %#v, want %#v", repo.gotFilter, f)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entities, want 2", len(got))
	}
	if got[0].ID() != "1" || got[0].Name != "alice" {
		t.Errorf("entity[0] = %+v, want id=1 name=alice", got[0])
	}
}

func TestEntityStoreListError(t *testing.T) {
	sentinel := errors.New("boom")
	repo := &fakeRepo{listErr: sentinel}
	store := NewEntityStore[*fakeEntity](repo, fakeMarshaler{})

	_, err := store.List(context.Background(), nil)
	if !errors.Is(err, sentinel) {
		t.Errorf("got error %v, want %v", err, sentinel)
	}
}

func TestEntityStoreGet(t *testing.T) {
	repo := &fakeRepo{getResult: &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(3), Data: []byte("alice")}}
	store := NewEntityStore[*fakeEntity](repo, fakeMarshaler{})

	got, err := store.Get(context.Background(), "1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.ID() != "1" || got.Name != "alice" {
		t.Errorf("entity = %+v, want id=1 name=alice", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./ -run TestEntityStore`
Expected: FAIL — `*fakeRepo` does not implement `EntityRepository` (missing `Get`, `List`); `store.List` undefined; `store.Get` undefined.

- [ ] **Step 3: Update the interface and EntityStore methods**

In `entity.go`, replace the `EntityRepository` interface (currently lines 50-53):

```go
// EntityRepository
type EntityRepository interface {
	Save(ctx context.Context, m *MarshaledEntity) error
	Get(ctx context.Context, typ, id string) (*MarshaledEntity, error)
	List(ctx context.Context, typ string, f Filter) ([]*MarshaledEntity, error)
}
```

Replace the `Load` method (currently lines 65-73) with `Get` and add `List`:

```go
func (s *EntityStore[E]) Get(ctx context.Context, id string) (E, error) {
	var empty E
	m, err := s.repository.Get(ctx, empty.Type(), id)
	if err != nil {
		return empty, err
	}

	return s.marshaler.Unmarshal(ctx, m)
}

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

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./ -run TestEntityStore`
Expected: PASS

Note: `go build ./...` will now fail in `postgres`/`mongo` because they still define `Load` and lack `List`. That is fixed in Tasks 3–6. Run `go vet ./` (root package only) to confirm the root compiles:
Run: `go vet ./`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add entity.go entity_test.go
git commit -m "feat: rename Load to Get and add List on EntityStore" \
  -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Postgres filter translator

**Files:**
- Create: `postgres/filter.go`
- Test: `postgres/filter_test.go`

The translator is a pure function: `buildWhere(f) -> (sqlExpr, args, error)` with placeholders numbered from `$1`. The repository (Task 4) offsets them after the `type` parameter. Reserved paths (`id`, `type`, `version`) map to columns; other paths map to `data#>>'{seg,seg}'` (text extraction). Numeric/bool values get a cast on jsonb-extracted text so comparisons are correct. `time.Time` is normalized to RFC3339Nano text (matching how JSON serializes it). Unsupported value types yield `ember.ErrUnsupportedFilter`.

- [ ] **Step 1: Write the failing test**

Create `postgres/filter_test.go`:

```go
package postgres

import (
	"errors"
	"reflect"
	"testing"

	"github.com/klemen-forstneric/ember"
)

func TestBuildWhere(t *testing.T) {
	tests := []struct {
		name     string
		filter   ember.Filter
		wantExpr string
		wantArgs []any
	}{
		{"nil matches all", nil, "", nil},
		{"eq string data path", ember.Eq("status", "open"), "data#>>'{status}' = $1", []any{"open"}},
		{"nested data path", ember.Eq("address.city", "NYC"), "data#>>'{address,city}' = $1", []any{"NYC"}},
		{"reserved id", ember.Eq("id", "x"), "id = $1", []any{"x"}},
		{"reserved version", ember.Gt("version", 5), "version > $1", []any{5}},
		{"gt numeric cast", ember.Gt("total", 4200), "(data#>>'{total}')::numeric > $1", []any{4200}},
		{"ne", ember.Ne("status", "open"), "data#>>'{status}' <> $1", []any{"open"}},
		{"in", ember.In("region", "EU", "UK"), "data#>>'{region}' IN ($1, $2)", []any{"EU", "UK"}},
		{"in empty is false", ember.In("region"), "FALSE", nil},
		{"exists true", ember.Exists("status", true), "data#>>'{status}' IS NOT NULL", nil},
		{"exists false", ember.Exists("status", false), "data#>>'{status}' IS NULL", nil},
		{"not", ember.Not(ember.Eq("status", "open")), "NOT (data#>>'{status}' = $1)", []any{"open"}},
		{
			"and",
			ember.And(ember.Eq("status", "open"), ember.Gt("total", 100)),
			"(data#>>'{status}' = $1) AND ((data#>>'{total}')::numeric > $2)",
			[]any{"open", 100},
		},
		{
			"or",
			ember.Or(ember.Eq("a", "1"), ember.Eq("b", "2")),
			"(data#>>'{a}' = $1) OR (data#>>'{b}' = $2)",
			[]any{"1", "2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, args, err := buildWhere(tt.filter)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if expr != tt.wantExpr {
				t.Errorf("expr = %q, want %q", expr, tt.wantExpr)
			}
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("args = %#v, want %#v", args, tt.wantArgs)
			}
		})
	}
}

func TestBuildWhereUnsupportedValue(t *testing.T) {
	_, _, err := buildWhere(ember.Eq("status", []string{"nope"}))
	if !errors.Is(err, ember.ErrUnsupportedFilter) {
		t.Errorf("got %v, want ErrUnsupportedFilter", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./postgres -run TestBuildWhere`
Expected: FAIL — `undefined: buildWhere`.

- [ ] **Step 3: Write the implementation**

Create `postgres/filter.go`:

```go
package postgres

import (
	"fmt"
	"strings"
	"time"

	"github.com/klemen-forstneric/ember"
)

// buildWhere translates a Filter into a SQL boolean expression with bind
// arguments. Placeholders are numbered from $1. A nil filter yields an empty
// expression (match all).
func buildWhere(f ember.Filter) (string, []any, error) {
	if f == nil {
		return "", nil, nil
	}
	t := &translator{}
	expr, err := t.node(f)
	if err != nil {
		return "", nil, err
	}
	return expr, t.args, nil
}

type translator struct {
	args []any
}

func (t *translator) placeholder(v any) (string, error) {
	nv, err := normalizeValue(v)
	if err != nil {
		return "", err
	}
	t.args = append(t.args, nv)
	return fmt.Sprintf("$%d", len(t.args)), nil
}

func (t *translator) node(f ember.Filter) (string, error) {
	switch n := f.(type) {
	case ember.Comparison:
		return t.comparison(n)
	case ember.Membership:
		return t.membership(n)
	case ember.Existence:
		return existence(n), nil
	case ember.Conjunction:
		return t.composite(n.Filters, "AND", "TRUE")
	case ember.Disjunction:
		return t.composite(n.Filters, "OR", "FALSE")
	case ember.Negation:
		inner, err := t.node(n.Filter)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("NOT (%s)", inner), nil
	default:
		return "", fmt.Errorf("%w: unknown node %T", ember.ErrUnsupportedFilter, f)
	}
}

func (t *translator) comparison(c ember.Comparison) (string, error) {
	col, reserved := column(c.Path)
	op, err := sqlOp(c.Op)
	if err != nil {
		return "", err
	}
	if !reserved {
		col = castFor(col, c.Value)
	}
	ph, err := t.placeholder(c.Value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s %s %s", col, op, ph), nil
}

func (t *translator) membership(m ember.Membership) (string, error) {
	if len(m.Values) == 0 {
		return "FALSE", nil
	}
	col, _ := column(m.Path)
	phs := make([]string, 0, len(m.Values))
	for _, v := range m.Values {
		ph, err := t.placeholder(v)
		if err != nil {
			return "", err
		}
		phs = append(phs, ph)
	}
	return fmt.Sprintf("%s IN (%s)", col, strings.Join(phs, ", ")), nil
}

func (t *translator) composite(fs []ember.Filter, joiner, empty string) (string, error) {
	if len(fs) == 0 {
		return empty, nil
	}
	parts := make([]string, 0, len(fs))
	for _, f := range fs {
		expr, err := t.node(f)
		if err != nil {
			return "", err
		}
		parts = append(parts, "("+expr+")")
	}
	return strings.Join(parts, " "+joiner+" "), nil
}

func existence(e ember.Existence) string {
	col, _ := column(e.Path)
	if e.Exists {
		return col + " IS NOT NULL"
	}
	return col + " IS NULL"
}

// column maps a filter path to a SQL column expression. Reserved metadata paths
// map to real columns; everything else extracts text from the jsonb data column.
func column(path string) (expr string, reserved bool) {
	switch path {
	case "id", "type", "version":
		return path, true
	default:
		segs := strings.Split(path, ".")
		return fmt.Sprintf("data#>>'{%s}'", strings.Join(segs, ",")), false
	}
}

// castFor wraps a jsonb text expression with a cast so non-string comparisons
// behave numerically/logically rather than lexically.
func castFor(col string, v any) string {
	switch v.(type) {
	case bool:
		return "(" + col + ")::boolean"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return "(" + col + ")::numeric"
	default:
		return col
	}
}

func sqlOp(op ember.Operator) (string, error) {
	switch op {
	case ember.OpEq:
		return "=", nil
	case ember.OpNe:
		return "<>", nil
	case ember.OpGt:
		return ">", nil
	case ember.OpGte:
		return ">=", nil
	case ember.OpLt:
		return "<", nil
	case ember.OpLte:
		return "<=", nil
	default:
		return "", fmt.Errorf("%w: operator %d", ember.ErrUnsupportedFilter, op)
	}
}

// normalizeValue validates a filter value and converts time.Time to RFC3339Nano
// text (matching JSON serialization of the stored data).
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

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./postgres -run TestBuildWhere`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add postgres/filter.go postgres/filter_test.go
git commit -m "feat: add postgres filter-to-WHERE translator" \
  -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Postgres repository — rename Get and add List

**Files:**
- Modify: `postgres/entity_repository.go:46-70` (rename `Load`→`Get`), add `List` and a compile-time interface assertion.

No live DB is available, so the test is a compile-time assertion that `*EntityRepository` satisfies `ember.EntityRepository` (this fails to compile if the method set is wrong). Behavior of the SQL is covered by Task 3's translator tests.

- [ ] **Step 1: Write the failing test (compile-time assertion)**

Append to `postgres/filter_test.go`:

```go
// Compile-time assertion that the repository satisfies the interface.
var _ ember.EntityRepository = (*EntityRepository)(nil)
```

- [ ] **Step 2: Run to verify it fails**

Run: `go build ./postgres`
Expected: FAIL — `*EntityRepository` does not implement `ember.EntityRepository` (missing method `Get`; has `Load`; missing `List`).

- [ ] **Step 3: Rename Load→Get and add List**

In `postgres/entity_repository.go`, rename the `Load` method to `Get` (change only the method name on line 46; body unchanged):

```go
func (r *EntityRepository) Get(ctx context.Context, typ, id string) (*ember.MarshaledEntity, error) {
```

Add the `List` method and ensure `strings` is imported:

```go
func (r *EntityRepository) List(ctx context.Context, typ string, f ember.Filter) ([]*ember.MarshaledEntity, error) {
	where, args, err := buildWhere(f)
	if err != nil {
		return nil, err
	}

	typeIdx := len(args) + 1
	args = append(args, typ)

	var sb strings.Builder
	fmt.Fprintf(&sb, "SELECT id, version, data FROM %s WHERE type = $%d", r.table, typeIdx)
	if where != "" {
		fmt.Fprintf(&sb, " AND (%s)", where)
	}

	rows, err := r.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*ember.MarshaledEntity
	for rows.Next() {
		var (
			id      string
			version uint64
			data    []byte
		)
		if err := rows.Scan(&id, &version, &data); err != nil {
			return nil, err
		}
		out = append(out, &ember.MarshaledEntity{
			ID:      id,
			Type:    typ,
			Version: ember.NewVersion(version),
			Data:    data,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
```

Update the import block at the top of the file to include `strings`:

```go
import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/klemen-forstneric/ember"
)
```

- [ ] **Step 4: Run to verify it passes**

Run: `go build ./postgres && go test ./postgres`
Expected: build succeeds; tests PASS.

- [ ] **Step 5: Commit**

```bash
git add postgres/entity_repository.go postgres/filter_test.go
git commit -m "feat: implement postgres List and rename Load to Get" \
  -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

> **Note for integration (deferred):** the `data` column must be `jsonb` for `data#>>'{...}'` extraction to work. Document this in the table DDL.

---

## Task 5: Mongo filter translator

**Files:**
- Create: `mongo/filter.go`
- Test: `mongo/filter_test.go`

Pure function `buildFilter(f) -> (bson.D, error)`. Reserved paths map to `_id`/`type`/`version`; other paths map to `data.<dotted.path>`. A nil filter yields an empty `bson.D{}` (match all). Comparisons use `$eq`/`$ne`/`$gt`/… uniformly; `In`→`$in`; `Exists`→`$exists`; `And`→`$and`; `Or`→`$or`; `Not`→`$nor`. Values are normalized the same way as Postgres (time→RFC3339Nano text; unsupported types→`ember.ErrUnsupportedFilter`).

- [ ] **Step 1: Write the failing test**

Create `mongo/filter_test.go`:

```go
package mongo

import (
	"errors"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/klemen-forstneric/ember"
)

func TestBuildFilter(t *testing.T) {
	tests := []struct {
		name   string
		filter ember.Filter
		want   bson.D
	}{
		{"nil matches all", nil, bson.D{}},
		{"eq data path", ember.Eq("status", "open"), bson.D{{Key: "data.status", Value: bson.D{{Key: "$eq", Value: "open"}}}}},
		{"nested data path", ember.Eq("address.city", "NYC"), bson.D{{Key: "data.address.city", Value: bson.D{{Key: "$eq", Value: "NYC"}}}}},
		{"reserved id", ember.Eq("id", "x"), bson.D{{Key: "_id", Value: bson.D{{Key: "$eq", Value: "x"}}}}},
		{"reserved version", ember.Gt("version", 5), bson.D{{Key: "version", Value: bson.D{{Key: "$gt", Value: 5}}}}},
		{"gt", ember.Gt("total", 100), bson.D{{Key: "data.total", Value: bson.D{{Key: "$gt", Value: 100}}}}},
		{"in", ember.In("region", "EU", "UK"), bson.D{{Key: "data.region", Value: bson.D{{Key: "$in", Value: bson.A{"EU", "UK"}}}}}},
		{"exists", ember.Exists("status", true), bson.D{{Key: "data.status", Value: bson.D{{Key: "$exists", Value: true}}}}},
		{
			"and",
			ember.And(ember.Eq("a", "1"), ember.Eq("b", "2")),
			bson.D{{Key: "$and", Value: bson.A{
				bson.D{{Key: "data.a", Value: bson.D{{Key: "$eq", Value: "1"}}}},
				bson.D{{Key: "data.b", Value: bson.D{{Key: "$eq", Value: "2"}}}},
			}}},
		},
		{
			"or",
			ember.Or(ember.Eq("a", "1"), ember.Eq("b", "2")),
			bson.D{{Key: "$or", Value: bson.A{
				bson.D{{Key: "data.a", Value: bson.D{{Key: "$eq", Value: "1"}}}},
				bson.D{{Key: "data.b", Value: bson.D{{Key: "$eq", Value: "2"}}}},
			}}},
		},
		{
			"not",
			ember.Not(ember.Eq("status", "open")),
			bson.D{{Key: "$nor", Value: bson.A{
				bson.D{{Key: "data.status", Value: bson.D{{Key: "$eq", Value: "open"}}}},
			}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildFilter(tt.filter)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestBuildFilterUnsupportedValue(t *testing.T) {
	_, err := buildFilter(ember.Eq("status", []string{"nope"}))
	if !errors.Is(err, ember.ErrUnsupportedFilter) {
		t.Errorf("got %v, want ErrUnsupportedFilter", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./mongo -run TestBuildFilter`
Expected: FAIL — `undefined: buildFilter`.

- [ ] **Step 3: Write the implementation**

Create `mongo/filter.go`:

```go
package mongo

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/klemen-forstneric/ember"
)

// buildFilter translates a Filter into a MongoDB query document. A nil filter
// yields an empty document (match all).
func buildFilter(f ember.Filter) (bson.D, error) {
	if f == nil {
		return bson.D{}, nil
	}
	return node(f)
}

func node(f ember.Filter) (bson.D, error) {
	switch n := f.(type) {
	case ember.Comparison:
		op, err := mongoOp(n.Op)
		if err != nil {
			return nil, err
		}
		v, err := normalizeValue(n.Value)
		if err != nil {
			return nil, err
		}
		return bson.D{{Key: field(n.Path), Value: bson.D{{Key: op, Value: v}}}}, nil
	case ember.Membership:
		vals := make(bson.A, 0, len(n.Values))
		for _, raw := range n.Values {
			v, err := normalizeValue(raw)
			if err != nil {
				return nil, err
			}
			vals = append(vals, v)
		}
		return bson.D{{Key: field(n.Path), Value: bson.D{{Key: "$in", Value: vals}}}}, nil
	case ember.Existence:
		return bson.D{{Key: field(n.Path), Value: bson.D{{Key: "$exists", Value: n.Exists}}}}, nil
	case ember.Conjunction:
		return composite("$and", n.Filters)
	case ember.Disjunction:
		return composite("$or", n.Filters)
	case ember.Negation:
		inner, err := node(n.Filter)
		if err != nil {
			return nil, err
		}
		return bson.D{{Key: "$nor", Value: bson.A{inner}}}, nil
	default:
		return nil, fmt.Errorf("%w: unknown node %T", ember.ErrUnsupportedFilter, f)
	}
}

func composite(op string, fs []ember.Filter) (bson.D, error) {
	arr := make(bson.A, 0, len(fs))
	for _, f := range fs {
		d, err := node(f)
		if err != nil {
			return nil, err
		}
		arr = append(arr, d)
	}
	return bson.D{{Key: op, Value: arr}}, nil
}

// field maps a filter path to a Mongo field path. Reserved metadata paths map to
// top-level fields; everything else lives under the data document.
func field(path string) string {
	switch path {
	case "id":
		return "_id"
	case "type", "version":
		return path
	default:
		return "data." + path
	}
}

func mongoOp(op ember.Operator) (string, error) {
	switch op {
	case ember.OpEq:
		return "$eq", nil
	case ember.OpNe:
		return "$ne", nil
	case ember.OpGt:
		return "$gt", nil
	case ember.OpGte:
		return "$gte", nil
	case ember.OpLt:
		return "$lt", nil
	case ember.OpLte:
		return "$lte", nil
	default:
		return "", fmt.Errorf("%w: operator %d", ember.ErrUnsupportedFilter, op)
	}
}

// normalizeValue validates a filter value and converts time.Time to RFC3339Nano
// text (matching JSON serialization of the stored data).
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

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./mongo -run TestBuildFilter`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add mongo/filter.go mongo/filter_test.go
git commit -m "feat: add mongo filter-to-bson translator" \
  -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Mongo repository — rename Get and add List

**Files:**
- Modify: `mongo/entity_repository.go:53-85` (rename `Load`→`Get`), add `List` and a compile-time interface assertion.

- [ ] **Step 1: Write the failing test (compile-time assertion)**

Append to `mongo/filter_test.go`:

```go
// Compile-time assertion that the repository satisfies the interface.
var _ ember.EntityRepository = (*EntityRepository)(nil)
```

- [ ] **Step 2: Run to verify it fails**

Run: `go build ./mongo`
Expected: FAIL — `*EntityRepository` does not implement `ember.EntityRepository` (missing `Get`/`List`).

- [ ] **Step 3: Rename Load→Get and add List**

In `mongo/entity_repository.go`, rename the `Load` method to `Get` (line 53; body unchanged):

```go
func (r *EntityRepository) Get(ctx context.Context, typ, id string) (*ember.MarshaledEntity, error) {
```

Add the `List` method (it reuses the same decode shape as `Get`, combining the type scope with the translated filter via `$and`):

```go
func (r *EntityRepository) List(ctx context.Context, typ string, f ember.Filter) ([]*ember.MarshaledEntity, error) {
	predicate, err := buildFilter(f)
	if err != nil {
		return nil, err
	}

	filter := bson.D{{Key: "type", Value: typ}}
	if len(predicate) > 0 {
		filter = bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "type", Value: typ}},
			predicate,
		}}}
	}

	cur, err := r.collection.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var out []*ember.MarshaledEntity
	for cur.Next(ctx) {
		var e struct {
			ID      string   `bson:"_id"`
			Type    string   `bson:"type"`
			Version uint64   `bson:"version"`
			Data    bson.Raw `bson:"data"`
		}
		if err := cur.Decode(&e); err != nil {
			return nil, err
		}

		data, err := bson.MarshalExtJSON(e.Data, false, false)
		if err != nil {
			return nil, err
		}

		out = append(out, &ember.MarshaledEntity{
			ID:      e.ID,
			Type:    e.Type,
			Version: ember.NewVersion(e.Version),
			Data:    data,
		})
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go build ./mongo && go test ./mongo`
Expected: build succeeds; tests PASS.

- [ ] **Step 5: Final full build and test, then commit**

```bash
go build ./... && go test ./...
```
Expected: all packages build; all tests PASS.

```bash
git add mongo/entity_repository.go mongo/filter_test.go
git commit -m "feat: implement mongo List and rename Load to Get" \
  -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review Notes

- **Spec coverage:** Filter AST + constructors (Task 1); `Get`/`List`/`Save` surface with `List` on `EntityRepository` (Task 2); reserved-path split + per-backend exhaustive translation (Tasks 3–6); `ErrUnsupportedFilter` (Tasks 3, 5); nil-means-all (Tasks 3, 5); restricted value types incl. `time.Time` via `normalizeValue` (Tasks 3, 5). DynamoDB and sort/pagination are explicitly deferred per spec scope.
- **Type consistency:** `buildWhere(f) (string, []any, error)` and `buildFilter(f) (bson.D, error)` are referenced consistently by their repositories. `column`/`field`/`normalizeValue`/`sqlOp`/`mongoOp` names match across definition and use. `EntityRepository` method set (`Save`/`Get`/`List`) is identical in the interface (Task 2) and both implementations (Tasks 4, 6).
- **Deferred & noted:** repository wiring (SQL execution, cursor iteration) is guarded only by compile-time interface assertions because there is no test-DB infra in the repo; live integration tests are a follow-up. The Postgres `data` column must be `jsonb`.
