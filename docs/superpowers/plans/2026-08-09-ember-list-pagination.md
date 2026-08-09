# Ember Typed Sort + List Pagination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `List` a declared sort ordering (lexical vs numeric) and bounded result sets via offset paging and keyset (cursor) paging, across all four backends.

**Architecture:** `Sort` gains an `Ordering` field so the comparison semantics of an `ORDER BY` are declared rather than inferred from storage types. A new `Page` value type carries `Limit` plus *either* `Offset` or a `Cursor`, and is threaded through `EntityRepository.List` as a fifth parameter. The existing `EntityStore.List`/`EntityLoader.List` signatures are unchanged and delegate with `Unpaged()`; a new `ListPage` is the paged entry point. `Cursor` owns its own text codec so a cursor can cross an HTTP boundary without each service hand-rolling a token format.

**Tech Stack:** Go 1.26.3, testify (`require`/`assert`, `suite` where a file already uses it), `mock.Mock` for doubles, squirrel for postgres SQL, mongo-driver v2, go-sqlmock for postgres unit tests.

## Global Constraints

- **Comments: none.** Allowed in source: a bare `// TypeName` label before a type declaration, and a one-line package doc. Nothing else — no doc comments on funcs/methods/fields, no rationale, no comments above guards, no comments on test functions. Rationale goes in the commit message. When editing a block that currently carries a prose comment, **delete the comment, do not shorten it.** Before each commit run `git diff | grep '^+.*//'` and justify every hit against this rule.
- **Deleting a type or renaming one?** Also grep the repo for its name inside comments and string literals — a deletion can leave a comment that is now false.
- Module path is `github.com/klemen-forstneric/ember`. Tests import it as `"github.com/klemen-forstneric/ember"`.
- Follow the per-file test idiom: `ember/loader_test.go` and `ember/store_test.go` use `testify/suite`; `ember/sort_test.go`, `embertest/*_test.go`, `mongo/*_test.go`, `postgres/*_test.go` use plain `func TestX(t *testing.T)` with `require`/`assert`. Match the file you are editing.
- Mongo and postgres integration tests **skip** when no server is reachable (`connectTestMongo`, `connectTestPostgres`). Run mongo-backed work with a live mongo (`docker compose up -d mongo`) or the tests silently pass having exercised nothing. `go test -short ./...` skips them deliberately.
- `Ordering` applies **only to data paths**. Reserved paths (`id`, `type`, `version`) are native top-level columns/fields and sort by their own type; `Ordering` is ignored for them.
- **Out of scope for this plan:** the fleet. Services pin ember by pseudo-version with no `replace` directive, so nothing outside `ember/` breaks. At bump time three call sites need updating — `conversation-service/internal/conversation/repository.go:71,86` gain `.Numeric()`, and two fleet test doubles implementing `ember.EntityRepository` need the new `List` parameter (`payment-service/internal/payment/openpay/customer_test.go`, `subscription-service/internal/subscription/repository_test.go`).

---

## File Structure

**Phase 1 — typed Sort**

| File | Responsibility |
|---|---|
| `sort.go` (modify) | `Ordering` type, `Sort.Ordering` field, `Numeric()`/`Lexical()` methods |
| `sort_test.go` (modify) | constructor/method round-trips |
| `postgres/sort.go` (create) | `orderBy(Sort, bool) []string` — the only place a sort expression is rendered |
| `postgres/sort_test.go` (create) | table test over rendered ORDER BY fragments |
| `postgres/entity_repository.go` (modify) | call `orderBy` instead of inlining the clause |
| `embertest/sort.go` (modify) | `applySort` honors `Ordering` and the paged tiebreak |
| `embertest/repository_test.go` (modify) | lexical vs numeric ordering |
| `mongo/sort_test.go` (modify) | prove mongo's native numeric ordering |

**Phase 2 — pagination**

| File | Responsibility |
|---|---|
| `page.go` (create) | `Page`, `Cursor`, constructors, `Validate`, text codec, sentinels |
| `page_test.go` (create) | constructors, validation, codec round-trips |
| `entity.go` (modify) | `EntityRepository.List` gains a `Page` parameter |
| `loader.go` (modify) | `List` delegates with `Unpaged()`; `ListPage` added and validates |
| `store.go` (modify) | `ListPage` passthrough |
| `mocks_test.go`, `loader_test.go`, `store_test.go` (modify) | mock signature + expectations |
| `mongo/entity_repository.go`, `mongo/sort.go` (modify) | `sortDoc`, `seekPredicate`, limit/skip |
| `postgres/entity_repository.go`, `postgres/sort.go` (modify) | `seekPredicate`, LIMIT/OFFSET |
| `embertest/repository.go`, `embertest/sort.go` (modify) | `seek`, offset/limit slicing |
| `dynamo/entity_repository.go` (modify) | native `Limit`, unsorted `ExclusiveStartKey`, reject offset |

---

## Task 1: Typed Sort core

**Files:**
- Modify: `sort.go`
- Test: `sort_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `type Ordering int`, `const (Lexical Ordering = iota; Numeric)`, `Sort{Path string; Direction Direction; Ordering Ordering}`, `func (s Sort) Lexical() Sort`, `func (s Sort) Numeric() Sort`. `Asc`, `Desc`, `Unsorted`, `Direction`, `Ascending`, `Descending`, `ErrUnsupportedSort` keep their current names and behavior.

- [ ] **Step 1: Write the failing test**

Append to `sort_test.go`:

```go
func TestSortOrdering(t *testing.T) {
	assert.Equal(t, Lexical, Asc("created_at").Ordering)
	assert.Equal(t, Lexical, Unsorted().Ordering)
	assert.Equal(t, Sort{Path: "seq", Direction: Ascending, Ordering: Numeric}, Asc("seq").Numeric())
	assert.Equal(t, Sort{Path: "seq", Direction: Descending, Ordering: Numeric}, Desc("seq").Numeric())
	assert.Equal(t, Asc("seq"), Asc("seq").Numeric().Lexical())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./ -run TestSortOrdering -v`
Expected: FAIL — `Asc("seq").Numeric undefined (type Sort has no field or method Numeric)`

- [ ] **Step 3: Write minimal implementation**

Replace the entire contents of `sort.go` with:

```go
package ember

import "errors"

// Direction
type Direction int

const (
	Ascending Direction = iota
	Descending
)

// Ordering
type Ordering int

const (
	Lexical Ordering = iota
	Numeric
)

// Sort
type Sort struct {
	Path      string
	Direction Direction
	Ordering  Ordering
}

func Unsorted() Sort { return Sort{} }

func Asc(path string) Sort { return Sort{Path: path, Direction: Ascending} }

func Desc(path string) Sort { return Sort{Path: path, Direction: Descending} }

func (s Sort) Lexical() Sort {
	s.Ordering = Lexical
	return s
}

func (s Sort) Numeric() Sort {
	s.Ordering = Numeric
	return s
}

var ErrUnsupportedSort = errors.New("ember: unsupported sort")
```

Note what this deletes: the long doc comment on `Sort` claiming ordering is lexical (now false), and the prose comments on `Direction`, `Unsorted`, `Asc`, `Desc`, `ErrUnsupportedSort`. That is intentional — see Global Constraints.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./ -run TestSort -v`
Expected: PASS for `TestSortOrdering` and the existing `TestSortConstructors`.

- [ ] **Step 5: Commit**

```bash
git add sort.go sort_test.go
git commit -m "feat: declare sort ordering on Sort

Sort claimed lexical ordering for every backend, which was never true:
mongo orders by native BSON type, so a numeric field already sorted
numerically while postgres extracted it as text and embertest compared it
as text. The claim hid a real divergence between the fake and production.

Ordering makes the comparison semantics explicit at the call site instead
of implicit in each backend's storage. Lexical is the zero value, so every
existing Sort literal and constructor keeps its current behavior.

Ordering applies only to data paths; reserved paths (id, type, version)
are native columns and sort by their own type."
```

---

## Task 2: Typed Sort in embertest

**Files:**
- Modify: `embertest/sort.go`
- Test: `embertest/repository_test.go`

**Interfaces:**
- Consumes: `ember.Ordering`, `ember.Lexical`, `ember.Numeric` from Task 1.
- Produces: `applySort(items []*ember.MarshaledEntity, s ember.Sort)` keeps its signature for now (Task 9 adds the paged tiebreak). New unexported `lessThan(a, b any, o ember.Ordering) bool`.

- [ ] **Step 1: Write the failing test**

In `embertest/repository_test.go`, replace `TestListSortIsLexical` (currently at line 92, including the two comment lines above it) with:

```go
func TestListSortLexicalVsNumeric(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("a", "t", 1, `{"n":9}`)))
	require.NoError(t, r.Save(ctx, me("b", "t", 1, `{"n":10}`)))
	require.NoError(t, r.Save(ctx, me("c", "t", 1, `{"n":2}`)))

	lex, err := r.List(ctx, "t", nil, ember.Asc("n"))
	require.NoError(t, err)
	require.Len(t, lex, 3)
	assert.Equal(t, []string{"b", "c", "a"}, []string{lex[0].ID, lex[1].ID, lex[2].ID})

	num, err := r.List(ctx, "t", nil, ember.Asc("n").Numeric())
	require.NoError(t, err)
	require.Len(t, num, 3)
	assert.Equal(t, []string{"c", "a", "b"}, []string{num[0].ID, num[1].ID, num[2].ID})

	desc, err := r.List(ctx, "t", nil, ember.Desc("n").Numeric())
	require.NoError(t, err)
	require.Len(t, desc, 3)
	assert.Equal(t, []string{"b", "a", "c"}, []string{desc[0].ID, desc[1].ID, desc[2].ID})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./embertest/ -run TestListSortLexicalVsNumeric -v`
Expected: FAIL — the `Numeric` case returns `b, c, a` (still lexical) rather than `c, a, b`.

- [ ] **Step 3: Write minimal implementation**

In `embertest/sort.go`, replace `applySort` (lines 11-32, including the doc comment above it) with:

```go
func applySort(items []*ember.MarshaledEntity, s ember.Sort) {
	if s.Path == "" {
		return
	}
	sort.SliceStable(items, func(i, j int) bool {
		vi, oki, _ := lookup(items[i], s.Path)
		vj, okj, _ := lookup(items[j], s.Path)
		if !oki || !okj {
			return oki && !okj
		}
		if s.Direction == ember.Descending {
			return lessThan(vj, vi, s.Ordering)
		}
		return lessThan(vi, vj, s.Ordering)
	})
}

func lessThan(a, b any, o ember.Ordering) bool {
	if o == ember.Numeric {
		c, ok := orderJSON(a, b)
		return ok && c < 0
	}
	return textOf(a) < textOf(b)
}
```

Also delete the doc comment above `textOf` (lines 34-35) — it explains the lexical rationale, which now lives in the commit message.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./embertest/ -v`
Expected: PASS, all tests in the package.

- [ ] **Step 5: Commit**

```bash
git add embertest/sort.go embertest/repository_test.go
git commit -m "fix(embertest): honor Sort.Ordering in the fake

The fake compared every sort value as text while mongo orders by native
BSON type, so a numeric field ordered 10 before 9 in tests and correctly
in production. A service test asserting message order by seq could pass
against an order production never produces.

orderJSON already existed for filter comparisons; the numeric path reuses
it rather than adding a second comparator."
```

---

## Task 3: Typed Sort in postgres

**Files:**
- Create: `postgres/sort.go`
- Create: `postgres/sort_test.go`
- Modify: `postgres/entity_repository.go:103-110`

**Interfaces:**
- Consumes: `ember.Ordering`, `ember.Numeric` from Task 1; `column(path string) (string, bool)` from `postgres/filter.go:133`.
- Produces: `sortExpr(s ember.Sort) string` returning the ORDER BY left-hand expression (cast applied for `Numeric` on a data path), and `orderBy(s ember.Sort) []string` returning ORDER BY fragments. Task 8 extends `orderBy` with a paged tiebreak.

- [ ] **Step 1: Write the failing test**

Create `postgres/sort_test.go`:

```go
package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/klemen-forstneric/ember"
)

func TestOrderBy(t *testing.T) {
	tests := []struct {
		name string
		sort ember.Sort
		want []string
	}{
		{"unsorted", ember.Unsorted(), nil},
		{"lexical asc", ember.Asc("created_at"), []string{"data#>>'{created_at}' ASC"}},
		{"lexical desc", ember.Desc("created_at"), []string{"data#>>'{created_at}' DESC"}},
		{"numeric asc", ember.Asc("seq").Numeric(), []string{"(data#>>'{seq}')::numeric ASC"}},
		{"numeric desc", ember.Desc("seq").Numeric(), []string{"(data#>>'{seq}')::numeric DESC"}},
		{"nested path", ember.Asc("job.id"), []string{"data#>>'{job,id}' ASC"}},
		{"reserved id", ember.Asc("id"), []string{"id ASC"}},
		{"reserved version ignores ordering", ember.Asc("version").Numeric(), []string{"version ASC"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, orderBy(tt.sort))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./postgres/ -run TestOrderBy -v`
Expected: FAIL — `undefined: orderBy`

- [ ] **Step 3: Write minimal implementation**

Create `postgres/sort.go`:

```go
package postgres

import "github.com/klemen-forstneric/ember"

func sortExpr(s ember.Sort) string {
	col, reserved := column(s.Path)
	if reserved || s.Ordering != ember.Numeric {
		return col
	}
	return "(" + col + ")::numeric"
}

func orderBy(s ember.Sort) []string {
	if s.Path == "" {
		return nil
	}
	return []string{sortExpr(s) + " " + direction(s.Direction)}
}

func direction(d ember.Direction) string {
	if d == ember.Descending {
		return "DESC"
	}
	return "ASC"
}
```

In `postgres/entity_repository.go`, replace lines 103-110:

```go
	if s.Path != "" {
		col, _ := column(s.Path)
		dir := "ASC"
		if s.Direction == ember.Descending {
			dir = "DESC"
		}
		qb = qb.OrderBy(col + " " + dir)
	}
```

with:

```go
	if clauses := orderBy(s); len(clauses) > 0 {
		qb = qb.OrderBy(clauses...)
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./postgres/ -v`
Expected: PASS. Integration tests skip if no postgres is reachable; that is fine here since `TestOrderBy` needs no server.

- [ ] **Step 5: Commit**

```bash
git add postgres/sort.go postgres/sort_test.go postgres/entity_repository.go
git commit -m "feat(postgres): cast the ORDER BY expression for a numeric sort

data#>>'{path}' extracts jsonb as text, so ORDER BY sorted numbers
lexically while the filter builder already cast the same path to numeric
via castFor. A numeric sort and a numeric filter on one path disagreed.

Rendering the sort expression in one function also gives the keyset
predicate a single place to read the comparison expression from, so
ORDER BY and the cursor comparison cannot drift apart."
```

---

## Task 4: Prove mongo's native numeric ordering

**Files:**
- Test: `mongo/sort_test.go`

**Interfaces:**
- Consumes: `ember.Numeric` from Task 1.
- Produces: test helper `makeNumEntity(n int, id string) bson.D` and `nNumbers(ms []*ember.MarshaledEntity) []float64` for later mongo tests.

No production code changes — mongo's BSON ordering is already value-typed, so `Numeric` is honored natively. This task pins that as a tested guarantee rather than an assumption.

- [ ] **Step 1: Write the failing test**

Append to `mongo/sort_test.go`:

```go
func makeNumEntity(n int, id string) bson.D {
	return bson.D{
		{Key: "entity_id", Value: id},
		{Key: "type", Value: "fake"},
		{Key: "version", Value: uint64(1)},
		{Key: "data", Value: bson.D{{Key: "n", Value: n}}},
	}
}

func nNumbers(ms []*ember.MarshaledEntity) []float64 {
	out := make([]float64, len(ms))
	for i, m := range ms {
		var d map[string]float64
		_ = json.Unmarshal(m.Data, &d)
		out[i] = d["n"]
	}
	return out
}

func TestListSortNumeric(t *testing.T) {
	col := connectTestMongo(t)
	ctx := context.Background()

	_, err := col.InsertMany(ctx, []interface{}{
		makeNumEntity(9, "id9"),
		makeNumEntity(10, "id10"),
		makeNumEntity(2, "id2"),
	})
	require.NoError(t, err)

	repo, err := NewEntityRepository(ctx, col)
	require.NoError(t, err)

	asc, err := repo.List(ctx, "fake", nil, ember.Asc("n").Numeric())
	require.NoError(t, err)
	require.Equal(t, []float64{2, 9, 10}, nNumbers(asc))

	desc, err := repo.List(ctx, "fake", nil, ember.Desc("n").Numeric())
	require.NoError(t, err)
	require.Equal(t, []float64{10, 9, 2}, nNumbers(desc))
}
```

- [ ] **Step 2: Run test to verify it passes for the right reason**

Run: `docker compose up -d mongo` (if not already running), then
`go test ./mongo/ -run 'TestMongoReachable|TestListSortNumeric' -v`
Expected: PASS. If it SKIPs, mongo is not reachable and this task has verified nothing — start mongo and rerun. `TestMongoReachable` fails loudly in that case.

- [ ] **Step 3: No implementation needed**

Mongo's `find` sort compares by BSON type, so numbers already order numerically. Confirm no change is needed to `mongo/entity_repository.go` — this step is a deliberate no-op.

- [ ] **Step 4: Run the whole mongo package**

Run: `go test ./mongo/ -v`
Expected: PASS, no skips.

- [ ] **Step 5: Commit**

```bash
git add mongo/sort_test.go
git commit -m "test(mongo): pin native numeric ordering

The existing sort test stored n as a string, so nothing covered the case
where mongo's BSON ordering diverges from postgres text extraction. That
divergence is the reason Sort.Ordering exists; mongo needs no code change
to honor it, which is only safe to rely on if a test says so."
```

---

## Task 5: Page and Cursor core

**Files:**
- Create: `page.go`
- Test: `page_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `type Cursor struct { Value any; ID string }`, `func (c Cursor) IsZero() bool`, `func (c Cursor) MarshalText() ([]byte, error)`, `func (c *Cursor) UnmarshalText(b []byte) error`
  - `type Page struct { Limit int; Offset int; Cursor Cursor }`, `func (p Page) IsZero() bool`, `func (p Page) Validate() error`
  - `func Unpaged() Page`, `func Limit(n int) Page`, `func (p Page) Skip(n int) Page`, `func (p Page) After(value any, id string) Page`, `func (p Page) AfterCursor(c Cursor) Page`
  - `var ErrInvalidPage`, `var ErrInvalidCursor`, `var ErrUnsupportedPage`

The `Page` field holding the cursor is named `Cursor`, not `After` — a method and field cannot share a name in Go, and `After` is the better method name.

- [ ] **Step 1: Write the failing test**

Create `page_test.go`:

```go
package ember

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageConstructors(t *testing.T) {
	assert.Equal(t, Page{}, Unpaged())
	assert.True(t, Unpaged().IsZero())
	assert.Equal(t, Page{Limit: 20}, Limit(20))
	assert.Equal(t, Page{Limit: 20, Offset: 40}, Limit(20).Skip(40))
	assert.Equal(t, Page{Limit: 20, Cursor: Cursor{Value: int64(7), ID: "x"}}, Limit(20).After(int64(7), "x"))
	assert.Equal(t, Page{Limit: 20, Cursor: Cursor{ID: "x"}}, Limit(20).After(nil, "x"))
	assert.Equal(t, Page{Limit: 20, Cursor: Cursor{Value: "a", ID: "x"}}, Limit(20).AfterCursor(Cursor{Value: "a", ID: "x"}))
	assert.False(t, Limit(20).IsZero())
}

func TestPageValidate(t *testing.T) {
	require.NoError(t, Unpaged().Validate())
	require.NoError(t, Limit(10).Validate())
	require.NoError(t, Limit(10).Skip(20).Validate())
	require.NoError(t, Limit(10).After(int64(1), "x").Validate())

	require.ErrorIs(t, Limit(-1).Validate(), ErrInvalidPage)
	require.ErrorIs(t, Page{Limit: 10, Offset: -1}.Validate(), ErrInvalidPage)
	require.ErrorIs(t, Limit(10).Skip(5).After(int64(1), "x").Validate(), ErrInvalidPage)
	require.ErrorIs(t, Page{Offset: 5}.Validate(), ErrInvalidPage)
	require.ErrorIs(t, Page{Cursor: Cursor{ID: "x"}}.Validate(), ErrInvalidPage)
}

func TestCursorIsZero(t *testing.T) {
	assert.True(t, Cursor{}.IsZero())
	assert.True(t, Cursor{Value: "a"}.IsZero())
	assert.False(t, Cursor{ID: "x"}.IsZero())
}

func TestCursorTextRoundTrip(t *testing.T) {
	ts := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		in   Cursor
		want Cursor
	}{
		{"unsorted", Cursor{ID: "pay_abc"}, Cursor{ID: "pay_abc"}},
		{"string", Cursor{Value: "2026-08-09", ID: "a"}, Cursor{Value: "2026-08-09", ID: "a"}},
		{"string with separator", Cursor{Value: "a|b|c", ID: "x|y"}, Cursor{Value: "a|b|c", ID: "x|y"}},
		{"int", Cursor{Value: 7, ID: "a"}, Cursor{Value: int64(7), ID: "a"}},
		{"int64 beyond float precision", Cursor{Value: int64(9007199254740993), ID: "a"}, Cursor{Value: int64(9007199254740993), ID: "a"}},
		{"float", Cursor{Value: 1.5, ID: "a"}, Cursor{Value: 1.5, ID: "a"}},
		{"time", Cursor{Value: ts, ID: "a"}, Cursor{Value: ts.Format(time.RFC3339Nano), ID: "a"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := tt.in.MarshalText()
			require.NoError(t, err)

			var got Cursor
			require.NoError(t, got.UnmarshalText(b))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCursorTextRejectsGarbage(t *testing.T) {
	var c Cursor
	require.ErrorIs(t, c.UnmarshalText([]byte("not base64 $$$")), ErrInvalidCursor)
	require.ErrorIs(t, c.UnmarshalText([]byte("")), ErrInvalidCursor)
}

func TestCursorMarshalRejectsUnsupportedValue(t *testing.T) {
	_, err := Cursor{Value: []string{"a"}, ID: "x"}.MarshalText()
	require.ErrorIs(t, err, ErrInvalidCursor)
}

func TestCursorSurvivesJSON(t *testing.T) {
	type resp struct {
		Next Cursor `json:"next"`
	}

	b, err := json.Marshal(resp{Next: Cursor{Value: int64(42), ID: "pay_abc"}})
	require.NoError(t, err)

	var got resp
	require.NoError(t, json.Unmarshal(b, &got))
	assert.Equal(t, Cursor{Value: int64(42), ID: "pay_abc"}, got.Next)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./ -run 'TestPage|TestCursor' -v`
Expected: FAIL — `undefined: Unpaged`, `undefined: Cursor`, `undefined: Page`

- [ ] **Step 3: Write minimal implementation**

Create `page.go`:

```go
package ember

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

var (
	ErrInvalidPage     = errors.New("ember: invalid page")
	ErrInvalidCursor   = errors.New("ember: invalid cursor")
	ErrUnsupportedPage = errors.New("ember: unsupported page")
)

// Cursor
type Cursor struct {
	Value any
	ID    string
}

func (c Cursor) IsZero() bool { return c.ID == "" }

// cursorWire
type cursorWire struct {
	Kind  string `json:"k,omitempty"`
	Value string `json:"v,omitempty"`
	ID    string `json:"i"`
}

func (c Cursor) MarshalText() ([]byte, error) {
	w := cursorWire{ID: c.ID}
	switch x := c.Value.(type) {
	case nil:
	case string:
		w.Kind, w.Value = "s", x
	case time.Time:
		w.Kind, w.Value = "s", x.UTC().Format(time.RFC3339Nano)
	case bool:
		w.Kind, w.Value = "s", strconv.FormatBool(x)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		w.Kind, w.Value = "n", fmt.Sprintf("%d", x)
	case float32, float64:
		w.Kind, w.Value = "n", fmt.Sprintf("%v", x)
	default:
		return nil, fmt.Errorf("%w: value type %T", ErrInvalidCursor, c.Value)
	}

	raw, err := json.Marshal(w)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}

	return []byte(base64.RawURLEncoding.EncodeToString(raw)), nil
}

func (c *Cursor) UnmarshalText(b []byte) error {
	raw, err := base64.RawURLEncoding.DecodeString(string(b))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}

	var w cursorWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}
	if w.ID == "" {
		return fmt.Errorf("%w: missing id", ErrInvalidCursor)
	}

	switch w.Kind {
	case "":
		c.Value = nil
	case "s":
		c.Value = w.Value
	case "n":
		if i, err := strconv.ParseInt(w.Value, 10, 64); err == nil {
			c.Value = i
			break
		}
		f, err := strconv.ParseFloat(w.Value, 64)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidCursor, err)
		}
		c.Value = f
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidCursor, w.Kind)
	}

	c.ID = w.ID

	return nil
}

// Page
type Page struct {
	Limit  int
	Offset int
	Cursor Cursor
}

func Unpaged() Page { return Page{} }

func Limit(n int) Page { return Page{Limit: n} }

func (p Page) Skip(n int) Page {
	p.Offset = n
	return p
}

func (p Page) After(value any, id string) Page {
	p.Cursor = Cursor{Value: value, ID: id}
	return p
}

func (p Page) AfterCursor(c Cursor) Page {
	p.Cursor = c
	return p
}

func (p Page) IsZero() bool {
	return p.Limit == 0 && p.Offset == 0 && p.Cursor.IsZero()
}

func (p Page) Validate() error {
	if p.Limit < 0 {
		return fmt.Errorf("%w: negative limit %d", ErrInvalidPage, p.Limit)
	}
	if p.Offset < 0 {
		return fmt.Errorf("%w: negative offset %d", ErrInvalidPage, p.Offset)
	}
	if p.Offset > 0 && !p.Cursor.IsZero() {
		return fmt.Errorf("%w: offset and cursor are mutually exclusive", ErrInvalidPage)
	}
	if p.Limit == 0 && (p.Offset > 0 || !p.Cursor.IsZero()) {
		return fmt.Errorf("%w: offset or cursor requires a limit", ErrInvalidPage)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./ -run 'TestPage|TestCursor' -v`
Expected: PASS, all subtests.

- [ ] **Step 5: Commit**

```bash
git add page.go page_test.go
git commit -m "feat: add Page and Cursor

Page carries a limit plus either an offset or a cursor. Offset paging suits
numbered pages and jump-to-page; keyset paging costs O(limit) per page
regardless of depth and does not skip or duplicate rows when writes land
mid-pagination. Both are legitimate for different consumers, so ember
exposes both rather than choosing.

A cursor names a position in the total order (sort value, entity id) —
the sort value alone cannot, because ties straddling a page boundary would
either skip rows (>) or repeat them forever (>=).

The codec lives here rather than in each service's HTTP layer: Value's Go
type is load-bearing for the comparison, and a JSON round trip through a
handler erases it. json+base64url rather than a delimiter so a value or id
containing the separator cannot corrupt the token, and ParseInt before
ParseFloat so an int64 past 2^53 survives."
```

---

## Task 6: Thread Page through the repository interface, loader, and store

**Files:**
- Modify: `entity.go:59`
- Modify: `loader.go:24-42`
- Modify: `store.go:24-26`
- Modify: `mocks_test.go:28-35`
- Modify: `loader_test.go:52,55,65,75`
- Modify: `store_test.go:58,61`
- Modify: `mongo/entity_repository.go:100`, `postgres/entity_repository.go:89`, `dynamo/entity_repository.go:85`, `embertest/repository.go:63` — signature only

**Interfaces:**
- Consumes: `Page`, `Unpaged`, `Page.Validate` from Task 5.
- Produces:
  - `EntityRepository.List(ctx context.Context, typ string, f Filter, s Sort, p Page) ([]*MarshaledEntity, error)`
  - `func (l *EntityLoader[E]) ListPage(ctx context.Context, f Filter, sort Sort, p Page) ([]E, error)`
  - `func (s *EntityStore[E]) ListPage(ctx context.Context, f Filter, sort Sort, p Page) ([]E, error)`
  - `List` on both keeps its existing signature.

This task adds the parameter everywhere and makes the module compile and pass, with each backend still ignoring `p`. Tasks 7-10 make each backend honor it. That keeps every commit green.

- [ ] **Step 1: Write the failing test**

Add to `loader_test.go` (it uses `testify/suite`; the suite type is `loaderSuite` with fields `repo`, `marshaler`, `loader`, `ctx` — match the existing methods' style):

```go
func (s *loaderSuite) TestListPassesUnpaged() {
	f := Eq("k", "v")
	m1 := &MarshaledEntity{ID: "1", Type: "fake"}
	s.repo.On("List", mock.Anything, "fake", f, Sort{}, Unpaged()).Return([]*MarshaledEntity{m1}, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m1).Return(&fakeEntity{id: "1"}, nil)

	_, err := s.loader.List(s.ctx, f, Sort{})

	s.Require().NoError(err)
	s.repo.AssertExpectations(s.T())
}

func (s *loaderSuite) TestListPagePassesPageThrough() {
	f := Eq("k", "v")
	p := Limit(2).After(int64(7), "1")
	m1 := &MarshaledEntity{ID: "2", Type: "fake"}
	s.repo.On("List", mock.Anything, "fake", f, Asc("seq").Numeric(), p).Return([]*MarshaledEntity{m1}, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m1).Return(&fakeEntity{id: "2"}, nil)

	got, err := s.loader.ListPage(s.ctx, f, Asc("seq").Numeric(), p)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.repo.AssertExpectations(s.T())
}

func (s *loaderSuite) TestListPageRejectsInvalidPage() {
	_, err := s.loader.ListPage(s.ctx, nil, Unsorted(), Page{Offset: 5})

	s.Require().ErrorIs(err, ErrInvalidPage)
	s.repo.AssertNotCalled(s.T(), "List")
}
```

Read `loader_test.go` first and match the actual suite receiver name and entity double — the names above assume `loaderSuite`, `s.repo`, `s.marshaler`, `s.loader`, `s.ctx`, and a `fakeEntity` double. Use whatever that file already defines.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./ -run TestLoader -v`
Expected: FAIL — `s.loader.ListPage undefined`, and the `List` expectations fail on argument count.

- [ ] **Step 3: Write minimal implementation**

`entity.go` — change the `EntityRepository` interface's `List`:

```go
	List(ctx context.Context, typ string, f Filter, s Sort, p Page) ([]*MarshaledEntity, error)
```

`loader.go` — replace `List` with:

```go
func (l *EntityLoader[E]) List(ctx context.Context, f Filter, sort Sort) ([]E, error) {
	return l.ListPage(ctx, f, sort, Unpaged())
}

func (l *EntityLoader[E]) ListPage(ctx context.Context, f Filter, sort Sort, p Page) ([]E, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}

	var empty E
	ms, err := l.repository.List(ctx, empty.Type(), f, sort, p)
	if err != nil {
		return nil, err
	}

	es := make([]E, 0, len(ms))
	for _, m := range ms {
		e, err := l.marshaler.Unmarshal(ctx, m)
		if err != nil {
			return nil, err
		}

		es = append(es, e)
	}

	return es, nil
}
```

`store.go` — add below `List`:

```go
func (s *EntityStore[E]) ListPage(ctx context.Context, f Filter, sort Sort, p Page) ([]E, error) {
	return s.loader.ListPage(ctx, f, sort, p)
}
```

`mocks_test.go` — update the mock:

```go
func (m *mockEntityRepository) List(ctx context.Context, typ string, f Filter, s Sort, p Page) ([]*MarshaledEntity, error) {
	args := m.Called(ctx, typ, f, s, p)
	var out []*MarshaledEntity
	if v := args.Get(0); v != nil {
		out = v.([]*MarshaledEntity)
	}
	return out, args.Error(1)
}
```

Every existing `s.repo.On("List", mock.Anything, "fake", f, Sort{})` in `loader_test.go` and `store_test.go` gains a fifth argument `Unpaged()`.

Then add the parameter to all four backends without using it yet — `mongo/entity_repository.go:100`, `postgres/entity_repository.go:89`, `dynamo/entity_repository.go:85`, `embertest/repository.go:63`:

```go
func (r *EntityRepository) List(ctx context.Context, typ string, f ember.Filter, s ember.Sort, _ ember.Page) ([]*ember.MarshaledEntity, error) {
```

(For `embertest`, the receiver's context parameter is already `_`; keep it as `_ context.Context`.)

- [ ] **Step 4: Run the whole module**

Run: `go build ./... && go test ./... -short`
Expected: PASS. Then with mongo and postgres up: `go test ./...` — PASS, no skips.

- [ ] **Step 5: Commit**

```bash
git add entity.go loader.go store.go mocks_test.go loader_test.go store_test.go mongo/entity_repository.go postgres/entity_repository.go dynamo/entity_repository.go embertest/repository.go
git commit -m "feat: thread Page through EntityRepository.List

List keeps its signature on the loader and store and delegates with
Unpaged(), so no call site changes and paging is opt-in via ListPage.
Validation lives in the loader so each backend does not repeat it.

Backends take the parameter and ignore it here; the following commits make
each one honor it, which keeps every commit green."
```

---

## Task 7: Mongo paging

**Files:**
- Modify: `mongo/sort.go`
- Modify: `mongo/entity_repository.go:100-148`
- Test: `mongo/entity_repository_test.go`

**Interfaces:**
- Consumes: `ember.Page`, `ember.Cursor`, `ember.ErrInvalidCursor` from Task 5; `field(path string) string` from `mongo/filter.go:93`; `normalizeValue` from `mongo/filter.go:130`.
- Produces: `sortDoc(s ember.Sort, paged bool) bson.D`, `seekPredicate(s ember.Sort, c ember.Cursor) (bson.D, error)`.

Behavior: any non-zero `Page` appends `entity_id` as a final sort key (ascending or descending to match the primary direction), and an `Unsorted()` paged List orders by `entity_id` alone. Paging an unordered result is undefined, so paging imposes a deterministic order. `Ordering` needs no handling — mongo compares by BSON type natively.

- [ ] **Step 1: Write the failing test**

Append to `mongo/entity_repository_test.go`. This file uses a suite (`s.repo`, `s.collection`); read it first and match. If the paging tests fit better as plain functions alongside `mongo/sort_test.go`'s style, put them there instead — but keep them in one of the two existing files, not a new one.

```go
func TestListPagingLimitAndSkip(t *testing.T) {
	col := connectTestMongo(t)
	ctx := context.Background()

	_, err := col.InsertMany(ctx, []interface{}{
		makeNumEntity(1, "id1"),
		makeNumEntity(2, "id2"),
		makeNumEntity(3, "id3"),
	})
	require.NoError(t, err)

	repo, err := NewEntityRepository(ctx, col)
	require.NoError(t, err)

	first, err := repo.List(ctx, "fake", nil, ember.Asc("n").Numeric(), ember.Limit(2))
	require.NoError(t, err)
	require.Equal(t, []float64{1, 2}, nNumbers(first))

	second, err := repo.List(ctx, "fake", nil, ember.Asc("n").Numeric(), ember.Limit(2).Skip(2))
	require.NoError(t, err)
	require.Equal(t, []float64{3}, nNumbers(second))
}

func TestListPagingUnsortedKeyset(t *testing.T) {
	col := connectTestMongo(t)
	ctx := context.Background()

	_, err := col.InsertMany(ctx, []interface{}{
		makeNumEntity(1, "id1"),
		makeNumEntity(2, "id2"),
		makeNumEntity(3, "id3"),
	})
	require.NoError(t, err)

	repo, err := NewEntityRepository(ctx, col)
	require.NoError(t, err)

	first, err := repo.List(ctx, "fake", nil, ember.Unsorted(), ember.Limit(2))
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.Equal(t, []string{"id1", "id2"}, []string{first[0].ID, first[1].ID})

	second, err := repo.List(ctx, "fake", nil, ember.Unsorted(), ember.Limit(2).After(nil, first[1].ID))
	require.NoError(t, err)
	require.Equal(t, []string{"id3"}, []string{second[0].ID})
}

func TestListPagingKeysetAcrossTie(t *testing.T) {
	col := connectTestMongo(t)
	ctx := context.Background()

	_, err := col.InsertMany(ctx, []interface{}{
		makeNumEntity(5, "idA"),
		makeNumEntity(5, "idB"),
		makeNumEntity(5, "idC"),
		makeNumEntity(6, "idD"),
	})
	require.NoError(t, err)

	repo, err := NewEntityRepository(ctx, col)
	require.NoError(t, err)

	sort := ember.Asc("n").Numeric()
	var seen []string
	page := ember.Limit(2)
	for {
		got, err := repo.List(ctx, "fake", nil, sort, page)
		require.NoError(t, err)
		if len(got) == 0 {
			break
		}
		for _, m := range got {
			seen = append(seen, m.ID)
		}
		last := got[len(got)-1]
		var d map[string]float64
		require.NoError(t, json.Unmarshal(last.Data, &d))
		page = ember.Limit(2).After(d["n"], last.ID)
		if len(got) < 2 {
			break
		}
	}

	require.Equal(t, []string{"idA", "idB", "idC", "idD"}, seen)
}

func TestListPagingSortedCursorNeedsValue(t *testing.T) {
	col := connectTestMongo(t)
	ctx := context.Background()

	repo, err := NewEntityRepository(ctx, col)
	require.NoError(t, err)

	_, err = repo.List(ctx, "fake", nil, ember.Asc("n").Numeric(), ember.Limit(2).After(nil, "idA"))
	require.ErrorIs(t, err, ember.ErrInvalidCursor)
}
```

`TestListPagingKeysetAcrossTie` is the test that matters: three rows share `n = 5`, and the page boundary falls inside the tie. Without the `entity_id` tiebreak this either skips `idC` or loops forever on `idA`/`idB`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./mongo/ -run TestListPaging -v`
Expected: FAIL — limit/skip are ignored so `TestListPagingLimitAndSkip` returns all three rows, and the cursor tests fail on `undefined: seekPredicate` once you start wiring.

- [ ] **Step 3: Write minimal implementation**

Append to `mongo/sort.go`:

```go
func sortDoc(s ember.Sort, paged bool) bson.D {
	dir := sortAscending
	if s.Direction == ember.Descending {
		dir = sortDescending
	}

	switch {
	case s.Path == "" && !paged:
		return nil
	case s.Path == "":
		return bson.D{{Key: "entity_id", Value: sortAscending}}
	case !paged:
		return bson.D{{Key: field(s.Path), Value: dir}}
	default:
		return bson.D{{Key: field(s.Path), Value: dir}, {Key: "entity_id", Value: dir}}
	}
}

func seekPredicate(s ember.Sort, c ember.Cursor) (bson.D, error) {
	op := "$gt"
	if s.Direction == ember.Descending {
		op = "$lt"
	}

	tie := bson.D{{Key: "entity_id", Value: bson.D{{Key: op, Value: c.ID}}}}
	if s.Path == "" {
		return tie, nil
	}

	if c.Value == nil {
		return nil, fmt.Errorf("%w: sorted cursor requires a value", ember.ErrInvalidCursor)
	}

	v, err := normalizeValue(c.Value)
	if err != nil {
		return nil, err
	}

	f := field(s.Path)

	return bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: f, Value: bson.D{{Key: op, Value: v}}}},
		bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: f, Value: v}},
			tie,
		}}},
	}}}, nil
}
```

`mongo/sort.go` needs `import ("fmt"; "go.mongodb.org/mongo-driver/v2/bson"; "github.com/klemen-forstneric/ember")` added.

Replace `mongo/entity_repository.go` `List` lines 100-127 (through the `Find` call) with:

```go
func (r *EntityRepository) List(ctx context.Context, typ string, f ember.Filter, s ember.Sort, p ember.Page) ([]*ember.MarshaledEntity, error) {
	predicate, err := buildFilter(f)
	if err != nil {
		return nil, err
	}

	conds := bson.A{bson.D{{Key: "type", Value: typ}}}
	if len(predicate) > 0 {
		conds = append(conds, predicate)
	}
	if !p.Cursor.IsZero() {
		seek, err := seekPredicate(s, p.Cursor)
		if err != nil {
			return nil, err
		}
		conds = append(conds, seek)
	}

	filter := bson.D{{Key: "$and", Value: conds}}

	opts := options.Find()
	if doc := sortDoc(s, !p.IsZero()); doc != nil {
		opts.SetSort(doc)
	}
	if p.Limit > 0 {
		opts.SetLimit(int64(p.Limit))
	}
	if p.Offset > 0 {
		opts.SetSkip(int64(p.Offset))
	}

	cur, err := r.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
```

The cursor-decode loop below it is unchanged.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./mongo/ -v`
Expected: PASS, no skips (mongo must be up).

- [ ] **Step 5: Commit**

```bash
git add mongo/sort.go mongo/entity_repository.go mongo/entity_repository_test.go
git commit -m "feat(mongo): honor Page in List

Limit and Offset map to SetLimit/SetSkip. A cursor becomes an \$or over
(sort path > value) and (sort path == value AND entity_id > id), which is
the tuple comparison that makes a page boundary inside a tie safe — the
across-tie test walks four rows where three share a sort value and would
otherwise skip one or repeat two forever.

Any non-zero Page appends entity_id to the sort, and a paged Unsorted List
orders by entity_id alone: paging an unordered result is undefined, and
that key rides the existing (type, entity_id) unique index.

The predicate is built from the sort expression rather than the Filter AST
so the comparison and the sort cannot disagree."
```

---

## Task 8: Postgres paging

**Files:**
- Modify: `postgres/sort.go`
- Modify: `postgres/entity_repository.go:89-115`
- Test: `postgres/sort_test.go`

**Interfaces:**
- Consumes: `sortExpr`, `orderBy`, `direction` from Task 3; `ember.Page`, `ember.Cursor`, `ember.ErrInvalidCursor` from Task 5; `normalizeValue` from `postgres/filter.go:158`.
- Produces: `orderBy(s ember.Sort, paged bool) []string` — **signature change from Task 3**, all callers updated in this task — and `seekPredicate(s ember.Sort, c ember.Cursor) (sq.Sqlizer, error)`.

- [ ] **Step 1: Write the failing test**

In `postgres/sort_test.go`, update `TestOrderBy` to pass the new `paged` argument (`orderBy(tt.sort, false)`) and append:

```go
func TestOrderByPagedAppendsTiebreak(t *testing.T) {
	assert.Equal(t, []string{"id ASC"}, orderBy(ember.Unsorted(), true))
	assert.Equal(t, []string{"data#>>'{created_at}' ASC", "id ASC"}, orderBy(ember.Asc("created_at"), true))
	assert.Equal(t, []string{"(data#>>'{seq}')::numeric DESC", "id DESC"}, orderBy(ember.Desc("seq").Numeric(), true))
}

func TestSeekPredicate(t *testing.T) {
	tests := []struct {
		name     string
		sort     ember.Sort
		cursor   ember.Cursor
		wantSQL  string
		wantArgs []any
	}{
		{
			"unsorted",
			ember.Unsorted(),
			ember.Cursor{ID: "pay_abc"},
			"id > ?",
			[]any{"pay_abc"},
		},
		{
			"lexical asc",
			ember.Asc("settle_by"),
			ember.Cursor{Value: "2026-08-09", ID: "pay_abc"},
			"(data#>>'{settle_by}', id) > (?, ?)",
			[]any{"2026-08-09", "pay_abc"},
		},
		{
			"numeric desc",
			ember.Desc("seq").Numeric(),
			ember.Cursor{Value: int64(7), ID: "m1"},
			"((data#>>'{seq}')::numeric, id) < (?::numeric, ?)",
			[]any{int64(7), "m1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred, err := seekPredicate(tt.sort, tt.cursor)
			require.NoError(t, err)

			gotSQL, gotArgs, err := pred.ToSql()
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, gotSQL)
			assert.Equal(t, tt.wantArgs, gotArgs)
		})
	}
}

func TestSeekPredicateSortedCursorNeedsValue(t *testing.T) {
	_, err := seekPredicate(ember.Asc("seq").Numeric(), ember.Cursor{ID: "m1"})
	require.ErrorIs(t, err, ember.ErrInvalidCursor)
}
```

Add `"github.com/stretchr/testify/require"` to the imports of `postgres/sort_test.go`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./postgres/ -run 'TestOrderBy|TestSeekPredicate' -v`
Expected: FAIL — `too many arguments in call to orderBy`, `undefined: seekPredicate`

- [ ] **Step 3: Write minimal implementation**

In `postgres/sort.go`, replace `orderBy` and append `seekPredicate`:

```go
func orderBy(s ember.Sort, paged bool) []string {
	dir := direction(s.Direction)

	switch {
	case s.Path == "" && !paged:
		return nil
	case s.Path == "":
		return []string{"id ASC"}
	case !paged:
		return []string{sortExpr(s) + " " + dir}
	default:
		return []string{sortExpr(s) + " " + dir, "id " + dir}
	}
}

func seekPredicate(s ember.Sort, c ember.Cursor) (sq.Sqlizer, error) {
	op := ">"
	if s.Direction == ember.Descending {
		op = "<"
	}

	if s.Path == "" {
		return sq.Expr("id "+op+" ?", c.ID), nil
	}

	if c.Value == nil {
		return nil, fmt.Errorf("%w: sorted cursor requires a value", ember.ErrInvalidCursor)
	}

	v, err := normalizeValue(c.Value)
	if err != nil {
		return nil, err
	}

	placeholder := "?"
	if _, reserved := column(s.Path); !reserved && s.Ordering == ember.Numeric {
		placeholder = "?::numeric"
	}

	return sq.Expr("("+sortExpr(s)+", id) "+op+" ("+placeholder+", ?)", v, c.ID), nil
}
```

`postgres/sort.go` imports become `("fmt"; sq "github.com/Masterminds/squirrel"; "github.com/klemen-forstneric/ember")`.

In `postgres/entity_repository.go`, replace the `List` body from the `qb :=` line through `ToSql()`:

```go
func (r *EntityRepository) List(ctx context.Context, typ string, f ember.Filter, s ember.Sort, p ember.Page) ([]*ember.MarshaledEntity, error) {
	pred, err := buildPredicate(f)
	if err != nil {
		return nil, err
	}

	qb := psql.
		Select("id", "version", "data").
		From(r.table).
		Where(sq.Eq{"type": typ})

	if pred != nil {
		qb = qb.Where(pred)
	}
	if !p.Cursor.IsZero() {
		seek, err := seekPredicate(s, p.Cursor)
		if err != nil {
			return nil, err
		}
		qb = qb.Where(seek)
	}
	if clauses := orderBy(s, !p.IsZero()); len(clauses) > 0 {
		qb = qb.OrderBy(clauses...)
	}
	if p.Limit > 0 {
		qb = qb.Limit(uint64(p.Limit))
	}
	if p.Offset > 0 {
		qb = qb.Offset(uint64(p.Offset))
	}

	query, args, err := qb.ToSql()
	if err != nil {
		return nil, err
	}
```

The row-scan loop below is unchanged.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./postgres/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add postgres/sort.go postgres/sort_test.go postgres/entity_repository.go
git commit -m "feat(postgres): honor Page in List

Limit and Offset map to LIMIT/OFFSET. A cursor becomes a row-tuple
comparison, (sort expression, id) > (value, id), against the same
expression the ORDER BY uses — including the ::numeric cast — so the
comparison and the ordering cannot disagree. The placeholder is cast too
so the row comparison does not depend on postgres inferring the operand
type from the left side.

A row whose sort path is NULL falls out of the tuple comparison and is
skipped by keyset paging. mongo and postgres already disagreed on where
such rows sort; documenting that is cheaper than defining it."
```

---

## Task 9: embertest paging

**Files:**
- Modify: `embertest/repository.go:63-83`
- Modify: `embertest/sort.go`
- Test: `embertest/repository_test.go`

**Interfaces:**
- Consumes: `lessThan` from Task 2; `lookup` from `embertest/filter.go:73`; `ember.Page`, `ember.Cursor`, `ember.ErrInvalidCursor` from Task 5.
- Produces: `applySort(items []*ember.MarshaledEntity, s ember.Sort, paged bool)` — **signature change from Task 2** — and `seek(items []*ember.MarshaledEntity, s ember.Sort, c ember.Cursor) ([]*ember.MarshaledEntity, error)`.

The fake must mirror the real backends: impose the `entity_id` tiebreak when paged, drop rows at or before the cursor, then offset, then limit.

- [ ] **Step 1: Write the failing test**

Append to `embertest/repository_test.go`:

```go
func TestListPagingLimitAndOffset(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("a", "t", 1, `{"n":1}`)))
	require.NoError(t, r.Save(ctx, me("b", "t", 1, `{"n":2}`)))
	require.NoError(t, r.Save(ctx, me("c", "t", 1, `{"n":3}`)))

	sort := ember.Asc("n").Numeric()

	first, err := r.List(ctx, "t", nil, sort, ember.Limit(2))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, ids(first))

	second, err := r.List(ctx, "t", nil, sort, ember.Limit(2).Skip(2))
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, ids(second))

	past, err := r.List(ctx, "t", nil, sort, ember.Limit(2).Skip(99))
	require.NoError(t, err)
	assert.Empty(t, past)
}

func TestListPagingKeysetAcrossTie(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("idA", "t", 1, `{"n":5}`)))
	require.NoError(t, r.Save(ctx, me("idB", "t", 1, `{"n":5}`)))
	require.NoError(t, r.Save(ctx, me("idC", "t", 1, `{"n":5}`)))
	require.NoError(t, r.Save(ctx, me("idD", "t", 1, `{"n":6}`)))

	sort := ember.Asc("n").Numeric()

	first, err := r.List(ctx, "t", nil, sort, ember.Limit(2))
	require.NoError(t, err)
	assert.Equal(t, []string{"idA", "idB"}, ids(first))

	second, err := r.List(ctx, "t", nil, sort, ember.Limit(2).After(float64(5), "idB"))
	require.NoError(t, err)
	assert.Equal(t, []string{"idC", "idD"}, ids(second))

	third, err := r.List(ctx, "t", nil, sort, ember.Limit(2).After(float64(6), "idD"))
	require.NoError(t, err)
	assert.Empty(t, third)
}

func TestListPagingUnsortedKeyset(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("a", "t", 1, `{}`)))
	require.NoError(t, r.Save(ctx, me("b", "t", 1, `{}`)))
	require.NoError(t, r.Save(ctx, me("c", "t", 1, `{}`)))

	first, err := r.List(ctx, "t", nil, ember.Unsorted(), ember.Limit(2))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, ids(first))

	second, err := r.List(ctx, "t", nil, ember.Unsorted(), ember.Limit(2).After(nil, "b"))
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, ids(second))
}

func TestListPagingSortedCursorNeedsValue(t *testing.T) {
	r := New()
	ctx := context.Background()
	require.NoError(t, r.Save(ctx, me("a", "t", 1, `{"n":1}`)))

	_, err := r.List(ctx, "t", nil, ember.Asc("n").Numeric(), ember.Limit(2).After(nil, "a"))
	require.ErrorIs(t, err, ember.ErrInvalidCursor)
}
```

Add this helper next to `me` at the top of `embertest/repository_test.go`:

```go
func ids(ms []*ember.MarshaledEntity) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}
```

Note the cursor values are `float64` — the fake reads sort values out of `Data` via `json.Unmarshal` into `map[string]any`, so a stored `5` looks up as `float64(5)`. `orderJSON` coerces both sides through `toFloat`, so an `int64(5)` cursor also works; the tests use `float64` to match what a caller would get back from the fake.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./embertest/ -run TestListPaging -v`
Expected: FAIL — `p` is ignored, so every call returns all rows.

- [ ] **Step 3: Write minimal implementation**

In `embertest/sort.go`, change `applySort` to take `paged` and add the tiebreak, then add `seek`/`afterCursor`:

```go
func applySort(items []*ember.MarshaledEntity, s ember.Sort, paged bool) {
	if s.Path == "" {
		if paged {
			sort.SliceStable(items, func(i, j int) bool { return items[i].ID < items[j].ID })
		}
		return
	}

	sort.SliceStable(items, func(i, j int) bool {
		vi, oki, _ := lookup(items[i], s.Path)
		vj, okj, _ := lookup(items[j], s.Path)
		if !oki || !okj {
			return oki && !okj
		}
		if lessThan(vi, vj, s.Ordering) {
			return s.Direction != ember.Descending
		}
		if lessThan(vj, vi, s.Ordering) {
			return s.Direction == ember.Descending
		}
		if !paged {
			return false
		}
		if s.Direction == ember.Descending {
			return items[j].ID < items[i].ID
		}
		return items[i].ID < items[j].ID
	})
}

func seek(items []*ember.MarshaledEntity, s ember.Sort, c ember.Cursor) ([]*ember.MarshaledEntity, error) {
	if s.Path != "" && c.Value == nil {
		return nil, fmt.Errorf("%w: sorted cursor requires a value", ember.ErrInvalidCursor)
	}

	out := make([]*ember.MarshaledEntity, 0, len(items))
	for _, m := range items {
		if afterCursor(m, s, c) {
			out = append(out, m)
		}
	}

	return out, nil
}

func afterCursor(m *ember.MarshaledEntity, s ember.Sort, c ember.Cursor) bool {
	idAfter := m.ID > c.ID
	if s.Direction == ember.Descending {
		idAfter = m.ID < c.ID
	}

	if s.Path == "" {
		return idAfter
	}

	v, ok, err := lookup(m, s.Path)
	if err != nil || !ok {
		return false
	}

	if lessThan(v, c.Value, s.Ordering) {
		return s.Direction == ember.Descending
	}
	if lessThan(c.Value, v, s.Ordering) {
		return s.Direction != ember.Descending
	}

	return idAfter
}
```

`embertest/sort.go` gains `"fmt"` in its imports.

In `embertest/repository.go`, replace `List`:

```go
func (r *EntityRepository) List(_ context.Context, typ string, f ember.Filter, s ember.Sort, p ember.Page) ([]*ember.MarshaledEntity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []*ember.MarshaledEntity
	for _, m := range r.docs {
		if m.Type != typ {
			continue
		}
		ok, err := matches(f, m)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, clone(m))
		}
	}

	applySort(out, s, !p.IsZero())

	if !p.Cursor.IsZero() {
		seeked, err := seek(out, s, p.Cursor)
		if err != nil {
			return nil, err
		}
		out = seeked
	}
	if p.Offset > 0 {
		if p.Offset >= len(out) {
			return nil, nil
		}
		out = out[p.Offset:]
	}
	if p.Limit > 0 && len(out) > p.Limit {
		out = out[:p.Limit]
	}

	return out, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./embertest/ -v`
Expected: PASS, all tests including the ones from Task 2.

- [ ] **Step 5: Commit**

```bash
git add embertest/repository.go embertest/sort.go embertest/repository_test.go
git commit -m "feat(embertest): honor Page in the fake

The fake exists so service tests exercise the same semantics as the real
backends, which means it has to reproduce the tiebreak too: sort, drop
rows at or before the cursor, offset, limit. The across-tie test is the
one that catches a missing tiebreak — the same case as the mongo test.

Rows missing the sort path fall out of the cursor comparison, matching how
a NULL falls out of postgres's row-tuple comparison."
```

---

## Task 10: Dynamo paging

**Files:**
- Modify: `dynamo/entity_repository.go:85-127`
- Test: `dynamo/filter_test.go`

**Interfaces:**
- Consumes: `ember.Page`, `ember.ErrUnsupportedPage` from Task 5.
- Produces: no new exported names.

Dynamo has no server-side offset, and sorting by an arbitrary path is already `ErrUnsupportedSort`. What it can do natively: `Limit`, and an unsorted cursor via `ExclusiveStartKey` on its `(type, id)` key. Its per-page `Limit` applies before the filter, so the accumulated result still needs trimming.

- [ ] **Step 1: Write the failing test**

Append to `dynamo/filter_test.go`:

```go
func TestListRejectsOffset(t *testing.T) {
	repo := NewEntityRepository(nil, "entities")

	_, err := repo.List(context.Background(), "order", nil, ember.Unsorted(), ember.Limit(10).Skip(10))

	require.ErrorIs(t, err, ember.ErrUnsupportedPage)
}

func TestListRejectsSortedCursor(t *testing.T) {
	repo := NewEntityRepository(nil, "entities")

	_, err := repo.List(context.Background(), "order", nil, ember.Asc("seq"), ember.Limit(10).After(int64(1), "x"))

	require.ErrorIs(t, err, ember.ErrUnsupportedSort)
}
```

Both pass a `nil` client deliberately: the guards return before any AWS call, so no client is needed. Add `"context"` and `"github.com/stretchr/testify/require"` to that file's imports if absent.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./dynamo/ -run TestListRejects -v`
Expected: FAIL — offset is silently ignored, so the call proceeds to a nil-client `Query` and panics rather than returning `ErrUnsupportedPage`.

- [ ] **Step 3: Write minimal implementation**

In `dynamo/entity_repository.go`, replace `List`:

```go
func (r *EntityRepository) List(ctx context.Context, typ string, f ember.Filter, s ember.Sort, p ember.Page) ([]*ember.MarshaledEntity, error) {
	if s.Path != "" {
		return nil, ember.ErrUnsupportedSort
	}
	if p.Offset > 0 {
		return nil, fmt.Errorf("%w: offset", ember.ErrUnsupportedPage)
	}

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

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(r.table),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	if !p.Cursor.IsZero() {
		input.ExclusiveStartKey = map[string]types.AttributeValue{
			"type": &types.AttributeValueMemberS{Value: typ},
			"id":   &types.AttributeValueMemberS{Value: p.Cursor.ID},
		}
	}
	if p.Limit > 0 {
		input.Limit = aws.Int32(int32(p.Limit))
	}

	paginator := dynamodb.NewQueryPaginator(r.client, input)

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
		if p.Limit > 0 && len(out) >= p.Limit {
			break
		}
	}
	if p.Limit > 0 && len(out) > p.Limit {
		out = out[:p.Limit]
	}

	return out, nil
}
```

The `ErrUnsupportedSort` guard moves above the offset guard so a sorted cursor reports the sort problem, which is the root one.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./dynamo/ -v`
Expected: PASS.

- [ ] **Step 5: Run the whole module and commit**

Run: `go build ./... && go vet ./... && go test ./...` with mongo and postgres up.
Expected: PASS, no skips.

```bash
git add dynamo/entity_repository.go dynamo/filter_test.go
git commit -m "feat(dynamo): honor Limit and an unsorted cursor

List drained every page regardless of how many rows the caller wanted, so
a Limit is a strict improvement even though this backend has no users yet.
An unsorted cursor maps to ExclusiveStartKey because id is already the
table's sort key. Offset has no server-side equivalent and emulating it by
draining and discarding would be worse than not supporting it."
```

---

## Deferred, named

Not built by this plan. Each is additive and breaks no call site added here.

- **`Count(ctx, f) (int64, error)`** — needed only to render totals or page numbers. Costs a second round trip, and counting matches on a `data.*` path scans every match while the page itself fetches 20, inverting the cost of the request. `len(items) < Limit` covers prev/next and sweep termination.
- **Compound indexes for sorted keyset** — `(type, data.<sortPath>, entity_id)`. Without one, mongo does an in-memory sort bounded at 32MB. Unsorted keyset needs nothing new: it rides `EnsureEntities`'s `(type, entity_id)`.
- **Making `Page` mandatory on `List`** — the only way to hard-close the unbounded-read door, at the cost of touching all 47 fleet call sites.
- **`iter.Seq2[E, error]` sweep helper** that pages internally — Go 1.26 is available, nothing needs it yet.
- **Fleet migration at ember bump time** — the three call sites named in Global Constraints, plus moving `conversation-service`'s `ListByConversation` off its fetch-all-then-slice-in-Go tail and `payment-service`'s `RunSweep` onto a batched loop.

---

## Self-Review

**Spec coverage.** Typed `Sort` — Tasks 1-4 (core, embertest, postgres, mongo proof). `Page`/`Cursor` with both offset and keyset — Task 5. Cursor text codec + `ErrInvalidCursor` — Task 5. `ErrInvalidPage`, `ErrUnsupportedPage` — Task 5. Repo interface + `ListPage` on loader and store, `List` unchanged — Task 6. Per-backend support — Tasks 7-10. `Count` and indexes — deferred by decision, recorded above.

**Placeholder scan.** No TBD/TODO. Every code step carries the actual code. Two steps deliberately instruct reading an existing file first to match a suite receiver name (Task 6 Step 1, Task 7 Step 1) — that is a real instruction with a named fallback, not a placeholder.

**Type consistency.** `Page.Cursor` (field) vs `Page.After` (method) — deliberately different names, Go forbids sharing. `orderBy` gains its `paged` parameter in Task 8 and Task 8 updates Task 3's test in the same commit. `applySort` gains `paged` in Task 9 after Task 2 introduces it with two parameters. `lessThan` is defined in Task 2 and consumed in Task 9. `makeNumEntity`/`nNumbers` are defined in Task 4 and consumed in Task 7. `seekPredicate` exists in both `mongo` and `postgres` as separate unexported functions with different return types (`bson.D` vs `sq.Sqlizer`) — same name, different packages, intentional.
