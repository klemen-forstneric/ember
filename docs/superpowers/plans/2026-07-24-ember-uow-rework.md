# Ember Unit of Work — Rework Plan (Binding / Loader / Saver)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Replace the opt-in `Store[E]` + `save`/`Save` guard with the read/write split: `Binding[E]` → `EntityLoader[E]` (typed reads) + non-generic `EntitySaver` (type-erased atomic writes) + `EntityStore[E]` (convenience).

**Builds on (already committed on `feat/unit-of-work`, do NOT redo):** `Events` (events.go), `EntityRoot` + sealed `Entity.events()` (entity.go), `EventStore` + shared `envelopeBuilder` + refactored `Publisher` (event.go/publisher.go), `Transactor` + reentrant mongo impl (transactor.go, mongo/transactor.go).

**Supersedes:** the old `EntityStore[E]` snapshot type with `save`/`Save`/guard/`ErrUnpublishedEvents` (in entity.go), and the standalone `Store[E]` (store.go, store_test.go). These are removed/replaced by this rework.

**Spec:** `docs/superpowers/specs/2026-07-24-ember-uow-design.md` (revised).

## Global Constraints

- Module `github.com/klemen-forstneric/ember`; run `go` from the ember repo root.
- White-box tests (`package ember`); testify `suite` + `mock`; mocks unexported in `mocks_test.go`.
- No new external dependencies. Comments terse (user strongly prefers minimal comments) — one-liners only where non-obvious.
- `gofmt -l .` MUST be empty before every commit.
- TDD: failing test → see it fail → implement → see it pass → commit.
- Run `go build ./...` and `go test ./` (root package). Do NOT run `go test ./...` (subpackages need live infra and hang). `go vet ./mongo/` to check mongo compiles.
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

## File Structure (after rework)

- `entity.go` — `Entity`, `EntityRoot`, `MarshaledEntity`, `EntityMarshaler`, `EntityRepository`, `ErrEntityNotFound`, `ErrVersionConflict`. **The `EntityStore[E]` type and its `save`/`Save`/guard/`ErrUnpublishedEvents` are removed.**
- `binding.go` (new) — `Binding[E]`, `Bind`, sealed `binding`/`binder`.
- `loader.go` (new) — `EntityLoader[E]` + `NewEntityLoader` (the `Get`/`List` moved from the old `EntityStore`).
- `saver.go` (new) — `EntitySaver`, `NewEntitySaver`, `persist`, `ErrUnregisteredEntity`.
- `store.go` (replaced) — `EntityStore[E]` convenience (loader + saver). The old `Store[E]` is gone.

---

### Task R1: `Binding[E]` + `EntityLoader[E]`

**Files:**
- Create: `binding.go`, `loader.go`
- Modify: `entity.go` (remove `EntityStore[E]`, its `Get`/`List`/`save`/`Save`, and `ErrUnpublishedEvents`)
- Test: `loader_test.go` (new), `binding_test.go` (new); update `entity_test.go` (drop the old `EntityStoreSuite`)

**Interfaces:**
- Consumes: `Entity`, `EntityRepository`, `EntityMarshaler[E]`, `MarshaledEntity`, `Filter`, `Sort`, `ErrEntityNotFound`.
- Produces:
  - `Binding[E Entity]` (fields `repo EntityRepository`, `marshaler EntityMarshaler[E]`); `Bind[E Entity](r EntityRepository, m EntityMarshaler[E]) Binding[E]`.
  - unexported `binding struct { typ string; repo EntityRepository; marshal func(context.Context, Entity) (*MarshaledEntity, error) }`; `func (Binding[E]) binding() binding`; unexported `binder interface{ binding() binding }`.
  - `EntityLoader[E Entity]`; `NewEntityLoader[E Entity](b Binding[E]) *EntityLoader[E]`; `Get(ctx, id) (E, error)`; `List(ctx, Filter, Sort) ([]E, error)`.

- [ ] **Step 1: Remove the old `EntityStore[E]` from `entity.go`**

Delete from `entity.go`: the `EntityStore[E]` struct, `NewEntityStore`, its `Get`, `List`, `Save`, `save` methods, and the `ErrUnpublishedEvents` var. Keep `Entity`, `EntityRoot`, `MarshaledEntity`, `EntityMarshaler`, `EntityRepository`, `ErrEntityNotFound`, `ErrVersionConflict`. Delete the now-orphaned `EntityStoreSuite` and its helpers (`versionedRepo`, `versionMarshaler`, `TestEntityStoreRepeatedSaveOfSameInstance`, and the `EntityStoreSuite` tests) from `entity_test.go` — they will be re-homed on the loader/saver. Keep `fakeEntity`/`newFakeEntity`/`fakeEvent` and the `TestEntityRoot*` tests.

Run `go build ./` — it will fail to compile (store.go references the deleted `EntityStore`). That is expected; Step 5 and Task R3 remove those references. To keep the tree compiling between tasks, temporarily comment out `store.go`'s body is NOT allowed — instead do R1 and R3's `store.go` replacement in the same working session before running the full build. For the RED/GREEN of R1, scope test runs to the new files: `go test ./ -run 'TestEntityLoader|TestBinding'`.

- [ ] **Step 2: Write failing tests**

`binding_test.go`:

```go
package ember

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBindingReportsTypeAndMarshals(t *testing.T) {
	repo := &mockEntityRepository{}
	marshaler := &mockEntityMarshaler[*fakeEntity]{}
	b := Bind[*fakeEntity](repo, marshaler).binding()

	require.Equal(t, "fake", b.typ) // fakeEntity.Type() == "fake"

	e := newFakeEntity("1")
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	marshaler.On("Marshal", mock.Anything, e).Return(m, nil)
	got, err := b.marshal(context.Background(), e)
	require.NoError(t, err)
	require.Same(t, m, got)
	marshaler.AssertExpectations(t)
}
```

(add `"github.com/stretchr/testify/mock"` import.)

`loader_test.go` — the `Get`/`List` tests moved from the old `EntityStoreSuite`, now over an `EntityLoader`:

```go
package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type EntityLoaderSuite struct {
	suite.Suite
	ctx       context.Context
	repo      *mockEntityRepository
	marshaler *mockEntityMarshaler[*fakeEntity]
	loader    *EntityLoader[*fakeEntity]
}

func TestEntityLoaderSuite(t *testing.T) { suite.Run(t, new(EntityLoaderSuite)) }

func (s *EntityLoaderSuite) SetupTest() {
	s.ctx = context.Background()
	s.repo = &mockEntityRepository{}
	s.marshaler = &mockEntityMarshaler[*fakeEntity]{}
	s.loader = NewEntityLoader(Bind[*fakeEntity](s.repo, s.marshaler))
}

func (s *EntityLoaderSuite) TearDownTest() {
	s.repo.AssertExpectations(s.T())
	s.marshaler.AssertExpectations(s.T())
}

func (s *EntityLoaderSuite) TestGet() {
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(3), Data: []byte("alice")}
	e := newFakeEntity("1")
	e.Name = "alice"
	s.repo.On("Get", mock.Anything, "fake", "1").Return(m, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m).Return(e, nil)

	got, err := s.loader.Get(s.ctx, "1")

	s.Require().NoError(err)
	s.Equal(e, got)
}

func (s *EntityLoaderSuite) TestList() {
	m1 := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(3), Data: []byte("alice")}
	e1 := newFakeEntity("1")
	e1.Name = "alice"
	f := Eq("name", "alice")
	s.repo.On("List", mock.Anything, "fake", f, Sort{}).Return([]*MarshaledEntity{m1}, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m1).Return(e1, nil)

	got, err := s.loader.List(s.ctx, f, Sort{})

	s.Require().NoError(err)
	s.Equal([]*fakeEntity{e1}, got)
}

func (s *EntityLoaderSuite) TestListError() {
	sentinel := errors.New("boom")
	s.repo.On("List", mock.Anything, "fake", mock.Anything, mock.Anything).Return(nil, sentinel)

	_, err := s.loader.List(s.ctx, nil, Sort{})

	s.ErrorIs(err, sentinel)
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./ -run 'TestBinding|TestEntityLoaderSuite' -v`
Expected: FAIL — `undefined: Bind` / `undefined: NewEntityLoader`.

- [ ] **Step 4: Implement `binding.go` and `loader.go`**

`binding.go`:

```go
package ember

import "context"

// Binding declares how one entity type is persisted: its repository + marshaler.
// It is the single source both EntityLoader and EntitySaver are built from.
type Binding[E Entity] struct {
	repo      EntityRepository
	marshaler EntityMarshaler[E]
}

func Bind[E Entity](r EntityRepository, m EntityMarshaler[E]) Binding[E] {
	return Binding[E]{repo: r, marshaler: m}
}

// binding is the type-erased view the saver consumes.
type binding struct {
	typ     string
	repo    EntityRepository
	marshal func(ctx context.Context, e Entity) (*MarshaledEntity, error)
}

// binder is sealed: only ember's Binding[E] satisfies it.
type binder interface{ binding() binding }

func (b Binding[E]) binding() binding {
	var zero E
	return binding{
		typ:  zero.Type(),
		repo: b.repo,
		marshal: func(ctx context.Context, e Entity) (*MarshaledEntity, error) {
			return b.marshaler.Marshal(ctx, e.(E))
		},
	}
}
```

`loader.go`:

```go
package ember

import "context"

// EntityLoader reads entities of a single type.
type EntityLoader[E Entity] struct {
	repository EntityRepository
	marshaler  EntityMarshaler[E]
}

func NewEntityLoader[E Entity](b Binding[E]) *EntityLoader[E] {
	return &EntityLoader[E]{repository: b.repo, marshaler: b.marshaler}
}

func (l *EntityLoader[E]) Get(ctx context.Context, id string) (E, error) {
	var empty E
	m, err := l.repository.Get(ctx, empty.Type(), id)
	if err != nil {
		return empty, err
	}
	return l.marshaler.Unmarshal(ctx, m)
}

func (l *EntityLoader[E]) List(ctx context.Context, f Filter, sort Sort) ([]E, error) {
	var empty E
	ms, err := l.repository.List(ctx, empty.Type(), f, sort)
	if err != nil {
		return nil, err
	}
	out := make([]E, 0, len(ms))
	for _, m := range ms {
		e, err := l.marshaler.Unmarshal(ctx, m)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}
```

- [ ] **Step 5: Run the new tests**

Run: `go test ./ -run 'TestBinding|TestEntityLoaderSuite' -v`
Expected: PASS. (A full `go build ./` still fails until R3 replaces `store.go`; that's fine — do not commit yet if the package doesn't compile. Proceed directly into R2 and R3 in the same session, then build + commit at R3. If you prefer to commit R1 independently, first delete `store.go`/`store_test.go` here so the package compiles, and recreate the convenience `EntityStore` in R3.)

- [ ] **Step 6: Make the package compile, then commit**

Delete `store.go` and `store_test.go` (the old `Store[E]` is superseded; the convenience `EntityStore` is rebuilt in R3). Confirm `go build ./... && go test ./ -run 'TestBinding|TestEntityLoaderSuite|TestEvents|TestEntityRoot'` passes and `gofmt -l .` is empty.

```bash
git rm store.go store_test.go
git add binding.go loader.go entity.go entity_test.go binding_test.go loader_test.go
git commit -m "refactor(ember): add Binding + EntityLoader; remove old EntityStore/Store

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task R2: `EntitySaver`

**Files:**
- Create: `saver.go`
- Test: `saver_test.go` (new)
- Modify: `mocks_test.go` if a second fake entity type is needed (add `fakeEntity2`)

**Interfaces:**
- Consumes: `Entity`, `Version`, `NewVersion`, `binding`/`binder` (R1), `EventStore` + `EventStore.Save` (Task 4), `Transactor` (Task 5), `Entity.events()`.
- Produces:
  - `ErrUnregisteredEntity` (var).
  - `EntitySaver` struct; `NewEntitySaver(ev *EventStore, tx Transactor, bindings ...binder) *EntitySaver`.
  - `EntitySaver.Save(ctx context.Context, entities ...Entity) error`.
  - unexported `EntitySaver.persist(ctx, Entity) (Version, error)`.

- [ ] **Step 1: Add a second fake entity type for cross-type tests**

Append to `entity_test.go` (near `fakeEntity`):

```go
// fakeEntity2 is a second entity type, for cross-type saver tests.
type fakeEntity2 struct {
	EntityRoot
	Name string
}

func newFakeEntity2(id string) *fakeEntity2 { return &fakeEntity2{EntityRoot: NewEntityRoot(id)} }
func (e *fakeEntity2) Type() string         { return "fake2" }
```

- [ ] **Step 2: Write failing tests**

`saver_test.go`:

```go
package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type EntitySaverSuite struct {
	suite.Suite
	ctx         context.Context
	entityRepo  *mockEntityRepository
	entityMarsh *mockEntityMarshaler[*fakeEntity]
	eventRepo   *mockEventRepository
	eventMarsh  *mockEventMarshaler
	tx          *mockTransactor
	saver       *EntitySaver
}

func TestEntitySaverSuite(t *testing.T) { suite.Run(t, new(EntitySaverSuite)) }

func (s *EntitySaverSuite) SetupTest() {
	s.ctx = context.Background()
	s.entityRepo = &mockEntityRepository{}
	s.entityMarsh = &mockEntityMarshaler[*fakeEntity]{}
	s.eventRepo = &mockEventRepository{}
	s.eventMarsh = &mockEventMarshaler{}
	s.tx = &mockTransactor{}
	events := NewEventStore(stubIDer{id: "evt-1"}, s.eventRepo, NoopMetadataGetter{}, s.eventMarsh)
	s.saver = NewEntitySaver(events, s.tx, Bind[*fakeEntity](s.entityRepo, s.entityMarsh))
}

func (s *EntitySaverSuite) TearDownTest() {
	s.entityRepo.AssertExpectations(s.T())
	s.entityMarsh.AssertExpectations(s.T())
	s.eventRepo.AssertExpectations(s.T())
	s.eventMarsh.AssertExpectations(s.T())
	s.tx.AssertExpectations(s.T())
}

func (s *EntitySaverSuite) TestSaveSingleNoEventsSkipsTx() {
	e := newFakeEntity("1")
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	// tx.WithinTx must NOT be called (no expectations set) — asserted by TearDownTest.

	err := s.saver.Save(s.ctx, e)

	s.Require().NoError(err)
	s.Equal(uint64(1), e.Version().Value()) // version advanced post-write
}

func (s *EntitySaverSuite) TestSaveSingleWithEventsUsesTxAndClears() {
	e := newFakeEntity("1")
	evt := fakeEvent{entityID: "1", typ: "Created"}
	e.Emit(evt)
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	mev := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, evt).Return(mev, nil)
	s.eventRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	err := s.saver.Save(s.ctx, e)

	s.Require().NoError(err)
	s.Empty(e.events().All())
	s.Equal(uint64(1), e.Version().Value())
}

func (s *EntitySaverSuite) TestSaveUnregisteredType() {
	err := s.saver.Save(s.ctx, newFakeEntity2("1")) // fake2 not bound

	s.Require().ErrorIs(err, ErrUnregisteredEntity)
}

func (s *EntitySaverSuite) TestSaveEntityFailureLeavesEntityUntouched() {
	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	version := e.Version()
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(nil, errors.New("boom"))

	err := s.saver.Save(s.ctx, e)

	s.Require().Error(err)
	s.Equal(version, e.Version()) // no bump leaked
	s.Len(e.events().All(), 1)    // buffer intact for retry
}

func (s *EntitySaverSuite) TestSaveEventFailureLeavesEntityUntouched() {
	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	version := e.Version()
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).Return(nil, errors.New("event boom"))

	err := s.saver.Save(s.ctx, e)

	s.Require().Error(err)
	s.Equal(version, e.Version())
	s.Len(e.events().All(), 1)
}

func (s *EntitySaverSuite) TestSaveCommitErrorLeavesEntityUntouched() {
	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	version := e.Version()
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	mev := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	commitErr := errors.New("commit boom")
	s.tx.On("WithinTx", mock.Anything).Return(commitErr).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mev).Return(mev, nil).Maybe()
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).Return(mev, nil)
	s.eventRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	err := s.saver.Save(s.ctx, e)

	s.Require().ErrorIs(err, commitErr)
	s.Equal(version, e.Version())
	s.Len(e.events().All(), 1)
}
```

Cross-type multi-entity save (two bindings, one tx) — add to `saver_test.go`:

```go
func (s *EntitySaverSuite) TestSaveTwoTypesOneTx() {
	repo2 := &mockEntityRepository{}
	marsh2 := &mockEntityMarshaler[*fakeEntity2]{}
	events := NewEventStore(stubIDer{id: "evt-1"}, s.eventRepo, NoopMetadataGetter{}, s.eventMarsh)
	saver := NewEntitySaver(events, s.tx,
		Bind[*fakeEntity](s.entityRepo, s.entityMarsh),
		Bind[*fakeEntity2](repo2, marsh2),
	)
	e1 := newFakeEntity("1")
	e2 := newFakeEntity2("2")
	e1.Emit(fakeEvent{entityID: "1", typ: "A"})
	m1 := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	m2 := &MarshaledEntity{ID: "2", Type: "fake2", Version: NewVersion(1)}
	mev := &MarshaledEvent{Type: "A", Data: []byte(`{}`)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e1).Return(m1, nil)
	s.entityRepo.On("Save", mock.Anything, m1).Return(nil)
	marsh2.On("Marshal", mock.Anything, e2).Return(m2, nil)
	repo2.On("Save", mock.Anything, m2).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).Return(mev, nil)
	s.eventRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	err := saver.Save(s.ctx, e1, e2)

	s.Require().NoError(err)
	s.Empty(e1.events().All())
	s.Equal(uint64(1), e1.Version().Value())
	s.Equal(uint64(1), e2.Version().Value())
	repo2.AssertExpectations(s.T())
	marsh2.AssertExpectations(s.T())
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./ -run 'TestEntitySaverSuite' -v`
Expected: FAIL — `undefined: NewEntitySaver`.

- [ ] **Step 4: Implement `saver.go`**

```go
package ember

import (
	"context"
	"errors"
	"fmt"
)

var ErrUnregisteredEntity = errors.New("ember: no binding registered for entity type")

// EntitySaver persists any registered entity(ies) plus the events they produced,
// atomically. It holds no notifier: delivery is the outbox relay's job.
type EntitySaver struct {
	bindings map[string]binding
	events   *EventStore
	tx       Transactor
}

func NewEntitySaver(ev *EventStore, tx Transactor, bindings ...binder) *EntitySaver {
	m := make(map[string]binding, len(bindings))
	for _, b := range bindings {
		bd := b.binding()
		m[bd.typ] = bd
	}
	return &EntitySaver{bindings: m, events: ev, tx: tx}
}

func (s *EntitySaver) Save(ctx context.Context, entities ...Entity) error {
	var events []Event
	for _, e := range entities {
		events = append(events, e.events().All()...)
	}

	type pending struct {
		entity  Entity
		version Version
	}
	var pend []pending

	work := func(ctx context.Context) error {
		pend = pend[:0]
		for _, e := range entities {
			v, err := s.persist(ctx, e)
			if err != nil {
				return err
			}
			pend = append(pend, pending{e, v})
		}
		if len(events) > 0 {
			return s.events.Save(ctx, events...)
		}
		return nil
	}

	var err error
	if len(entities) == 1 && len(events) == 0 {
		err = work(ctx)
	} else {
		err = s.tx.WithinTx(ctx, work)
	}
	if err != nil {
		return err
	}

	for _, p := range pend {
		p.entity.SetVersion(p.version)
		p.entity.events().Clear()
	}
	return nil
}

// persist marshals the snapshot at the next version and writes it without
// permanently mutating e; it returns the version to adopt once the write is durable.
func (s *EntitySaver) persist(ctx context.Context, e Entity) (Version, error) {
	b, ok := s.bindings[e.Type()]
	if !ok {
		return Version{}, fmt.Errorf("%w: %s", ErrUnregisteredEntity, e.Type())
	}
	prev := e.Version()
	next := prev.Inc()
	e.SetVersion(next)
	m, err := b.marshal(ctx, e)
	e.SetVersion(prev)
	if err != nil {
		return prev, err
	}
	if err := b.repo.Save(ctx, m); err != nil {
		return prev, err
	}
	return NewVersion(next.Value()), nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./ -run 'TestEntitySaverSuite' -v`
Expected: PASS (all cases).

- [ ] **Step 6: Commit**

Confirm `gofmt -l .` empty.

```bash
git add saver.go saver_test.go entity_test.go
git commit -m "feat(ember): add type-erased EntitySaver (atomic entity + events)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task R3: `EntityStore[E]` convenience

**Files:**
- Create: `store.go` (fresh — the old `Store[E]` file was removed in R1)
- Test: `store_test.go` (fresh)

**Interfaces:**
- Consumes: `Bind`/`Binding[E]`, `EntityLoader[E]`/`NewEntityLoader` (R1), `EntitySaver`/`NewEntitySaver` (R2), `EventStore` (Task 4), `Transactor` (Task 5).
- Produces:
  - `EntityStore[E Entity]` struct (`loader *EntityLoader[E]`, `saver *EntitySaver`).
  - `NewEntityStore[E Entity](r EntityRepository, m EntityMarshaler[E], ev *EventStore, tx Transactor) *EntityStore[E]` — builds the binding, loader, and a single-type internal saver.
  - `Get(ctx, id) (E, error)`, `List(ctx, Filter, Sort) ([]E, error)`, `Save(ctx, e E) error`.

The store is self-contained: it constructs its own internal `EntitySaver` from `(ev, tx, Bind[E](r, m))`. The public `EntitySaver` (R2) remains for cross-type atomic saves.

- [ ] **Step 1: Write failing tests**

`store_test.go`:

```go
package ember

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type EntityStoreSuite struct {
	suite.Suite
	ctx        context.Context
	repo       *mockEntityRepository
	marshaler  *mockEntityMarshaler[*fakeEntity]
	eventRepo  *mockEventRepository
	eventMarsh *mockEventMarshaler
	tx         *mockTransactor
	store      *EntityStore[*fakeEntity]
}

func TestEntityStoreSuite(t *testing.T) { suite.Run(t, new(EntityStoreSuite)) }

func (s *EntityStoreSuite) SetupTest() {
	s.ctx = context.Background()
	s.repo = &mockEntityRepository{}
	s.marshaler = &mockEntityMarshaler[*fakeEntity]{}
	s.eventRepo = &mockEventRepository{}
	s.eventMarsh = &mockEventMarshaler{}
	s.tx = &mockTransactor{}
	events := NewEventStore(stubIDer{id: "evt-1"}, s.eventRepo, NoopMetadataGetter{}, s.eventMarsh)
	s.store = NewEntityStore[*fakeEntity](s.repo, s.marshaler, events, s.tx)
}

func (s *EntityStoreSuite) TearDownTest() {
	s.repo.AssertExpectations(s.T())
	s.marshaler.AssertExpectations(s.T())
	s.tx.AssertExpectations(s.T())
}

func (s *EntityStoreSuite) TestGetDelegatesToLoader() {
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(2)}
	e := newFakeEntity("1")
	s.repo.On("Get", mock.Anything, "fake", "1").Return(m, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m).Return(e, nil)

	got, err := s.store.Get(s.ctx, "1")

	s.Require().NoError(err)
	s.Equal(e, got)
}

func (s *EntityStoreSuite) TestSaveDelegatesToSaver() {
	e := newFakeEntity("1")
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.marshaler.On("Marshal", mock.Anything, e).Return(m, nil)
	s.repo.On("Save", mock.Anything, m).Return(nil)
	// no events -> no tx; tx has no expectations.

	err := s.store.Save(s.ctx, e)

	s.Require().NoError(err)
	s.Equal(uint64(1), e.Version().Value())
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./ -run 'TestEntityStoreSuite' -v`
Expected: FAIL — `undefined: NewEntityStore` (the old one was removed in R1).

- [ ] **Step 3: Implement `store.go`**

```go
package ember

import "context"

// EntityStore is a self-contained single-type convenience: typed reads plus an
// internal saver that persists the entity and its events atomically.
type EntityStore[E Entity] struct {
	loader *EntityLoader[E]
	saver  *EntitySaver
}

func NewEntityStore[E Entity](r EntityRepository, m EntityMarshaler[E], ev *EventStore, tx Transactor) *EntityStore[E] {
	b := Bind[E](r, m)
	return &EntityStore[E]{loader: NewEntityLoader(b), saver: NewEntitySaver(ev, tx, b)}
}

func (s *EntityStore[E]) Get(ctx context.Context, id string) (E, error) {
	return s.loader.Get(ctx, id)
}

func (s *EntityStore[E]) List(ctx context.Context, f Filter, sort Sort) ([]E, error) {
	return s.loader.List(ctx, f, sort)
}

func (s *EntityStore[E]) Save(ctx context.Context, e E) error {
	return s.saver.Save(ctx, e)
}
```

- [ ] **Step 4: Run tests + full build**

Run: `go build ./... && go test ./ -v && go vet ./mongo/`
Expected: whole root package PASSES; `go vet ./mongo/` clean. `gofmt -l .` empty.

- [ ] **Step 5: Commit**

```bash
git add store.go store_test.go
git commit -m "feat(ember): add EntityStore convenience (loader + saver)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

## Self-Review

**Spec coverage:** Binding/Bind + sealed binder (§5) → R1. EntityLoader (§6) → R1. EntitySaver + persist + ErrUnregisteredEntity + tx-when-needed + post-commit apply (§7) → R2. EntityStore convenience (§8) → R3. Old EntityStore/save/Save/guard/Store removed → R1. Events/EntityRoot/EventStore/Transactor unchanged (Tasks 1/2/4/5, committed).

**Placeholder scan:** none — every step has complete code and exact commands.

**Type consistency:** `Binding[E]`/`Bind`/`binding()`/`binder` consistent R1↔R2. `NewEntityLoader(Binding[E])`, `NewEntitySaver(*EventStore, Transactor, ...binder)`, `NewEntityStore(Binding[E], *EntitySaver)` consistent. `persist` returns `Version`; `Save` applies it post-commit. `ErrUnregisteredEntity` defined R2, exercised R2. Mocks (`mockEntityRepository`, `mockEntityMarshaler[E]`, `mockEventRepository`, `mockEventMarshaler`, `stubIDer`, `mockTransactor`) all pre-exist in `mocks_test.go`; `fakeEntity2` added R2.
