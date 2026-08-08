# Ember Event Ordering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the outbox's wall-clock ordering key (`seq = Timestamp.UnixNano()`) with a per-entity `(version, index)` key, and make the polling relay drain two-phase so high-version entities cannot starve.

**Architecture:** `EntitySaver` stamps every event with its entity's next version plus the event's index within that save; the envelope carries both; both outbox backends persist and sort on them. The relay stops asking for "the N oldest events" and instead samples entity ids at random, then drains each entity in version order. The WAL relay is untouched — WAL commit order already provides the invariant.

**Tech Stack:** Go, `testify/suite` + `testify/mock`, mongo-driver v2, `Masterminds/squirrel`, `DATA-DOG/go-sqlmock`.

**Spec:** `docs/superpowers/specs/2026-08-08-ember-event-ordering-design.md`

## Global Constraints

- Module is `github.com/klemen-forstneric/ember`. Work on branch `feat/event-ordering`.
- Tests use `testify/suite` with `SetupTest`, and `testify/mock` doubles. Mocks and fakes are unexported and live in `*_test.go`. New tests go in the canonical test file for the unit under test.
- Comments: none, except `// TypeName` labels above exported types and a terse one-liner where the code is genuinely non-obvious. Rationale belongs in the commit message.
- The storage field is `idx` on both backends. The Go envelope field is `Index`. Do not "unify" them.
- Version 0 is reserved for the unordered `Publisher.Publish` lane. Ordered events always have version >= 1.
- `go build ./... && go test ./...` must pass at the end of every task. Mongo integration tests skip when mongo is unavailable; that is a pass, not a failure.
- Do not add `entity_type` to the outbox. Do not expose `Version` on `ReceivedEvent` or any transport other than the WAL message. Both were explicitly rejected in the spec.

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `event.go` | `EventEnvelope`, `staged`, `envelopeBuilder` | Modify: add `Version`/`Index`, add `staged`, `build` takes `[]staged` |
| `saver.go` | `EntitySaver`, `ErrForeignEvent` | Modify: stamp version/index, enforce own-entity events |
| `publisher.go` | `Publisher.stage`, `Publisher.Publish` | Modify: `stage` takes `[]staged`; `Publish` stamps version 0 |
| `polling_relay.go` | relay config, repository contract, drain loop | Modify: two-knob config, new `ListUnpublished` signature, progress-based continuation |
| `mocks_test.go` | `mockEventRepository` | Modify: new `ListUnpublished` signature |
| `mongo/event_repository.go` | mongo outbox | Modify: `version`/`idx` fields, two-phase `ListUnpublished` |
| `mongo/ensure.go` | mongo index provisioning | Modify: swap the partial `seq` index |
| `postgres/event_repository.go` | postgres outbox | Modify: `version`/`idx` columns, CTE-based `ListUnpublished` |
| `postgres/wal/message.go` | WAL wire format | Modify: carry `version`/`idx` for lossless round-trip |
| `docs/superpowers/specs/2026-07-24-postgres-transactor-design.md` | postgres table doc | Modify: column table + `ListUnpublished` description |
| `docs/RUNBOOK-event-ordering.md` | migration runbook | Create |

Task order is chosen so nothing is left uncompilable between commits. Tasks 1-2 change only the `ember` package; the backend packages implement `PollingRelayRepository` structurally and are not referenced by `NewPollingRelay` anywhere inside the module, so they keep compiling until Tasks 3-4 update them.

---

### Task 1: Stamp events with `(version, index)`

**Files:**
- Modify: `event.go:26-33` (`EventEnvelope`), `event.go:58-86` (`envelopeBuilder`)
- Modify: `saver.go:9-11` (error vars), `saver.go:35-101` (`Save`)
- Modify: `publisher.go:23-46` (`stage`, `Publish`)
- Test: `saver_test.go`, `publisher_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `EventEnvelope.Version uint64`, `EventEnvelope.Index int`
  - `ember.ErrForeignEvent` (exported sentinel error)
  - unexported `staged{event Event; version uint64; index int}`
  - `(*Publisher).stage(ctx context.Context, events []staged) (delivery, error)`
  - `(envelopeBuilder).build(ctx context.Context, events []staged) ([]EventEnvelope, error)`

- [ ] **Step 1: Write the failing tests**

Add to `saver_test.go`:

```go
func (s *EntitySaverSuite) TestSaveStampsVersionAndIndexPerEntity() {
	repo2 := &mockEntityRepository{}
	marsh2 := &mockEntityMarshaler[*fakeEntity2]{}
	publisher := NewPublisher(stubIDer{id: "evt-1"}, NopMetadataGetter{}, s.eventMarsh, AtLeastOnce(s.eventRepo))
	saver := NewEntitySaver(publisher, s.tx, nil,
		Bind[*fakeEntity](s.entityRepo, s.entityMarsh),
		Bind[*fakeEntity2](repo2, marsh2),
	)

	e1 := newFakeEntity("1")
	e1.SetVersion(NewVersion(5))
	e1.Emit(fakeEvent{entityID: "1", typ: "A"}, fakeEvent{entityID: "1", typ: "B"})
	e2 := newFakeEntity2("2")
	e2.Emit(fakeEvent{entityID: "2", typ: "C"})

	m1 := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(6)}
	m2 := &MarshaledEntity{ID: "2", Type: "fake2", Version: NewVersion(1)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e1).Return(m1, nil)
	s.entityRepo.On("Save", mock.Anything, m1).Return(nil)
	marsh2.On("Marshal", mock.Anything, e2).Return(m2, nil)
	repo2.On("Save", mock.Anything, m2).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).
		Return(&MarshaledEvent{Type: "T", Data: []byte(`{}`)}, nil)

	var got []EventEnvelope
	s.eventRepo.On("Save", mock.Anything, mock.Anything).Return(nil).Once().
		Run(func(args mock.Arguments) { got = args.Get(1).([]EventEnvelope) })

	s.Require().NoError(saver.Save(s.ctx, e1, e2))

	s.Require().Len(got, 3)
	s.Equal([]uint64{6, 6, 1}, []uint64{got[0].Version, got[1].Version, got[2].Version})
	s.Equal([]int{0, 1, 0}, []int{got[0].Index, got[1].Index, got[2].Index})
	s.Equal([]string{"1", "1", "2"}, []string{got[0].EntityID, got[1].EntityID, got[2].EntityID})
	repo2.AssertExpectations(s.T())
	marsh2.AssertExpectations(s.T())
}

func (s *EntitySaverSuite) TestSaveForeignEventErrors() {
	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "2", typ: "A"})
	version := e.Version()

	err := s.saver.Save(s.ctx, e)

	s.Require().ErrorIs(err, ErrForeignEvent)
	s.Equal(version, e.Version())
	s.Len(e.events().All(), 1)
	s.entityRepo.AssertNotCalled(s.T(), "Save", mock.Anything, mock.Anything)
	s.eventRepo.AssertNotCalled(s.T(), "Save", mock.Anything, mock.Anything)
}
```

`TestSaveForeignEventErrors` deliberately sets no mock expectations: the check runs before the transaction opens, so nothing may be called. `TearDownTest` already asserts that.

Add to `publisher_test.go`:

```go
func (s *PublisherSuite) TestPublishStampsUnorderedLane() {
	evt1 := fakeEvent{entityID: "A", typ: "Created"}
	evt2 := fakeEvent{entityID: "A", typ: "Updated"}
	s.marshaler.On("Marshal", mock.Anything, mock.Anything).
		Return(&MarshaledEvent{Type: "T", Data: []byte(`{}`)}, nil)

	var got []EventEnvelope
	s.repo.On("Save", mock.Anything, mock.Anything).Return(nil).Once().
		Run(func(args mock.Arguments) { got = args.Get(1).([]EventEnvelope) })

	s.Require().NoError(s.atLeastOncePublisher().Publish(s.ctx, evt1, evt2))

	s.Require().Len(got, 2)
	s.Equal([]uint64{0, 0}, []uint64{got[0].Version, got[1].Version})
	s.Equal([]int{0, 1}, []int{got[0].Index, got[1].Index})
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test . -run 'TestEntitySaverSuite|TestPublisherSuite' -v`
Expected: FAIL — `undefined: ErrForeignEvent`, and `got[0].Version` undefined on `EventEnvelope`.

- [ ] **Step 3: Add the envelope fields and `staged`**

In `event.go`, replace the `EventEnvelope` struct:

```go
// EventEnvelope
type EventEnvelope struct {
	ID        string
	EntityID  string
	Version   uint64
	Index     int
	Event     *MarshaledEvent
	Metadata  Metadata
	Timestamp time.Time
}
```

Below `envelopeBuilder`, add the stamped pair and rewrite `build`:

```go
// staged pairs an event with the per-entity ordering key assigned at save time.
// Version 0 marks the unordered Publisher.Publish lane.
type staged struct {
	event   Event
	version uint64
	index   int
}

func (b envelopeBuilder) build(ctx context.Context, events []staged) ([]EventEnvelope, error) {
	metadata, err := b.metadata.Get(ctx)
	if err != nil {
		return nil, err
	}

	envelopes := make([]EventEnvelope, 0, len(events))
	for _, e := range events {
		marshaled, err := b.marshaler.Marshal(ctx, e.event)
		if err != nil {
			return nil, err
		}
		envelopes = append(envelopes, EventEnvelope{
			ID:        b.ider.ID(),
			EntityID:  e.event.EntityID(),
			Version:   e.version,
			Index:     e.index,
			Event:     marshaled,
			Metadata:  metadata,
			Timestamp: time.Now().UTC(),
		})
	}
	return envelopes, nil
}
```

`Timestamp` stays for observability only — `polling_relay.go` logs elapsed time from it. Nothing sorts on it now.

- [ ] **Step 4: Update `Publisher`**

In `publisher.go`:

```go
func (p *Publisher) stage(ctx context.Context, events []staged) (delivery, error) {
	if len(events) == 0 {
		return nil, nil
	}

	envelopes, err := p.builder.build(ctx, events)
	if err != nil {
		return nil, err
	}
	return p.guarantee.stage(ctx, envelopes)
}

// Publish is the entity-less path: no entity means no version, so these events
// are the unordered lane (version 0) and carry no ordering guarantee. No
// transaction is in scope, so a deferred delivery runs immediately.
func (p *Publisher) Publish(ctx context.Context, events ...Event) error {
	st := make([]staged, 0, len(events))
	for i, e := range events {
		st = append(st, staged{event: e, index: i})
	}

	d, err := p.stage(ctx, st)
	if err != nil {
		return err
	}
	if d == nil {
		return nil
	}
	return d(ctx)
}
```

- [ ] **Step 5: Stamp in `EntitySaver` and enforce own-entity events**

In `saver.go`, extend the error block:

```go
var (
	ErrUnregisteredEntity = errors.New("ember: no binding registered for entity type")
	ErrForeignEvent       = errors.New("ember: entity emitted an event for another entity")
)
```

Replace the event-collection preamble in `Save` (currently `saver.go:40-43`):

```go
	var events []staged
	for _, e := range es {
		v := e.Version().Inc().Value()
		for i, ev := range e.events().All() {
			if ev.EntityID() != e.ID() {
				return fmt.Errorf("%w: %s emitted an event for %s", ErrForeignEvent, e.ID(), ev.EntityID())
			}
			events = append(events, staged{event: ev, version: v, index: i})
		}
	}
```

And change the staging call inside `fn` from `s.publisher.stage(ctx, events...)` to:

```go
		deliver, err = s.publisher.stage(ctx, events)
```

The `len(es) == 1 && len(events) == 0` fast path at `saver.go:76` is unchanged — `len` works the same on `[]staged`.

- [ ] **Step 6: Fix the two `stage` call sites in the existing tests**

`publisher_test.go` calls `stage` with a bare event in two places. Update both:

```go
func (s *PublisherSuite) TestAtLeastOnceStageDefersNothing() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(&MarshaledEvent{Type: "Created"}, nil)
	s.repo.On("Save", mock.Anything, mock.Anything).Return(nil)

	d, err := s.atLeastOncePublisher().stage(s.ctx, []staged{{event: evt, version: 1}})

	s.Require().NoError(err)
	s.Nil(d, "the Relay delivers; nothing waits on commit")
}

func (s *PublisherSuite) TestBestEffortStageDefersDelivery() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(&MarshaledEvent{Type: "Created"}, nil)

	d, err := s.bestEffortPublisher().stage(s.ctx, []staged{{event: evt, version: 1}})

	s.Require().NoError(err)
	s.Require().NotNil(d, "delivery must wait for commit")
	s.sink.AssertNotCalled(s.T(), "Publish", mock.Anything, mock.Anything)

	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()
	s.Require().NoError(d(s.ctx))
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test ./... `
Expected: PASS (mongo suites may report SKIP).

- [ ] **Step 8: Commit**

```bash
git add event.go saver.go publisher.go saver_test.go publisher_test.go
git commit -m "feat: stamp events with their entity's version and save index

The outbox ordered by a wall-clock reading, which ties within a save and
inverts on NTP steps or replica skew. Version is per-entity, persisted and
monotonic, so it orders correctly without a clock. A save emits several events
at one version, so the index within the save completes the key.

An entity emitting an event for another entity would stamp a version that does
not describe the group it lands in, so that is now an error rather than a
silent mis-sequencing."
```

---

### Task 2: Two-phase drain contract, config, and continuation

**Files:**
- Modify: `polling_relay.go:13-53` (config + validation), `polling_relay.go:55-59` (`PollingRelayRepository`), `polling_relay.go:91-93` (fetch call), `polling_relay.go:151-163` (drain loop)
- Modify: `mocks_test.go:70-77` (`mockEventRepository.ListUnpublished`)
- Test: `polling_relay_test.go`

**Interfaces:**
- Consumes: `EventEnvelope.Version`, `EventEnvelope.Index` from Task 1.
- Produces:
  - `PollingRelayConfig{IdleInterval time.Duration; MaxEntitiesPerRound int; MaxEventsPerEntity int; LockKey string; Retention time.Duration}`
  - `PollingRelayRepository.ListUnpublished(ctx context.Context, maxEntities, maxEventsPerEntity int) ([]EventEnvelope, error)` — the contract Tasks 3 and 4 implement.

- [ ] **Step 1: Write the failing tests**

In `polling_relay_test.go`, replace `testRelayConfig` and add three tests. Also update every existing `On("ListUnpublished", mock.Anything, 10)` to `On("ListUnpublished", mock.Anything, 3, 4)`.

```go
func testRelayConfig() PollingRelayConfig {
	return PollingRelayConfig{
		IdleInterval:        time.Millisecond,
		MaxEntitiesPerRound: 3,
		MaxEventsPerEntity:  4,
		LockKey:             "outbox:test",
		Retention:           24 * time.Hour,
	}
}

// versioned builds an envelope carrying the ordering key the repository sorts on.
func versioned(id, entityID string, version uint64, index int) EventEnvelope {
	e := evt(id, entityID)
	e.Version = version
	e.Index = index
	return e
}

func (s *PollingRelaySuite) TestPublishPreservesRepositoryOrderWithinAGroup() {
	// The repository owns ordering; the relay must group without re-sorting.
	batch := []EventEnvelope{
		versioned("e1", "A", 1, 0),
		versioned("e2", "A", 1, 1),
		versioned("e3", "A", 2, 0),
	}
	s.repository.On("ListUnpublished", mock.Anything, 3, 4).Return(batch, nil).Once()
	s.sink.On("Publish", mock.Anything, batch).Return(nil).Once()
	s.repository.On("MarkPublished", mock.Anything, sameIDs("e1", "e2", "e3"), mock.Anything).Return(nil).Once()

	published, err := s.r.publish(context.Background())

	s.Require().NoError(err)
	s.Equal(3, published)
	s.repository.AssertExpectations(s.T())
}

func (s *PollingRelaySuite) TestTickDrainsWhileRoundsMakeProgress() {
	lock := &mockLock{}
	s.locker.On("TryLock", mock.Anything, "outbox:test").Return(lock, nil).Once()
	lock.On("Release", mock.Anything).Return(nil).Once()

	// A short round no longer means "empty" — only a round that publishes
	// nothing ends the tick.
	s.repository.On("ListUnpublished", mock.Anything, 3, 4).
		Return([]EventEnvelope{versioned("e1", "A", 1, 0)}, nil).Once()
	s.repository.On("ListUnpublished", mock.Anything, 3, 4).
		Return([]EventEnvelope{}, nil).Once()
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()
	s.repository.On("MarkPublished", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	s.r.tick(context.Background())

	s.repository.AssertNumberOfCalls(s.T(), "ListUnpublished", 2)
	lock.AssertExpectations(s.T())
}

func (s *PollingRelaySuite) TestTickStopsWhenARoundPublishesNothing() {
	lock := &mockLock{}
	s.locker.On("TryLock", mock.Anything, "outbox:test").Return(lock, nil).Once()
	lock.On("Release", mock.Anything).Return(nil).Once()

	// Every group fails to publish: the round makes no progress, so the tick
	// must exit rather than refetch the same rows forever.
	batch := []EventEnvelope{versioned("e1", "A", 1, 0)}
	s.repository.On("ListUnpublished", mock.Anything, 3, 4).Return(batch, nil).Once()
	s.sink.On("Publish", mock.Anything, batch).Return(errors.New("broker down")).Once()

	s.r.tick(context.Background())

	s.repository.AssertNumberOfCalls(s.T(), "ListUnpublished", 1)
	s.repository.AssertNotCalled(s.T(), "MarkPublished", mock.Anything, mock.Anything, mock.Anything)
}
```

Replace `TestDefaultRelayConfig`'s `BatchSize` assertion:

```go
func (s *PollingRelaySuite) TestDefaultRelayConfig() {
	cfg := DefaultPollingRelayConfig("k")

	s.Equal(200*time.Millisecond, cfg.IdleInterval)
	s.Equal(100, cfg.MaxEntitiesPerRound)
	s.Equal(20, cfg.MaxEventsPerEntity)
	s.Equal("k", cfg.LockKey)
	s.Equal(7*24*time.Hour, cfg.Retention)
}

func (s *PollingRelaySuite) TestNewRelayRejectsNonPositiveLimits() {
	cfg := testRelayConfig()
	cfg.MaxEntitiesPerRound = 0
	_, err := NewPollingRelay(s.repository, s.sink, s.locker, NopLogger, cfg)
	s.Require().ErrorIs(err, ErrInvalidRelayConfig)

	cfg = testRelayConfig()
	cfg.MaxEventsPerEntity = 0
	_, err = NewPollingRelay(s.repository, s.sink, s.locker, NopLogger, cfg)
	s.Require().ErrorIs(err, ErrInvalidRelayConfig)
}
```

Delete `TestTickDrainsWhileFullBatch` — `TestTickDrainsWhileRoundsMakeProgress` replaces it, and its premise (a full batch means more work) is exactly what this task removes.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test . -run TestPollingRelaySuite -v`
Expected: FAIL — `unknown field MaxEntitiesPerRound in struct literal`.

- [ ] **Step 3: Update the config, validation, and defaults**

In `polling_relay.go`:

```go
// PollingRelayConfig
type PollingRelayConfig struct {
	IdleInterval        time.Duration // idle poll cadence (jittered per replica)
	MaxEntitiesPerRound int           // distinct entities sampled per round
	MaxEventsPerEntity  int           // events drained per entity, per round
	LockKey             string        // redis efficiency-lock key
	Retention           time.Duration // published_at + Retention → expires_at (TTL)
}

const (
	defaultIdleInterval        = 200 * time.Millisecond
	defaultMaxEntitiesPerRound = 100
	defaultMaxEventsPerEntity  = 20
	defaultRetention           = 7 * 24 * time.Hour
)

// DefaultPollingRelayConfig returns a PollingRelayConfig with sensible defaults.
// key must be unique per service and shared by that service's replicas.
func DefaultPollingRelayConfig(key string) PollingRelayConfig {
	return PollingRelayConfig{
		IdleInterval:        defaultIdleInterval,
		MaxEntitiesPerRound: defaultMaxEntitiesPerRound,
		MaxEventsPerEntity:  defaultMaxEventsPerEntity,
		LockKey:             key,
		Retention:           defaultRetention,
	}
}

func validateRelayConfig(cfg PollingRelayConfig) error {
	switch {
	case cfg.LockKey == "":
		return fmt.Errorf("%w: LockKey must not be empty", ErrInvalidRelayConfig)
	case cfg.IdleInterval <= 0:
		return fmt.Errorf("%w: IdleInterval must be positive", ErrInvalidRelayConfig)
	case cfg.MaxEntitiesPerRound <= 0:
		return fmt.Errorf("%w: MaxEntitiesPerRound must be positive", ErrInvalidRelayConfig)
	case cfg.MaxEventsPerEntity <= 0:
		return fmt.Errorf("%w: MaxEventsPerEntity must be positive", ErrInvalidRelayConfig)
	case cfg.Retention <= 0:
		return fmt.Errorf("%w: Retention must be positive", ErrInvalidRelayConfig)
	}
	return nil
}
```

Delete the now-unused `defaultBatchSize` constant.

- [ ] **Step 4: Update the repository contract and the fetch call**

```go
// PollingRelayRepository is the drain side of a table-backed outbox.
// ListUnpublished picks up to maxEntities distinct entity ids at random from
// the unpublished set and returns up to maxEventsPerEntity events for each,
// flat, ordered by (entity_id, version, index). Selection must be genuinely
// random: an unordered LIMIT returns the same entities every round and starves
// the rest.
type PollingRelayRepository interface {
	ListUnpublished(ctx context.Context, maxEntities, maxEventsPerEntity int) ([]EventEnvelope, error)
	MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error
}
```

In `publish`, change the fetch:

```go
	events, err := r.repository.ListUnpublished(ctx, r.cfg.MaxEntitiesPerRound, r.cfg.MaxEventsPerEntity)
```

The grouping loop below it is unchanged — it groups by `EntityID` while preserving arrival order, which is exactly what the new contract requires. Do not add a sort.

- [ ] **Step 5: Replace the continuation test**

In `tick`, replace the trailing condition:

```go
		published, err := r.publish(ctx)
		if err != nil {
			r.logger.Error(ctx, "Failed to drain outbox batch", err)
			return
		}
		if published == 0 {
			return
		}
```

- [ ] **Step 6: Update the mock**

In `mocks_test.go`:

```go
func (m *mockEventRepository) ListUnpublished(ctx context.Context, maxEntities, maxEventsPerEntity int) ([]EventEnvelope, error) {
	args := m.Called(ctx, maxEntities, maxEventsPerEntity)
	var envs []EventEnvelope
	if v := args.Get(0); v != nil {
		envs = v.([]EventEnvelope)
	}
	return envs, args.Error(1)
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add polling_relay.go polling_relay_test.go mocks_test.go
git commit -m "feat: drain the outbox by sampling entities, not by global order

Sorting the whole outbox by version starves long-lived entities: an entity at
version 8000 sorts behind every version-1 event, and under a backlog new
entities keep arriving at v1 and keep jumping the queue. The drain now samples
entity ids and takes each entity's events in version order, so no entity can be
held back by another's version numbering.

A short round no longer implies an empty outbox, so the tick continues on
progress instead of on batch fullness. That also stops a tick spinning when
every group fails to publish."
```

---

### Task 3: Mongo outbox — schema, index, two-phase query

**Files:**
- Modify: `mongo/event_repository.go:27-38` (`entry`), `:40-65` (`Save`), `:67-105` (`ListUnpublished`)
- Modify: `mongo/ensure.go:37-55` (`EnsureOutbox`)
- Modify: `mongo/bench_test.go:106,121,146,155` (fixtures)
- Test: `mongo/event_repository_test.go`

**Interfaces:**
- Consumes: `EventEnvelope.Version`/`Index` (Task 1), the `ListUnpublished(ctx, maxEntities, maxEventsPerEntity)` contract (Task 2).
- Produces: a `*mongo.EventRepository` satisfying both `ember.EventRepository` and `ember.PollingRelayRepository`.

- [ ] **Step 1: Write the failing tests**

In `mongo/event_repository_test.go`, extend the fixture helper and replace `TestSaveThenListUnpublishedOrdersBySeq`:

```go
func versioned(id, entityID string, version uint64, index int, ts time.Time) ember.EventEnvelope {
	e := env(id, entityID, ts)
	e.Version = version
	e.Index = index
	return e
}

func (s *EventRepositorySuite) TestListUnpublishedOrdersByVersionThenIndex() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	s.Require().NoError(s.repo.Save(ctx, []ember.EventEnvelope{
		versioned("e3", "A", 2, 0, base),
		versioned("e1", "A", 1, 0, base),
		versioned("e2", "A", 1, 1, base),
	}))

	got, err := s.repo.ListUnpublished(ctx, 10, 10)
	s.Require().NoError(err)
	s.Equal([]string{"e1", "e2", "e3"}, ids(got))
	s.Equal(uint64(1), got[0].Version)
	s.Equal(1, got[1].Index)
	s.Equal("Created", got[0].Event.Type)
	s.Equal([]byte(`{"k":"v"}`), got[0].Event.Data)
	s.Equal("corr-e1", got[0].Metadata[ember.MetadataKey("correlation_id")])
}

// TestListUnpublishedIgnoresTheClock is the regression test for the bug this
// redesign exists to fix: under seq = Timestamp.UnixNano() these events would
// come back in timestamp order, which here is deliberately backwards and tied.
func (s *EventRepositorySuite) TestListUnpublishedIgnoresTheClock() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	s.Require().NoError(s.repo.Save(ctx, []ember.EventEnvelope{
		versioned("e1", "A", 1, 0, base.Add(3*time.Second)),
		versioned("e2", "A", 2, 0, base),
		versioned("e3", "A", 3, 0, base),
	}))

	got, err := s.repo.ListUnpublished(ctx, 10, 10)
	s.Require().NoError(err)
	s.Equal([]string{"e1", "e2", "e3"}, ids(got))
}

func (s *EventRepositorySuite) TestListUnpublishedCapsEntitiesAndEventsPerEntity() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	var envs []ember.EventEnvelope
	for _, entity := range []string{"A", "B", "C"} {
		for v := uint64(1); v <= 5; v++ {
			envs = append(envs, versioned(fmt.Sprintf("%s-%d", entity, v), entity, v, 0, base))
		}
	}
	s.Require().NoError(s.repo.Save(ctx, envs))

	got, err := s.repo.ListUnpublished(ctx, 2, 3)
	s.Require().NoError(err)

	perEntity := map[string][]uint64{}
	for _, e := range got {
		perEntity[e.EntityID] = append(perEntity[e.EntityID], e.Version)
	}
	s.Len(perEntity, 2, "at most two entities per round")
	for entity, versions := range perEntity {
		s.LessOrEqual(len(versions), 3, "at most three events for %s", entity)
		s.Equal(uint64(1), versions[0], "each entity's run must start at its lowest unpublished version")
		s.True(slices.IsSorted(versions), "each entity's run must be version-ordered")
	}
}

// TestListUnpublishedSamplesAcrossEntities pins the anti-starvation property:
// an unordered LIMIT would return the same two entities every round.
func (s *EventRepositorySuite) TestListUnpublishedSamplesAcrossEntities() {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	var envs []ember.EventEnvelope
	for _, entity := range []string{"A", "B", "C", "D"} {
		envs = append(envs, versioned(entity+"-1", entity, 1, 0, base))
	}
	s.Require().NoError(s.repo.Save(ctx, envs))

	seen := map[string]bool{}
	for range 40 {
		got, err := s.repo.ListUnpublished(ctx, 2, 10)
		s.Require().NoError(err)
		for _, e := range got {
			seen[e.EntityID] = true
		}
	}

	s.Len(seen, 4, "every entity must be reachable across rounds")
}
```

Add `"fmt"` and `"slices"` to that file's imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./mongo/ -run TestEventRepositorySuite -v`
Expected: FAIL — `too many arguments in call to s.repo.ListUnpublished`. (If mongo is unavailable the suite skips; start it before this task — the two-phase query is the thing under test and a fake would not prove it.)

- [ ] **Step 3: Update the stored document and `Save`**

In `mongo/event_repository.go`:

```go
// Data is a nested document, not bytes, so the payload is readable and
// queryable in the shell. MarshaledEvent.Data must be a JSON object.
type entry struct {
	ID          string         `bson:"_id"`
	EntityID    string         `bson:"entity_id"`
	Type        string         `bson:"type"`
	Data        bson.Raw       `bson:"data"`
	Metadata    ember.Metadata `bson:"metadata"`
	Version     uint64         `bson:"version"`
	Idx         int            `bson:"idx"`
	CreatedAt   time.Time      `bson:"created_at"`
	Published   bool           `bson:"published"`
	PublishedAt *time.Time     `bson:"published_at,omitempty"`
	ExpiresAt   *time.Time     `bson:"expires_at,omitempty"`
}
```

In `Save`, replace the `Seq` line with:

```go
			Version:   e.Version,
			Idx:       e.Index,
```

- [ ] **Step 4: Implement the two-phase `ListUnpublished`**

```go
func (r *EventRepository) ListUnpublished(ctx context.Context, maxEntities, maxEventsPerEntity int) ([]ember.EventEnvelope, error) {
	keys, err := r.sampleEntities(ctx, maxEntities)
	if err != nil || len(keys) == 0 {
		return nil, err
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "entity_id", Value: 1}, {Key: "version", Value: 1}, {Key: "idx", Value: 1}}).
		SetLimit(int64(maxEntities * maxEventsPerEntity))
	filter := bson.D{
		{Key: "published", Value: false},
		{Key: "entity_id", Value: bson.D{{Key: "$in", Value: keys}}},
	}
	cur, err := r.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var (
		out    []ember.EventEnvelope
		counts = make(map[string]int, len(keys))
	)
	for cur.Next(ctx) {
		var d entry
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		if counts[d.EntityID] == maxEventsPerEntity {
			continue
		}
		counts[d.EntityID]++

		data, err := bson.MarshalExtJSON(d.Data, false, false)
		if err != nil {
			return nil, err
		}

		out = append(out, ember.EventEnvelope{
			ID:       d.ID,
			EntityID: d.EntityID,
			Version:  d.Version,
			Index:    d.Idx,
			Event: &ember.MarshaledEvent{
				Type: d.Type,
				Data: data,
			},
			Metadata:  d.Metadata,
			Timestamp: d.CreatedAt,
		})
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// sampleEntities picks entity ids uniformly at random. $sample runs over the
// grouped keys rather than the collection, so this costs one pass over the
// distinct unpublished ids — backlog-sized, near zero in steady state.
func (r *EventRepository) sampleEntities(ctx context.Context, n int) ([]string, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "published", Value: false}}}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$entity_id"}}}},
		{{Key: "$sample", Value: bson.D{{Key: "size", Value: n}}}},
	}
	cur, err := r.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var keys []string
	for cur.Next(ctx) {
		var d struct {
			ID string `bson:"_id"`
		}
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		keys = append(keys, d.ID)
	}
	return keys, cur.Err()
}
```

Truncating a version-ordered run leaves a valid prefix, so the per-entity `continue` is safe wherever the global limit lands.

- [ ] **Step 5: Swap the index in `EnsureOutbox`**

In `mongo/ensure.go`, replace the first `IndexModel`:

```go
		{
			// Drain path: sample entity ids, then read one entity's events in
			// order. Partial on published:false so the index tracks the
			// backlog, not the retained history. Equality is required —
			// mongo partial filters do not allow $exists:false.
			Keys: bson.D{
				{Key: "entity_id", Value: 1},
				{Key: "version", Value: 1},
				{Key: "idx", Value: 1},
			},
			Options: options.Index().
				SetPartialFilterExpression(bson.D{{Key: "published", Value: false}}),
		},
```

Leave the `expires_at` TTL index alone.

- [ ] **Step 6: Update the bench fixtures**

In `mongo/bench_test.go`, replace each `Seq: ts.UnixNano()` with `Version: 1, Idx: 0`. There are four occurrences (lines 106, 121, 146, 155). `oldEntry` is the pre-change shape used to benchmark the extended-JSON conversion; give it the same two fields so both structs still marshal comparably.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test ./mongo/ -v`
Expected: PASS, with the four new tests green.

- [ ] **Step 8: Commit**

```bash
git add mongo/event_repository.go mongo/ensure.go mongo/event_repository_test.go mongo/bench_test.go
git commit -m "feat(mongo): order the outbox by (version, idx) and sample entities

Replaces the seq index with (entity_id, version, idx), partial on
published:false, which serves both drain phases. Sampling uses \$group +
\$sample so selection is genuinely random — an unordered limit returns the same
entities every round and starves the rest.

The per-entity cap is applied while scanning: a global limit plus truncation
still leaves each entity a valid version prefix."
```

---

### Task 4: Postgres outbox — columns and CTE query

**Files:**
- Modify: `postgres/event_repository.go:25-57` (`Save`), `:59-114` (`ListUnpublished`)
- Modify: `docs/superpowers/specs/2026-07-24-postgres-transactor-design.md:95-112` (column table + method notes)
- Test: `postgres/event_repository_test.go`

**Interfaces:**
- Consumes: `EventEnvelope.Version`/`Index` (Task 1), the `ListUnpublished(ctx, maxEntities, maxEventsPerEntity)` contract (Task 2).
- Produces: a `*postgres.EventRepository` satisfying both `ember.EventRepository` and `ember.PollingRelayRepository`.

- [ ] **Step 1: Write the failing tests**

In `postgres/event_repository_test.go`:

```go
func TestEventListUnpublishedSamplesEntitiesAndRanksByVersion(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "entity_id", "type", "data", "metadata", "version", "idx", "created_at"}).
		AddRow("e1", "A", "Created", []byte(`{"k":"v"}`), []byte(`{"corr":"c-e1"}`), int64(1), 0, time.Unix(1, 0).UTC()).
		AddRow("e2", "A", "Created", []byte(`{"k":"v"}`), []byte(`{"corr":"c-e2"}`), int64(1), 1, time.Unix(1, 0).UTC())

	mock.ExpectQuery("ORDER BY random\\(\\)").WithArgs(2, 3).WillReturnRows(rows)

	repo := NewEventRepository(NewDB(db), "events")
	got, err := repo.ListUnpublished(context.Background(), 2, 3)

	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, uint64(1), got[0].Version)
	require.Equal(t, 1, got[1].Index)
	require.Equal(t, "A", got[0].EntityID)
	require.Equal(t, []byte(`{"k":"v"}`), got[0].Event.Data)
	require.Equal(t, "c-e1", got[0].Metadata[ember.MetadataKey("corr")])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEventSaveWritesVersionAndIdx(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	e := env("e1", time.Unix(1, 0).UTC())
	e.Version = 7
	e.Index = 2

	mock.ExpectExec("INSERT INTO events").
		WithArgs("e1", "A", "Created", []byte(`{"k":"v"}`), sqlmock.AnyArg(), int64(7), 2, e.Timestamp, false).
		WillReturnResult(sqlmock.NewResult(0, 1))

	repo := NewEventRepository(NewDB(db), "events")
	require.NoError(t, repo.Save(context.Background(), []ember.EventEnvelope{e}))
	require.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./postgres/ -run 'TestEventListUnpublishedSamples|TestEventSaveWritesVersion' -v`
Expected: FAIL — `too many arguments in call to repo.ListUnpublished`.

- [ ] **Step 3: Write `version`/`idx` in `Save`**

In `postgres/event_repository.go`, change the column list and the values:

```go
	insert := psql.Insert(r.table).
		Columns("id", "entity_id", "type", "data", "metadata", "version", "idx", "created_at", "published")
```

```go
		insert = insert.Values(
			e.ID,
			e.EntityID,
			e.Event.Type,
			e.Event.Data,
			metadata,
			int64(e.Version),
			e.Index,
			e.Timestamp.UTC(),
			false,
		)
```

`version` is written as `int64` because `database/sql` has no `uint64` driver value.

- [ ] **Step 4: Implement the two-phase `ListUnpublished`**

Squirrel has no window-function builder, so this is one raw statement with the table name interpolated (it is a constructor argument, not user input, same as every other method here):

```go
func (r *EventRepository) ListUnpublished(ctx context.Context, maxEntities, maxEventsPerEntity int) ([]ember.EventEnvelope, error) {
	query := fmt.Sprintf(`
WITH picked AS (
  SELECT DISTINCT entity_id FROM %[1]s WHERE NOT published ORDER BY random() LIMIT $1
), ranked AS (
  SELECT o.id, o.entity_id, o.type, o.data, o.metadata, o.version, o.idx, o.created_at,
         row_number() OVER (PARTITION BY o.entity_id ORDER BY o.version, o.idx) rn
  FROM %[1]s o JOIN picked p USING (entity_id) WHERE NOT o.published
)
SELECT id, entity_id, type, data, metadata, version, idx, created_at
FROM ranked WHERE rn <= $2 ORDER BY entity_id, version, idx`, r.table)

	rows, err := r.db.Conn(ctx).QueryContext(ctx, query, maxEntities, maxEventsPerEntity)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var es []ember.EventEnvelope
	for rows.Next() {
		var (
			id, entityID, typ string
			data, metadata    []byte
			version           int64
			idx               int
			createdAt         time.Time
		)
		if err := rows.Scan(&id, &entityID, &typ, &data, &metadata, &version, &idx, &createdAt); err != nil {
			return nil, err
		}
		md := ember.Metadata{}
		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &md); err != nil {
				return nil, err
			}
		}

		es = append(es, ember.EventEnvelope{
			ID:       id,
			EntityID: entityID,
			Version:  uint64(version),
			Index:    idx,
			Event: &ember.MarshaledEvent{
				Type: typ,
				Data: data,
			},
			Metadata:  md,
			Timestamp: createdAt,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return es, nil
}
```

Add `"fmt"` to the imports; drop `sq "github.com/Masterminds/squirrel"` only if `MarkPublished` no longer uses it (it does — keep it).

- [ ] **Step 5: Update the postgres table documentation**

In `docs/superpowers/specs/2026-07-24-postgres-transactor-design.md`, replace the `seq` row of the column table with two rows and rewrite the `ListUnpublished` bullet:

```markdown
| `version`     | bigint        | emitting entity's version, 0 = unordered |
| `idx`         | int           | event index within that save            |
```

```markdown
- `ListUnpublished(ctx, maxEntities, maxEventsPerEntity)`: samples up to `maxEntities` distinct `entity_id`s with `ORDER BY random()`, then returns each one's first `maxEventsPerEntity` events by `(version, idx)`, flat, ordered by `(entity_id, version, idx)`.
```

Add the drain index to the same doc, next to the table:

```sql
CREATE INDEX outbox_pending ON outbox (entity_id, version, idx) WHERE NOT published;
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./postgres/...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add postgres/event_repository.go postgres/event_repository_test.go docs/superpowers/specs/2026-07-24-postgres-transactor-design.md
git commit -m "feat(postgres): order the outbox by (version, idx) and sample entities

One statement: a CTE samples entity ids with ORDER BY random(), a second ranks
each entity's unpublished events by (version, idx), and the outer select cuts
at the per-entity cap. Squirrel has no window-function builder, so this is raw
SQL with the table name interpolated from the constructor argument.

The relay is still dormant on postgres, so the column change lands before any
service runs this table under load."
```

---

### Task 5: Carry the key through the WAL message

**Files:**
- Modify: `postgres/wal/message.go`
- Test: `postgres/wal/message_test.go`

**Interfaces:**
- Consumes: `EventEnvelope.Version`/`Index` (Task 1).
- Produces: nothing later tasks depend on.

WAL ordering comes from commit order and never used `seq`, so this task changes no behavior. It exists so `decode(encode(e)) == e` stays true.

- [ ] **Step 1: Write the failing test**

In `postgres/wal/message_test.go`:

```go
func TestEncodeDecodeRoundTripsTheOrderingKey(t *testing.T) {
	e := ember.EventEnvelope{
		ID:        "e1",
		EntityID:  "A",
		Version:   7,
		Index:     2,
		Event:     &ember.MarshaledEvent{Type: "Created", Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{ember.MetadataKey("corr"): "c-1"},
		Timestamp: time.Unix(1_700_000_000, 0).UTC(),
	}

	b, err := encode(e)
	require.NoError(t, err)
	got, err := decode(b)

	require.NoError(t, err)
	require.Equal(t, uint64(7), got.Version)
	require.Equal(t, 2, got.Index)
	require.Equal(t, e.EntityID, got.EntityID)
	require.Equal(t, e.Event.Type, got.Event.Type)
}
```

Match the file's existing import style; add `"testing"`, `"time"`, `require`, and the `ember` import if they are not already present.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./postgres/wal/ -run TestEncodeDecodeRoundTripsTheOrderingKey -v`
Expected: FAIL — `Version: expected 7, got 0`.

- [ ] **Step 3: Add the fields to the wire format**

In `postgres/wal/message.go`:

```go
type message struct {
	ID        string          `json:"id"`
	EntityID  string          `json:"entity_id"`
	Version   uint64          `json:"version"`
	Idx       int             `json:"idx"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Metadata  ember.Metadata  `json:"metadata,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}
```

Add `Version: e.Version, Idx: e.Index` to `encode`'s literal and `Version: m.Version, Index: m.Idx` to `decode`'s.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./postgres/wal/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add postgres/wal/message.go postgres/wal/message_test.go
git commit -m "feat(wal): round-trip the ordering key through the WAL message

WAL ordering comes from commit order and never read seq, so nothing on this
path behaves differently. Dropping the fields would make decode(encode(e))
!= e, which is the kind of asymmetry that bites much later."
```

---

### Task 6: Migration runbook

**Files:**
- Create: `docs/RUNBOOK-event-ordering.md`

**Interfaces:**
- Consumes: everything above.
- Produces: the operator procedure referenced by the spec's Rollout section.

- [ ] **Step 1: Write the runbook**

```markdown
# Runbook: ember event ordering migration

Applies to every service running a table- or collection-backed ember outbox with
a `PollingRelay`. Services on `postgres/wal` need only the ember bump — the WAL
path is unchanged.

Legacy outbox rows carry `seq` and no `version`, and the version cannot be
recovered: the outbox never stored it and the entity has moved on since. So the
migration is a drain, not a backfill.

Run this per service. There is no fleet-wide cutover — each service owns its own
outbox and bumps ember on its own schedule.

## Procedure

1. **Stop the service.** All replicas. Writers must stop so nothing new enters
   the outbox.
2. **Let the relay finish**, or run one final drain.
3. **Verify the outbox is empty.** This is the gate, not a formality:

   ```js
   // mongo
   db.outbox.countDocuments({published: false})   // must be 0
   ```

   ```sql
   -- postgres
   SELECT count(*) FROM outbox WHERE NOT published;   -- must be 0
   ```

4. **Deploy** the new ember and the call-site changes.
5. **Start the service.** `EnsureOutbox` creates the new partial index on
   `(entity_id, version, idx)`.
6. **Drop the stale index.** Manual — `EnsureOutbox` is additive and will not
   remove it:

   ```js
   db.outbox.dropIndex("seq_1")
   ```

   On postgres, drop whatever index backed `ORDER BY seq` and create the drain
   index if the service creates its own schema:

   ```sql
   CREATE INDEX outbox_pending ON outbox (entity_id, version, idx) WHERE NOT published;
   ```

## If step 3 is not zero

Leftover rows decode as `version 0` and sort ahead of everything for their
entity — accidentally correct, since they genuinely are older. Only their order
relative to each other is lost. The window is safe to abort halfway; the
leftovers degrade to today's behavior rather than corrupting anything.

## Rollback

Cheaper than the `_id` normalisation — no snapshot required. Old ember sorts by
a `seq` field that new rows lack, and mongo sorts missing fields first, so a
reverted binary publishes new rows before old ones: degraded, not corrupt.
Draining again before reverting removes even that.

## Bundling

conversation-service already has a window planned for its unit-of-work adoption
and the `_id` normalisation. This drain fits inside it — same stop/verify/deploy
shape, and the outbox is empty at that point anyway because the service is
stopped.

## Call-site changes

`PollingRelayConfig.BatchSize` is gone, replaced by `MaxEntitiesPerRound` and
`MaxEventsPerEntity`. Services constructing config via
`ember.DefaultPollingRelayConfig(key)` need no change. Services setting
`BatchSize` explicitly must set both new fields.

Any service with a custom `PollingRelayRepository` must implement the new
`ListUnpublished(ctx, maxEntities, maxEventsPerEntity)` signature.

An entity emitting an event whose `EntityID()` is not its own `ID()` now fails
`Save` with `ember.ErrForeignEvent`. Grep for cross-entity `Emit` calls before
deploying.
```

- [ ] **Step 2: Verify the referenced identifiers exist**

Run: `grep -rn "MaxEntitiesPerRound\|MaxEventsPerEntity\|ErrForeignEvent" --include="*.go" . | grep -v _test`
Expected: hits in `polling_relay.go` and `saver.go`. If any name differs, fix the runbook to match the code.

- [ ] **Step 3: Run the full suite one last time**

Run: `go build ./... && go test ./...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add docs/RUNBOOK-event-ordering.md
git commit -m "docs: add the event ordering migration runbook

The migration is a drain rather than a backfill because legacy rows carry no
version and it cannot be recovered. Records the empty-outbox gate, what a
half-finished window degrades to, and the call-site changes each service needs."
```

---

## Verification

After Task 6, confirm against the spec:

- [ ] `grep -rn "UnixNano" --include="*.go" .` returns only unrelated hits (no outbox code).
- [ ] `grep -rn '"seq"' --include="*.go" .` returns nothing.
- [ ] `go build ./... && go test ./...` passes.
- [ ] Mongo integration suite ran green, not skipped — the two-phase query and the clock-independence regression test are the point of the change.
