# WAL-backed Relay for Postgres Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver ember domain events at-least-once through the Postgres WAL — `pg_logical_emit_message` on the write side, a logical replication slot on the read side — with no outbox table, no Redis lock, and no polling.

**Architecture:** `AtLeastOnce` writes each envelope into the WAL as a logical decoding message inside the caller's transaction. A new `wal.Relay` tails a replication slot, buffers each transaction between `Begin` and `Commit`, publishes the batch to an `ember.Sink`, and only then advances its cursor. The slot's exclusivity is the leader election, so replicas need no distributed lock.

**Tech Stack:** Go 1.26.3, `github.com/jackc/pglogrepl`, `github.com/jackc/pgx/v5` (replication connection only), `database/sql` + squirrel (writes), testify (`suite`, `mock`), `go-sqlmock`.

**Spec:** `docs/superpowers/specs/2026-07-26-ember-wal-relay-design.md`

**Branch:** `feat/wal-relay` (already created, spec already committed)

## Global Constraints

- Module is `github.com/klemen-forstneric/ember`, Go 1.26.3.
- Comments are minimal: a terse one-liner only where naming cannot carry the meaning. No paragraph rationale blocks.
- Tests use `testify`. Prefer `suite.Suite` where setup is shared; use `testify/mock` for doubles, never hand-rolled fake structs. Mocks and helpers stay unexported and live in `*_test.go`.
- Do not unit-test third-party behavior (`pglogrepl` parsing, pgoutput, slot mechanics, `database/sql`). Test ember's state machine, failure policy, and codec.
- Integration tests connect to a real Postgres and `t.Skipf` when it is unavailable, mirroring `mongo/sort_test.go:19`.
- `pgconn` is not concurrency-safe. All replication I/O happens on one goroutine. Never add a keepalive goroutine.
- The relay must never advance its cursor past an event it has not successfully published.
- Commit after every task. Conventional Commits format.

## File Structure

| File | Responsibility |
|---|---|
| `event.go` (modify) | `EventRepository` narrows to `Save` only |
| `polling_relay.go` (modify) | Declares `PollingRelayRepository`, the drain side |
| `guarantee.go` (modify) | Doc comment only |
| `postgres/transactor.go` (modify) | Export the `conn` interface as `Conn` so other packages can use `DB.Conn(ctx)` |
| `postgres/wal/message.go` (create) | Wire struct plus `encode`/`decode` |
| `postgres/wal/event_repository.go` (create) | `Save` via `pg_logical_emit_message` |
| `postgres/wal/decoder.go` (create) | `Begin`→`Commit` buffering state machine, pure |
| `postgres/wal/config.go` (create) | `RelayConfig`, defaults, validation |
| `postgres/wal/errors.go` (create) | `IsDuplicateObject` / `IsObjectInUse` SQLSTATE predicates |
| `postgres/wal/ensure.go` (create) | `EnsurePublication`, idempotent |
| `postgres/wal/relay.go` (create) | `replConn` seam, adapter, acquire loop, stream loop, publish retry |

Two seams exist purely so the failure policy is testable without a server: `replConn`
narrows replication I/O, and `Relay.parse` defaults to `pglogrepl.Parse`. Neither is
exported. The real parse path and real slot mechanics are covered by Task 7's integration
tests, which is where third-party behavior belongs.

## Deviation from the spec

The spec's wiring sketch showed `wal.NewRelay(cfg, replConn, sink, log)`, taking a
pre-built connection. That cannot work: the relay must re-dial after connection loss and
after losing a slot-acquisition race, and a handed-in connection can only be used once.
`NewRelay` therefore takes a connection string and dials internally. Task 7 updates the
spec to match.

---

### Task 1: Split EventRepository from the drain side

**Files:**
- Modify: `event.go:20-27`
- Modify: `polling_relay.go:56-67`
- Modify: `guarantee.go:23-31`
- Test: `event_test.go` (existing file, append), `postgres/event_repository_test.go:101`, `mongo/event_repository_test.go:98`

**Interfaces:**
- Produces: `ember.EventRepository` with the single method `Save(ctx context.Context, envelopes []EventEnvelope) error`. `ember.PollingRelayRepository` with `ListUnpublished(ctx context.Context, limit int) ([]EventEnvelope, error)` and `MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error`.

- [ ] **Step 1: Write the failing test**

Append to `event_test.go`:

```go
// saveOnlyRepository has no drain methods, like a WAL-backed store whose drain
// is a replication slot.
type saveOnlyRepository struct{}

func (saveOnlyRepository) Save(context.Context, []EventEnvelope) error { return nil }

var _ EventRepository = saveOnlyRepository{}
```

Make sure `event_test.go` imports `"context"`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go build ./... && go vet ./...`
Expected: FAIL — `saveOnlyRepository does not implement EventRepository (missing method ListUnpublished)`

- [ ] **Step 3: Narrow EventRepository**

Replace `event.go:20-27` with:

```go
// EventRepository is the outbox's durable write side. Save runs inside the
// caller's transaction.
type EventRepository interface {
	Save(ctx context.Context, envelopes []EventEnvelope) error
}
```

- [ ] **Step 4: Declare the drain side next to its consumer**

In `polling_relay.go`, immediately above the `PollingRelay` struct:

```go
// PollingRelayRepository is the drain side of a table-backed outbox.
type PollingRelayRepository interface {
	ListUnpublished(ctx context.Context, limit int) ([]EventEnvelope, error)
	MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error
}
```

Change the struct field at `polling_relay.go:58` to `repository PollingRelayRepository`, and the constructor signature at `polling_relay.go:67` to `func NewPollingRelay(r PollingRelayRepository, s Sink, l Locker, log LoggerCtx, cfg PollingRelayConfig) (*PollingRelay, error)`.

Add `"time"` to `polling_relay.go` imports if goimports does not (it is already imported).

- [ ] **Step 5: Update the guarantee doc comment**

At `guarantee.go:27-28`, replace the comment above `AtLeastOnce` with:

```go
// AtLeastOnce persists envelopes to the outbox inside the caller's transaction.
// A relay is the sole publisher: PollingRelay for a table-backed outbox, or
// postgres/wal.Relay for a WAL-backed one.
```

The signature `func AtLeastOnce(r EventRepository) Guarantee` is unchanged.

- [ ] **Step 6: Add the drain-side assertions**

In `postgres/event_repository_test.go`, replace line 101 with:

```go
var (
	_ ember.EventRepository        = (*EventRepository)(nil)
	_ ember.PollingRelayRepository = (*EventRepository)(nil)
)
```

In `mongo/event_repository_test.go`, replace line 98 with the same two-assertion block.

- [ ] **Step 7: Run the full suite**

Run: `go build ./... && go test ./...`
Expected: PASS. Mongo tests skip if mongo is not running; that is expected.

- [ ] **Step 8: Commit**

```bash
git add event.go event_test.go polling_relay.go guarantee.go postgres/event_repository_test.go mongo/event_repository_test.go
git commit -m "refactor: split EventRepository from PollingRelayRepository

AtLeastOnce demanded ListUnpublished/MarkPublished it never calls, which made
a Save-only store unwireable. A WAL-backed outbox has no drain methods - its
drain is a replication slot - so each interface now names exactly what its one
consumer needs."
```

---

### Task 2: Export postgres.Conn, add the wal codec and EventRepository

**Files:**
- Modify: `postgres/transactor.go:13-18,31-37`
- Create: `postgres/wal/message.go`
- Create: `postgres/wal/event_repository.go`
- Test: `postgres/wal/message_test.go`, `postgres/wal/event_repository_test.go`

**Interfaces:**
- Consumes: `ember.EventRepository` (Task 1).
- Produces: `postgres.Conn` interface. `wal.NewEventRepository(db *postgres.DB, prefix string) *wal.EventRepository`. Package-private `encode(ember.EventEnvelope) ([]byte, error)` and `decode([]byte) (ember.EventEnvelope, error)`, used by Task 3's decoder.

- [ ] **Step 1: Add the pgx dependency**

```bash
go get github.com/jackc/pglogrepl@latest
go get github.com/jackc/pgx/v5@latest
```

- [ ] **Step 2: Export the conn interface**

In `postgres/transactor.go`, rename the unexported `conn` interface to `Conn` and update the two references. The result:

```go
// Conn is the subset of *sql.DB / *sql.Tx the repositories use, so a write
// can run on whichever is active on the ctx.
type Conn interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
```

and the method becomes `func (d *DB) Conn(ctx context.Context) Conn`. A package-level type `Conn` and a method named `Conn` coexist without conflict.

- [ ] **Step 3: Write the failing codec test**

Create `postgres/wal/message_test.go`:

```go
package wal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
)

// The relay must reconstruct exactly what the repository wrote, metadata
// included: pulsar.Publisher fails a publish with no correlation id.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	ts := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	in := ember.EventEnvelope{
		ID:        "e1",
		EntityID:  "A",
		Event:     &ember.MarshaledEvent{Type: "Created", Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{ember.MetadataKey("correlation_id"): "c-1"},
		Timestamp: ts,
	}

	payload, err := encode(in)
	require.NoError(t, err)

	out, err := decode(payload)
	require.NoError(t, err)

	require.Equal(t, in.ID, out.ID)
	require.Equal(t, in.EntityID, out.EntityID)
	require.Equal(t, in.Event.Type, out.Event.Type)
	require.JSONEq(t, string(in.Event.Data), string(out.Event.Data))
	require.Equal(t, "c-1", out.Metadata[ember.MetadataKey("correlation_id")])
	require.True(t, in.Timestamp.Equal(out.Timestamp))
}

func TestDecodeRejectsMalformedPayload(t *testing.T) {
	_, err := decode([]byte(`not json`))
	require.Error(t, err)
}
```

- [ ] **Step 4: Run to verify it fails**

Run: `go test ./postgres/wal/ -run TestEncodeDecode -v`
Expected: FAIL — build error, package `wal` does not exist.

- [ ] **Step 5: Write the codec**

Create `postgres/wal/message.go`:

```go
package wal

import (
	"encoding/json"
	"time"

	"github.com/klemen-forstneric/ember"
)

// message is the WAL payload. EventRepository encodes it and the decoder reads
// it back, so the two must agree byte-for-byte.
type message struct {
	ID        string          `json:"id"`
	EntityID  string          `json:"entity_id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Metadata  ember.Metadata  `json:"metadata,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}

func encode(e ember.EventEnvelope) ([]byte, error) {
	return json.Marshal(message{
		ID:        e.ID,
		EntityID:  e.EntityID,
		Type:      e.Event.Type,
		Data:      e.Event.Data,
		Metadata:  e.Metadata,
		Timestamp: e.Timestamp,
	})
}

func decode(b []byte) (ember.EventEnvelope, error) {
	var m message
	if err := json.Unmarshal(b, &m); err != nil {
		return ember.EventEnvelope{}, err
	}
	return ember.EventEnvelope{
		ID:        m.ID,
		EntityID:  m.EntityID,
		Event:     &ember.MarshaledEvent{Type: m.Type, Data: m.Data},
		Metadata:  m.Metadata,
		Timestamp: m.Timestamp,
	}, nil
}
```

- [ ] **Step 6: Run to verify it passes**

Run: `go test ./postgres/wal/ -run TestEncodeDecode -v && go test ./postgres/wal/ -run TestDecodeRejects -v`
Expected: PASS

- [ ] **Step 7: Write the failing EventRepository test**

Create `postgres/wal/event_repository_test.go`:

```go
package wal

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/postgres"
)

func env(id string) ember.EventEnvelope {
	return ember.EventEnvelope{
		ID:        id,
		EntityID:  "A",
		Event:     &ember.MarshaledEvent{Type: "Created", Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{ember.MetadataKey("correlation_id"): "c-" + id},
		Timestamp: time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC),
	}
}

func TestSaveEmitsOneMessagePerEnvelope(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	for _, id := range []string{"e1", "e2"} {
		payload, err := encode(env(id))
		require.NoError(t, err)
		mock.ExpectExec("SELECT pg_logical_emit_message").
			WithArgs("svc_events", payload).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}

	repo := NewEventRepository(postgres.NewDB(db), "svc_events")
	require.NoError(t, repo.Save(context.Background(), []ember.EventEnvelope{env("e1"), env("e2")}))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSaveNoEnvelopesIsNoop(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	repo := NewEventRepository(postgres.NewDB(db), "svc_events")
	require.NoError(t, repo.Save(context.Background(), nil))
	require.NoError(t, mock.ExpectationsWereMet())
}

var _ ember.EventRepository = (*EventRepository)(nil)
```

- [ ] **Step 8: Run to verify it fails**

Run: `go test ./postgres/wal/ -run TestSave -v`
Expected: FAIL — `undefined: NewEventRepository`

- [ ] **Step 9: Write EventRepository**

Create `postgres/wal/event_repository.go`:

```go
package wal

import (
	"context"

	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/postgres"
)

const emitQuery = `SELECT pg_logical_emit_message(true, $1, $2)`

// EventRepository writes events into the WAL as logical decoding messages.
// Save must run inside the caller's transaction: the emit is transactional, so
// a slot sees the message only if that transaction commits.
type EventRepository struct {
	db     *postgres.DB
	prefix string
}

func NewEventRepository(db *postgres.DB, prefix string) *EventRepository {
	return &EventRepository{db: db, prefix: prefix}
}

func (r *EventRepository) Save(ctx context.Context, envelopes []ember.EventEnvelope) error {
	for _, e := range envelopes {
		payload, err := encode(e)
		if err != nil {
			return err
		}
		if _, err := r.db.Conn(ctx).ExecContext(ctx, emitQuery, r.prefix, payload); err != nil {
			return err
		}
	}
	return nil
}
```

- [ ] **Step 10: Run to verify it passes**

Run: `go test ./postgres/... -v && go build ./...`
Expected: PASS

- [ ] **Step 11: Commit**

```bash
git add go.mod go.sum postgres/transactor.go postgres/wal/
git commit -m "feat(wal): add WAL EventRepository and message codec

Save emits one transactional logical decoding message per envelope over the
ambient *sql.Tx, so events land in the WAL atomically with the entity write.
The codec carries Metadata, which pg-logrepl's message struct omits and
pulsar.Publisher requires."
```

---

### Task 3: Transaction-buffering decoder

**Files:**
- Create: `postgres/wal/decoder.go`
- Test: `postgres/wal/decoder_test.go`

**Interfaces:**
- Consumes: `decode` from Task 2.
- Produces: `decoder` struct with `pending() bool` and `apply(m pglogrepl.Message) (batch []ember.EventEnvelope, commitLSN pglogrepl.LSN, ready bool, err error)`. Task 6's relay drives it.

- [ ] **Step 1: Write the failing tests**

Create `postgres/wal/decoder_test.go`:

```go
package wal

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type DecoderSuite struct {
	suite.Suite
	dec *decoder
}

func TestDecoderSuite(t *testing.T) {
	suite.Run(t, new(DecoderSuite))
}

func (s *DecoderSuite) SetupTest() {
	s.dec = &decoder{prefix: "svc_events"}
}

// msg builds a logical decoding message carrying the encoded envelope for id.
func (s *DecoderSuite) msg(prefix, id string) *pglogrepl.LogicalDecodingMessage {
	payload, err := encode(env(id))
	s.Require().NoError(err)
	return &pglogrepl.LogicalDecodingMessage{Prefix: prefix, Content: payload}
}

func (s *DecoderSuite) apply(m pglogrepl.Message) ([]ember.EventEnvelope, pglogrepl.LSN, bool) {
	batch, lsn, ready, err := s.dec.apply(m)
	s.Require().NoError(err)
	return batch, lsn, ready
}

func (s *DecoderSuite) TestCommitEmitsBufferedEventsInOrder() {
	s.apply(&pglogrepl.BeginMessage{})
	s.apply(s.msg("svc_events", "e1"))
	s.apply(s.msg("svc_events", "e2"))

	batch, lsn, ready := s.apply(&pglogrepl.CommitMessage{CommitLSN: 42})

	s.True(ready)
	s.Equal(pglogrepl.LSN(42), lsn)
	s.Require().Len(batch, 2)
	s.Equal("e1", batch[0].ID)
	s.Equal("e2", batch[1].ID)
}

func (s *DecoderSuite) TestForeignPrefixIsSkipped() {
	s.apply(&pglogrepl.BeginMessage{})
	s.apply(s.msg("other_events", "x1"))
	s.apply(s.msg("svc_events", "e1"))

	batch, _, ready := s.apply(&pglogrepl.CommitMessage{CommitLSN: 7})

	s.True(ready)
	s.Require().Len(batch, 1)
	s.Equal("e1", batch[0].ID)
}

// A commit that produced nothing for us must still be reported, so the relay
// advances past it instead of pinning the slot.
func (s *DecoderSuite) TestEmptyTransactionIsReadyWithNoBatch() {
	s.apply(&pglogrepl.BeginMessage{})
	s.apply(s.msg("other_events", "x1"))

	batch, lsn, ready := s.apply(&pglogrepl.CommitMessage{CommitLSN: 9})

	s.True(ready)
	s.Equal(pglogrepl.LSN(9), lsn)
	s.Empty(batch)
}

func (s *DecoderSuite) TestBufferDoesNotLeakAcrossTransactions() {
	s.apply(&pglogrepl.BeginMessage{})
	s.apply(s.msg("svc_events", "e1"))
	s.apply(&pglogrepl.CommitMessage{CommitLSN: 1})

	s.apply(&pglogrepl.BeginMessage{})
	s.apply(s.msg("svc_events", "e2"))
	batch, _, _ := s.apply(&pglogrepl.CommitMessage{CommitLSN: 2})

	s.Require().Len(batch, 1)
	s.Equal("e2", batch[0].ID)
}

// pending gates the keepalive cursor advance: it must be true for the whole
// transaction, not merely while the buffer is non-empty, because a later
// message in the same transaction may still be ours.
func (s *DecoderSuite) TestPendingSpansTheWholeTransaction() {
	s.False(s.dec.pending())

	s.apply(&pglogrepl.BeginMessage{})
	s.True(s.dec.pending(), "pending immediately after Begin, before any message")

	s.apply(s.msg("svc_events", "e1"))
	s.True(s.dec.pending())

	s.apply(&pglogrepl.CommitMessage{CommitLSN: 3})
	s.False(s.dec.pending())
}

func (s *DecoderSuite) TestMalformedContentReturnsError() {
	s.apply(&pglogrepl.BeginMessage{})

	_, _, ready, err := s.dec.apply(&pglogrepl.LogicalDecodingMessage{
		Prefix:  "svc_events",
		Content: []byte("not json"),
	})

	s.Require().Error(err)
	s.False(ready)
}

func (s *DecoderSuite) TestUnhandledMessageTypeIsIgnored() {
	_, _, ready := s.apply(&pglogrepl.OriginMessage{Name: "x"})
	s.False(ready)
}
```

Imports are `testing`, `github.com/jackc/pglogrepl`, `github.com/stretchr/testify/suite`, and
`github.com/klemen-forstneric/ember` (for the `apply` helper's return type). The suite uses
`s.Require()`, so `testify/require` is not needed.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./postgres/wal/ -run TestDecoderSuite -v`
Expected: FAIL — `undefined: decoder`

- [ ] **Step 3: Write the decoder**

Create `postgres/wal/decoder.go`:

```go
package wal

import (
	"github.com/jackc/pglogrepl"

	"github.com/klemen-forstneric/ember"
)

// decoder turns parsed replication messages into per-transaction batches, so a
// multi-event Save is delivered as the unit it was written as.
type decoder struct {
	prefix string
	inTx   bool
	buf    []ember.EventEnvelope
}

// pending reports that a transaction is open. The relay must not adopt a
// keepalive's ServerWALEnd while it is true, or it would advance past events
// this transaction has not delivered yet.
func (d *decoder) pending() bool { return d.inTx }

// apply feeds one message. ready is true only on commit, including a commit
// that produced no events for this prefix — the relay still advances past it.
func (d *decoder) apply(m pglogrepl.Message) ([]ember.EventEnvelope, pglogrepl.LSN, bool, error) {
	switch v := m.(type) {
	case *pglogrepl.BeginMessage:
		d.inTx = true
		d.buf = nil

	case *pglogrepl.LogicalDecodingMessage:
		if v.Prefix != d.prefix {
			return nil, 0, false, nil
		}
		e, err := decode(v.Content)
		if err != nil {
			return nil, 0, false, err
		}
		d.buf = append(d.buf, e)

	case *pglogrepl.CommitMessage:
		batch := d.buf
		d.buf, d.inTx = nil, false
		return batch, v.CommitLSN, true, nil
	}
	return nil, 0, false, nil
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./postgres/wal/ -run TestDecoderSuite -v`
Expected: PASS, 7 subtests

- [ ] **Step 5: Commit**

```bash
git add postgres/wal/decoder.go postgres/wal/decoder_test.go
git commit -m "feat(wal): add transaction-buffering decoder

Buffers between Begin and Commit so a multi-event Save is published as one
batch. An empty transaction still reports ready, so the relay advances past
other services' commits instead of pinning the slot. pending() spans the whole
transaction rather than tracking buffer emptiness, since a later message in the
same transaction may still be ours."
```

---

### Task 4: Relay config and SQLSTATE predicates

**Files:**
- Create: `postgres/wal/config.go`
- Create: `postgres/wal/errors.go`
- Test: `postgres/wal/config_test.go`, `postgres/wal/errors_test.go`

**Interfaces:**
- Produces: `wal.RelayConfig` struct, `wal.DefaultRelayConfig(service string) RelayConfig`, `wal.ErrInvalidRelayConfig`, package-private `validateRelayConfig(RelayConfig) error`, `wal.IsDuplicateObject(error) bool`, `wal.IsObjectInUse(error) bool`. Task 5 uses `IsDuplicateObject`; Task 6 uses everything.

- [ ] **Step 1: Write the failing config test**

Create `postgres/wal/config_test.go`:

```go
package wal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultRelayConfigDerivesNamesFromService(t *testing.T) {
	cfg := DefaultRelayConfig("order")

	require.Equal(t, "order_events_slot", cfg.SlotName)
	require.Equal(t, "order_events_pub", cfg.PublicationName)
	require.Equal(t, "order_events", cfg.MessagePrefix)
	require.NoError(t, validateRelayConfig(cfg))
}

func TestValidateRelayConfigRejectsBadValues(t *testing.T) {
	cases := map[string]func(*RelayConfig){
		"empty slot name":        func(c *RelayConfig) { c.SlotName = "" },
		"empty publication name": func(c *RelayConfig) { c.PublicationName = "" },
		"empty message prefix":   func(c *RelayConfig) { c.MessagePrefix = "" },
		"zero keepalive":         func(c *RelayConfig) { c.KeepAliveInterval = 0 },
		"zero acquire interval":  func(c *RelayConfig) { c.AcquireInterval = 0 },
		"zero max backoff":       func(c *RelayConfig) { c.MaxRetryBackoff = 0 },
		"negative keepalive":     func(c *RelayConfig) { c.KeepAliveInterval = -time.Second },
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultRelayConfig("order")
			mutate(&cfg)
			require.ErrorIs(t, validateRelayConfig(cfg), ErrInvalidRelayConfig)
		})
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./postgres/wal/ -run TestDefaultRelayConfig -v`
Expected: FAIL — `undefined: DefaultRelayConfig`

- [ ] **Step 3: Write the config**

Create `postgres/wal/config.go`:

```go
package wal

import (
	"errors"
	"fmt"
	"time"
)

// RelayConfig
type RelayConfig struct {
	SlotName          string
	PublicationName   string
	MessagePrefix     string
	KeepAliveInterval time.Duration // standby update cadence
	AcquireInterval   time.Duration // slot contention retry, jittered
	MaxRetryBackoff   time.Duration // cap on publish retry backoff
}

const (
	defaultKeepAliveInterval = 10 * time.Second
	defaultAcquireInterval   = 5 * time.Second
	defaultMaxRetryBackoff   = 30 * time.Second
)

// DefaultRelayConfig derives slot, publication and prefix from the service
// name. The prefix must match the one given to NewEventRepository.
func DefaultRelayConfig(service string) RelayConfig {
	return RelayConfig{
		SlotName:          service + "_events_slot",
		PublicationName:   service + "_events_pub",
		MessagePrefix:     service + "_events",
		KeepAliveInterval: defaultKeepAliveInterval,
		AcquireInterval:   defaultAcquireInterval,
		MaxRetryBackoff:   defaultMaxRetryBackoff,
	}
}

// ErrInvalidRelayConfig is returned by NewRelay when cfg fails validation.
var ErrInvalidRelayConfig = errors.New("ember/wal: invalid relay config")

func validateRelayConfig(cfg RelayConfig) error {
	switch {
	case cfg.SlotName == "":
		return fmt.Errorf("%w: SlotName must not be empty", ErrInvalidRelayConfig)
	case cfg.PublicationName == "":
		return fmt.Errorf("%w: PublicationName must not be empty", ErrInvalidRelayConfig)
	case cfg.MessagePrefix == "":
		return fmt.Errorf("%w: MessagePrefix must not be empty", ErrInvalidRelayConfig)
	case cfg.KeepAliveInterval <= 0:
		return fmt.Errorf("%w: KeepAliveInterval must be positive", ErrInvalidRelayConfig)
	case cfg.AcquireInterval <= 0:
		return fmt.Errorf("%w: AcquireInterval must be positive", ErrInvalidRelayConfig)
	case cfg.MaxRetryBackoff <= 0:
		return fmt.Errorf("%w: MaxRetryBackoff must be positive", ErrInvalidRelayConfig)
	}
	return nil
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./postgres/wal/ -run 'TestDefaultRelayConfig|TestValidateRelayConfig' -v`
Expected: PASS

- [ ] **Step 5: Write the failing error-predicate test**

Create `postgres/wal/errors_test.go`:

```go
package wal

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestIsDuplicateObject(t *testing.T) {
	require.True(t, IsDuplicateObject(&pgconn.PgError{Code: "42710"}))
	require.False(t, IsDuplicateObject(&pgconn.PgError{Code: "55006"}))
	require.False(t, IsDuplicateObject(errors.New("boom")))
	require.False(t, IsDuplicateObject(nil))
}

func TestIsObjectInUse(t *testing.T) {
	require.True(t, IsObjectInUse(&pgconn.PgError{Code: "55006"}))
	require.False(t, IsObjectInUse(&pgconn.PgError{Code: "42710"}))
	require.False(t, IsObjectInUse(errors.New("boom")))
}

// pgconn returns wrapped errors from some call paths, so a bare type assertion
// (as the pg-logrepl prototype used) would miss them.
func TestPredicatesUnwrap(t *testing.T) {
	require.True(t, IsObjectInUse(fmt.Errorf("start replication: %w", &pgconn.PgError{Code: "55006"})))
	require.True(t, IsDuplicateObject(fmt.Errorf("create slot: %w", &pgconn.PgError{Code: "42710"})))
}
```

- [ ] **Step 6: Run to verify it fails**

Run: `go test ./postgres/wal/ -run TestIs -v`
Expected: FAIL — `undefined: IsDuplicateObject`

- [ ] **Step 7: Write the predicates**

Create `postgres/wal/errors.go`:

```go
package wal

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

func code(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// IsDuplicateObject reports SQLSTATE 42710: the slot or publication already exists.
func IsDuplicateObject(err error) bool { return code(err) == "42710" }

// IsObjectInUse reports SQLSTATE 55006: another replica holds the slot.
func IsObjectInUse(err error) bool { return code(err) == "55006" }
```

- [ ] **Step 8: Run to verify it passes**

Run: `go test ./postgres/wal/ -v`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add postgres/wal/config.go postgres/wal/config_test.go postgres/wal/errors.go postgres/wal/errors_test.go
git commit -m "feat(wal): add relay config and SQLSTATE predicates

DefaultRelayConfig derives slot, publication and prefix from one service name
so the store and relay cannot drift apart. Predicates use errors.As rather than
a bare type assertion, since pgconn wraps errors on some call paths."
```

---

### Task 5: Integration harness and EnsurePublication

**Files:**
- Create: `postgres/wal/ensure.go`
- Test: `postgres/wal/integration_test.go`

**Interfaces:**
- Consumes: `IsDuplicateObject` (Task 4).
- Produces: `wal.EnsurePublication(ctx context.Context, db *postgres.DB, name string) error`. Test helpers `connectTestPostgres(t *testing.T) *sql.DB` and `testConnString()`, used by Task 7.

- [ ] **Step 1: Write the failing integration test**

Create `postgres/wal/integration_test.go`:

```go
package wal

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember/postgres"
)

// testConnString is the base DSN for integration tests. Start a suitable
// server with:
//
//	docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:17 \
//	  -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10
func testConnString() string {
	if v := os.Getenv("EMBER_TEST_POSTGRES"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
}

// connectTestPostgres opens a pool and skips the test when no server with
// logical decoding is reachable.
func connectTestPostgres(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", testConnString())
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Skipf("postgres unavailable: %v", err)
	}

	var walLevel string
	if err := db.QueryRow(`SHOW wal_level`).Scan(&walLevel); err != nil {
		_ = db.Close()
		t.Skipf("postgres unavailable: %v", err)
	}
	if walLevel != "logical" {
		_ = db.Close()
		t.Skipf("postgres wal_level is %q, need \"logical\"", walLevel)
	}

	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestEnsurePublicationIsIdempotent(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)
	name := "ember_ensure_pub_test"
	ctx := context.Background()

	t.Cleanup(func() {
		_, _ = pool.Exec(`DROP PUBLICATION IF EXISTS ` + name)
	})

	require.NoError(t, EnsurePublication(ctx, db, name))
	require.NoError(t, EnsurePublication(ctx, db, name), "second call must succeed")

	var count int
	require.NoError(t,
		pool.QueryRow(`SELECT count(*) FROM pg_publication WHERE pubname = $1`, name).Scan(&count))
	require.Equal(t, 1, count)

	// A publication with no tables is all pgoutput needs to deliver messages.
	require.NoError(t,
		pool.QueryRow(`SELECT count(*) FROM pg_publication_tables WHERE pubname = $1`, name).Scan(&count))
	require.Equal(t, 0, count)
}
```

- [ ] **Step 2: Run to verify it fails**

Start Postgres first:

```bash
docker run -d --name ember-pg -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:17 \
  -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10
```

Run: `go test ./postgres/wal/ -run TestEnsurePublication -v`
Expected: FAIL — `undefined: EnsurePublication`

- [ ] **Step 3: Write EnsurePublication**

Create `postgres/wal/ensure.go`:

```go
package wal

import (
	"context"
	"fmt"

	"github.com/klemen-forstneric/ember/postgres"
)

// EnsurePublication creates the publication if it does not already exist. Run
// it at startup over the ordinary pool — the relay only holds a replication
// connection. The publication needs no tables: pgoutput requires the name, but
// logical decoding messages are not filtered by table membership.
//
// name is interpolated, not bound: CREATE PUBLICATION takes an identifier, not
// a parameter. Pass a config-derived name, never user input.
func EnsurePublication(ctx context.Context, db *postgres.DB, name string) error {
	_, err := db.Conn(ctx).ExecContext(ctx, fmt.Sprintf("CREATE PUBLICATION %s", pgQuoteIdent(name)))
	if err != nil && !IsDuplicateObject(err) {
		return err
	}
	return nil
}
```

Add `pgQuoteIdent` to the same file:

```go
// pgQuoteIdent double-quotes an identifier, escaping embedded quotes.
func pgQuoteIdent(s string) string {
	out := make([]rune, 0, len(s)+2)
	out = append(out, '"')
	for _, r := range s {
		if r == '"' {
			out = append(out, '"')
		}
		out = append(out, r)
	}
	return string(append(out, '"'))
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./postgres/wal/ -run TestEnsurePublication -v`
Expected: PASS

- [ ] **Step 5: Verify the skip path works**

Run: `EMBER_TEST_POSTGRES=postgres://nobody@127.0.0.1:1/none go test ./postgres/wal/ -run TestEnsurePublication -v`
Expected: SKIP with "postgres unavailable"

- [ ] **Step 6: Commit**

```bash
git add postgres/wal/ensure.go postgres/wal/integration_test.go
git commit -m "feat(wal): add EnsurePublication and the integration harness

connectTestPostgres skips when no logical-decoding server is reachable, the
same shape as connectTestMongo. The test asserts the publication carries no
tables, which is what lets pgoutput deliver messages without any table-level
decoding overhead."
```

---

### Task 6: The relay

**Files:**
- Create: `postgres/wal/relay.go`
- Test: `postgres/wal/relay_test.go`

**Interfaces:**
- Consumes: `decoder` (Task 3), `RelayConfig`/`validateRelayConfig`/`IsObjectInUse`/`IsDuplicateObject` (Task 4).
- Produces: `wal.NewRelay(cfg RelayConfig, connString string, s ember.Sink, l ember.LoggerCtx) (*Relay, error)`, `(*Relay).Run(ctx context.Context)`, `(*Relay).Close() error`.

- [ ] **Step 1: Write the failing relay tests**

Create `postgres/wal/relay_test.go`:

```go
package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/klemen-forstneric/ember"
)

// mockSink is a testify mock for ember.Sink.
type mockSink struct {
	mock.Mock
}

func (m *mockSink) Publish(ctx context.Context, envelopes []ember.EventEnvelope) error {
	return m.Called(ctx, envelopes).Error(0)
}

// fakeConn is a scripted replConn. It hands out queued backend messages, then
// blocks until the test's context is cancelled, and records every standby
// position it was asked to send.
type fakeConn struct {
	mu       sync.Mutex
	queue    []pgproto3.BackendMessage
	standby  []pglogrepl.LSN
	startErr error
	closed   bool
}

func (c *fakeConn) CreateReplicationSlot(context.Context, string) error { return nil }

func (c *fakeConn) StartReplication(context.Context, string, pglogrepl.LSN, []string) error {
	return c.startErr
}

func (c *fakeConn) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	c.mu.Lock()
	if len(c.queue) > 0 {
		m := c.queue[0]
		c.queue = c.queue[1:]
		c.mu.Unlock()
		return m, nil
	}
	c.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *fakeConn) SendStandbyStatusUpdate(_ context.Context, u pglogrepl.StandbyStatusUpdate) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.standby = append(c.standby, u.WALWritePosition)
	return nil
}

func (c *fakeConn) Close(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *fakeConn) positions() []pglogrepl.LSN {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]pglogrepl.LSN(nil), c.standby...)
}

func (c *fakeConn) maxPosition() pglogrepl.LSN {
	var max pglogrepl.LSN
	for _, p := range c.positions() {
		if p > max {
			max = p
		}
	}
	return max
}

// scriptedParse stands in for pglogrepl.Parse. Tests address messages by index
// so no test has to hand-assemble pgoutput wire bytes; the real Parse path is
// covered by the integration tests in Task 7, where a live server produces the
// bytes. The XLogData header itself is still parsed for real.
type scriptedParse struct {
	msgs []pglogrepl.Message
}

func (s *scriptedParse) parse(walData []byte) (pglogrepl.Message, error) {
	if len(walData) != 1 || int(walData[0]) >= len(s.msgs) {
		return nil, fmt.Errorf("scriptedParse: no message for %v", walData)
	}
	return s.msgs[walData[0]], nil
}

// copyXLog wraps the index of a scripted message in a real XLogData frame:
// 8-byte WALStart, 8-byte ServerWALEnd, 8-byte ServerTime, then the payload.
func copyXLog(index int) *pgproto3.CopyData {
	body := make([]byte, 1+24+1)
	body[0] = pglogrepl.XLogDataByteID
	body[25] = byte(index)
	return &pgproto3.CopyData{Data: body}
}

// copyKeepalive builds a PrimaryKeepaliveMessage carrying serverWALEnd.
func copyKeepalive(serverWALEnd pglogrepl.LSN, replyRequested bool) *pgproto3.CopyData {
	body := make([]byte, 1+8+8+1)
	body[0] = pglogrepl.PrimaryKeepaliveMessageByteID
	binary.BigEndian.PutUint64(body[1:], uint64(serverWALEnd))
	if replyRequested {
		body[17] = 1
	}
	return &pgproto3.CopyData{Data: body}
}

type RelaySuite struct {
	suite.Suite
	sink *mockSink
	conn *fakeConn
	cfg  RelayConfig
}

func TestRelaySuite(t *testing.T) {
	suite.Run(t, new(RelaySuite))
}

func (s *RelaySuite) SetupTest() {
	s.sink = &mockSink{}
	s.conn = &fakeConn{}
	s.cfg = DefaultRelayConfig("svc")
	s.cfg.KeepAliveInterval = 20 * time.Millisecond
	s.cfg.AcquireInterval = 10 * time.Millisecond
	s.cfg.MaxRetryBackoff = 20 * time.Millisecond
}

// event builds the logical decoding message the relay would decode for id.
func (s *RelaySuite) event(prefix, id string) *pglogrepl.LogicalDecodingMessage {
	payload, err := encode(env(id))
	s.Require().NoError(err)
	return &pglogrepl.LogicalDecodingMessage{Prefix: prefix, Content: payload}
}

// newTestRelay builds a relay whose dialer always returns the suite's fakeConn
// and whose parse step resolves scripted messages by index.
func (s *RelaySuite) newTestRelay(msgs ...pglogrepl.Message) *Relay {
	r := newRelay(s.cfg, func(context.Context) (replConn, error) { return s.conn, nil }, s.sink, ember.NopLogger)
	r.parse = (&scriptedParse{msgs: msgs}).parse
	return r
}

// txn scripts a Begin, one event, and a Commit at commitLSN, and queues the
// matching CopyData frames on the fake connection.
func (s *RelaySuite) txn(id string, commitLSN pglogrepl.LSN) *Relay {
	msgs := []pglogrepl.Message{
		&pglogrepl.BeginMessage{},
		s.event(s.cfg.MessagePrefix, id),
		&pglogrepl.CommitMessage{CommitLSN: commitLSN},
	}
	s.conn.queue = []pgproto3.BackendMessage{copyXLog(0), copyXLog(1), copyXLog(2)}
	return s.newTestRelay(msgs...)
}

// runFor runs the relay until d elapses, then closes it and waits.
func (s *RelaySuite) runFor(r *Relay, d time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	time.Sleep(d)
	cancel()
	<-done
}

func (s *RelaySuite) TestPublishesCommittedTransactionAndAdvances() {
	r := s.txn("e1", 100)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()

	s.runFor(r, 200*time.Millisecond)

	s.sink.AssertExpectations(s.T())
	published := s.sink.Calls[0].Arguments.Get(1).([]ember.EventEnvelope)
	s.Require().Len(published, 1)
	s.Equal("e1", published[0].ID)
	s.Equal(pglogrepl.LSN(100), s.conn.maxPosition())
}

// A foreign prefix in the same transaction must not reach the sink.
func (s *RelaySuite) TestForeignPrefixIsNotPublished() {
	msgs := []pglogrepl.Message{
		&pglogrepl.BeginMessage{},
		s.event("other_events", "x1"),
		&pglogrepl.CommitMessage{CommitLSN: 100},
	}
	s.conn.queue = []pgproto3.BackendMessage{copyXLog(0), copyXLog(1), copyXLog(2)}

	s.runFor(s.newTestRelay(msgs...), 200*time.Millisecond)

	s.sink.AssertNotCalled(s.T(), "Publish", mock.Anything, mock.Anything)
	s.Equal(pglogrepl.LSN(100), s.conn.maxPosition(), "an empty transaction must still advance")
}

// A failing sink must never let the cursor move past the batch.
func (s *RelaySuite) TestFailedPublishDoesNotAdvance() {
	r := s.txn("e1", 100)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(errors.New("broker down"))

	s.runFor(r, 200*time.Millisecond)

	s.Greater(len(s.sink.Calls), 1, "publish must be retried")
	s.Equal(pglogrepl.LSN(0), s.conn.maxPosition(), "cursor must not advance past a failed publish")
}

// The connection dies without keepalives, so retries must keep sending the
// unadvanced position.
func (s *RelaySuite) TestKeepalivesContinueDuringRetry() {
	r := s.txn("e1", 100)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(errors.New("broker down"))

	s.runFor(r, 200*time.Millisecond)

	s.NotEmpty(s.conn.positions(), "standby updates must keep flowing while retrying")
	for _, p := range s.conn.positions() {
		s.Equal(pglogrepl.LSN(0), p, "every standby update during retry repeats the unadvanced position")
	}
}

// Unrelated WAL produces no messages for us, so only the keepalive can move the
// cursor. Without this the slot pins WAL forever.
func (s *RelaySuite) TestKeepaliveAdvancesCursorWhenIdle() {
	s.conn.queue = []pgproto3.BackendMessage{copyKeepalive(500, true)}

	s.runFor(s.newTestRelay(), 200*time.Millisecond)

	s.Equal(pglogrepl.LSN(500), s.conn.maxPosition())
	s.sink.AssertNotCalled(s.T(), "Publish", mock.Anything, mock.Anything)
}

// Mid-transaction the keepalive's position may sit past events we have not
// delivered, so it must be ignored until commit.
func (s *RelaySuite) TestKeepaliveDoesNotAdvanceMidTransaction() {
	msgs := []pglogrepl.Message{&pglogrepl.BeginMessage{}}
	s.conn.queue = []pgproto3.BackendMessage{copyXLog(0), copyKeepalive(500, true)}

	s.runFor(s.newTestRelay(msgs...), 200*time.Millisecond)

	s.Equal(pglogrepl.LSN(0), s.conn.maxPosition())
}

// Another replica holds the slot: stand by, and do not hold a wal_sender.
func (s *RelaySuite) TestSlotInUseClosesConnectionAndRetries() {
	s.conn.startErr = &pgconn.PgError{Code: "55006"}

	s.runFor(s.newTestRelay(), 100*time.Millisecond)

	s.True(s.conn.closed, "a standby must close its connection before sleeping")
}

func (s *RelaySuite) TestCloseIsIdempotent() {
	r := s.newTestRelay()
	s.NoError(r.Close())
	s.NoError(r.Close())
}

func (s *RelaySuite) TestNewRelayValidatesConfig() {
	cfg := DefaultRelayConfig("svc")
	cfg.SlotName = ""
	_, err := NewRelay(cfg, "postgres://localhost/x", s.sink, ember.NopLogger)
	s.ErrorIs(err, ErrInvalidRelayConfig)
}
```

Add `"fmt"` and `"github.com/jackc/pgx/v5/pgconn"` to the imports.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./postgres/wal/ -run TestRelaySuite -v`
Expected: FAIL — `undefined: Relay`, `undefined: newRelay`, `undefined: replConn`

- [ ] **Step 3: Write the relay**

Create `postgres/wal/relay.go`:

```go
package wal

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/klemen-forstneric/ember"
)

const initialRetryBackoff = 100 * time.Millisecond

// errClosed unwinds the stream loop when Close is called.
var errClosed = errors.New("ember/wal: relay closed")

// replConn is the replication I/O the relay needs, narrowed so the failure
// policy can be tested without a server.
type replConn interface {
	CreateReplicationSlot(ctx context.Context, slot string) error
	StartReplication(ctx context.Context, slot string, startLSN pglogrepl.LSN, pluginArgs []string) error
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
	SendStandbyStatusUpdate(ctx context.Context, u pglogrepl.StandbyStatusUpdate) error
	Close(ctx context.Context) error
}

type dialer func(ctx context.Context) (replConn, error)

// pgReplConn adapts *pgconn.PgConn, whose replication calls are package
// functions rather than methods.
type pgReplConn struct{ conn *pgconn.PgConn }

func (c *pgReplConn) CreateReplicationSlot(ctx context.Context, slot string) error {
	_, err := pglogrepl.CreateReplicationSlot(ctx, c.conn, slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil && !IsDuplicateObject(err) {
		return err
	}
	return nil
}

func (c *pgReplConn) StartReplication(ctx context.Context, slot string, startLSN pglogrepl.LSN, args []string) error {
	return pglogrepl.StartReplication(ctx, c.conn, slot, startLSN,
		pglogrepl.StartReplicationOptions{Mode: pglogrepl.LogicalReplication, PluginArgs: args})
}

func (c *pgReplConn) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	return c.conn.ReceiveMessage(ctx)
}

func (c *pgReplConn) SendStandbyStatusUpdate(ctx context.Context, u pglogrepl.StandbyStatusUpdate) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, c.conn, u)
}

func (c *pgReplConn) Close(ctx context.Context) error { return c.conn.Close(ctx) }

// Relay tails a logical replication slot and publishes each committed
// transaction to the Sink. The slot is exclusive, so it doubles as the leader
// election: replicas that lose the race stand by and retry.
type Relay struct {
	cfg    RelayConfig
	dial   dialer
	sink   ember.Sink
	logger ember.LoggerCtx
	// parse is pglogrepl.Parse in production. Tests replace it so they need not
	// hand-assemble pgoutput wire bytes; the real Parse is covered end-to-end.
	parse     func([]byte) (pglogrepl.Message, error)
	done      chan struct{}
	closeOnce sync.Once
}

// NewRelay dials connString on demand; the string must carry
// replication=database. The relay re-dials after connection loss and after
// losing a slot-acquisition race.
func NewRelay(cfg RelayConfig, connString string, s ember.Sink, l ember.LoggerCtx) (*Relay, error) {
	if err := validateRelayConfig(cfg); err != nil {
		return nil, err
	}
	dial := func(ctx context.Context) (replConn, error) {
		c, err := pgconn.Connect(ctx, connString)
		if err != nil {
			return nil, err
		}
		return &pgReplConn{conn: c}, nil
	}
	return newRelay(cfg, dial, s, l), nil
}

func newRelay(cfg RelayConfig, d dialer, s ember.Sink, l ember.LoggerCtx) *Relay {
	if l == nil {
		l = ember.NopLogger
	}
	return &Relay{
		cfg:    cfg,
		dial:   d,
		sink:   s,
		logger: l,
		parse:  pglogrepl.Parse,
		done:   make(chan struct{}),
	}
}

func (r *Relay) Run(ctx context.Context) {
	for {
		if r.stopped(ctx) {
			return
		}
		if err := r.session(ctx); err != nil && !errors.Is(err, errClosed) && ctx.Err() == nil {
			r.logger.Warn(ctx, "WAL relay session ended", "error", err, "slot", r.cfg.SlotName)
		}
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		case <-time.After(r.acquireInterval()):
		}
	}
}

func (r *Relay) Close() error {
	r.closeOnce.Do(func() { close(r.done) })
	return nil
}

func (r *Relay) stopped(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

func (r *Relay) acquireInterval() time.Duration {
	return r.cfg.AcquireInterval + time.Duration(rand.Int64N(int64(r.cfg.AcquireInterval)))
}

// session dials, acquires the slot, and streams until something ends it. A
// standby closes its connection before returning so it holds no wal_sender.
func (r *Relay) session(ctx context.Context) error {
	conn, err := r.dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	if err := conn.CreateReplicationSlot(ctx, r.cfg.SlotName); err != nil {
		return err
	}

	args := []string{
		`"proto_version" '1'`,
		fmt.Sprintf(`"publication_names" '%s'`, r.cfg.PublicationName),
		`"messages" 'true'`,
	}
	// Start LSN 0: resume from the slot's confirmed_flush_lsn, not the WAL head.
	if err := conn.StartReplication(ctx, r.cfg.SlotName, pglogrepl.LSN(0), args); err != nil {
		if IsObjectInUse(err) {
			r.logger.Debug(ctx, "WAL slot held by another replica; standing by", "slot", r.cfg.SlotName)
			return nil
		}
		return err
	}

	r.logger.Info(ctx, "WAL relay acquired slot", "slot", r.cfg.SlotName)
	return r.stream(ctx, conn)
}

func (r *Relay) stream(ctx context.Context, conn replConn) error {
	dec := &decoder{prefix: r.cfg.MessagePrefix}
	var logPos pglogrepl.LSN
	deadline := time.Now().Add(r.cfg.KeepAliveInterval)

	for {
		if r.stopped(ctx) {
			return errClosed
		}

		if time.Now().After(deadline) {
			if err := conn.SendStandbyStatusUpdate(ctx, statusUpdate(logPos)); err != nil {
				return err
			}
			deadline = time.Now().Add(r.cfg.KeepAliveInterval)
		}

		recvCtx, cancel := context.WithDeadline(ctx, deadline)
		raw, err := conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) && ctx.Err() == nil {
				continue
			}
			return err
		}

		data, ok := raw.(*pgproto3.CopyData)
		if !ok || len(data.Data) == 0 {
			continue
		}

		switch data.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data.Data[1:])
			if err != nil {
				return err
			}
			// Only safe outside a transaction: mid-transaction this position may
			// sit past events we have not delivered.
			if !dec.pending() && pkm.ServerWALEnd > logPos {
				logPos = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				deadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(data.Data[1:])
			if err != nil {
				return err
			}
			msg, err := r.parse(xld.WALData)
			if err != nil {
				return err
			}

			batch, commitLSN, ready, err := dec.apply(msg)
			if err != nil {
				// Undecodable content will never decode; skipping matches how
				// kafka.Subscriber treats a poison payload.
				r.logger.Error(ctx, "Could not decode WAL message; skipping", err,
					"wal_start", xld.WALStart.String())
				continue
			}
			if !ready {
				continue
			}
			if len(batch) > 0 {
				if err := r.publish(ctx, conn, batch, logPos); err != nil {
					return err
				}
			}
			if commitLSN > logPos {
				logPos = commitLSN
			}
			if err := conn.SendStandbyStatusUpdate(ctx, statusUpdate(logPos)); err != nil {
				return err
			}
			deadline = time.Now().Add(r.cfg.KeepAliveInterval)
		}
	}
}

// publish retries until the batch lands. It never gives up and never advances:
// the event is a committed domain fact, so blocking is preferable to loss.
// Recovery is a deploy, surfaced by slot lag.
func (r *Relay) publish(ctx context.Context, conn replConn, batch []ember.EventEnvelope, logPos pglogrepl.LSN) error {
	backoff := initialRetryBackoff
	for attempt := 1; ; attempt++ {
		err := r.sink.Publish(ctx, batch)
		if err == nil {
			for _, e := range batch {
				r.logger.Info(ctx, "Published event", "event_id", e.ID, "type", e.Event.Type,
					"entity_id", e.EntityID, "elapsed_ms", time.Since(e.Timestamp).Milliseconds())
			}
			return nil
		}

		r.logger.Error(ctx, "Could not publish events; retrying", err,
			"attempt", attempt, "events", len(batch), "event_ids", envelopeIDs(batch))

		if err := r.waitAlive(ctx, conn, backoff, logPos); err != nil {
			return err
		}
		if backoff < r.cfg.MaxRetryBackoff {
			backoff = min(backoff*2, r.cfg.MaxRetryBackoff)
		}
	}
}

// waitAlive sleeps for d while keeping the replication connection alive at the
// unadvanced position. Postgres drops a connection that stops sending standby
// updates, so a plain sleep longer than KeepAliveInterval would kill the
// session mid-retry.
func (r *Relay) waitAlive(ctx context.Context, conn replConn, d time.Duration, logPos pglogrepl.LSN) error {
	deadline := time.Now().Add(d)
	for {
		wait := min(time.Until(deadline), r.cfg.KeepAliveInterval)
		if wait <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.done:
			return errClosed
		case <-time.After(wait):
		}
		if err := conn.SendStandbyStatusUpdate(ctx, statusUpdate(logPos)); err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			return nil
		}
	}
}

func statusUpdate(l pglogrepl.LSN) pglogrepl.StandbyStatusUpdate {
	return pglogrepl.StandbyStatusUpdate{WALWritePosition: l, WALFlushPosition: l, WALApplyPosition: l}
}

func envelopeIDs(batch []ember.EventEnvelope) []string {
	ids := make([]string, 0, len(batch))
	for _, e := range batch {
		ids = append(ids, e.ID)
	}
	return ids
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./postgres/wal/ -run TestRelaySuite -v -race`
Expected: PASS, 9 subtests

- [ ] **Step 5: Run the whole package with race detection**

Run: `go test ./... -race`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add postgres/wal/relay.go postgres/wal/relay_test.go
git commit -m "feat(wal): add the replication-slot relay

Buffers each transaction, publishes it, and only then advances the cursor, so a
failed publish blocks rather than being stepped over. Keepalives keep flowing at
the unadvanced position during retry, since postgres drops a replication
connection that goes quiet. An idle keepalive's ServerWALEnd advances the
cursor past WAL we do not care about, which is what stops the slot pinning
another service's traffic. Standbys close their connection so they hold no
wal_sender."
```

---

### Task 7: End-to-end integration tests and spec sync

**Files:**
- Modify: `postgres/wal/integration_test.go` (append)
- Modify: `docs/superpowers/specs/2026-07-26-ember-wal-relay-design.md`

**Interfaces:**
- Consumes: everything from Tasks 2–6.

- [ ] **Step 1: Write the end-to-end tests**

Append to `postgres/wal/integration_test.go`:

```go
// replConnString adds replication=database to the base DSN.
func replConnString() string {
	sep := "&"
	if !strings.Contains(testConnString(), "?") {
		sep = "?"
	}
	return testConnString() + sep + "replication=database"
}

// collectingSink records every batch it is handed.
type collectingSink struct {
	mu      sync.Mutex
	batches [][]ember.EventEnvelope
}

func (s *collectingSink) Publish(_ context.Context, envelopes []ember.EventEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, append([]ember.EventEnvelope(nil), envelopes...))
	return nil
}

func (s *collectingSink) all() []ember.EventEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ember.EventEnvelope
	for _, b := range s.batches {
		out = append(out, b...)
	}
	return out
}

// setupWAL provisions a uniquely named slot and publication and returns a
// config plus a cleanup that drops both.
func setupWAL(t *testing.T, pool *sql.DB, service string) RelayConfig {
	t.Helper()
	cfg := DefaultRelayConfig(service)
	cfg.KeepAliveInterval = 200 * time.Millisecond
	cfg.AcquireInterval = 100 * time.Millisecond
	cfg.MaxRetryBackoff = time.Second

	ctx := context.Background()
	require.NoError(t, EnsurePublication(ctx, postgres.NewDB(pool), cfg.PublicationName))

	t.Cleanup(func() {
		_, _ = pool.Exec(`SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1`, cfg.SlotName)
		_, _ = pool.Exec(`DROP PUBLICATION IF EXISTS ` + pgQuoteIdent(cfg.PublicationName))
	})
	return cfg
}

// runRelay starts a relay and returns a stop func.
func runRelay(t *testing.T, cfg RelayConfig, sink ember.Sink) func() {
	t.Helper()
	r, err := NewRelay(cfg, replConnString(), sink, ember.NopLogger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	return func() {
		cancel()
		_ = r.Close()
		<-done
	}
}

// eventually polls cond until it holds or the deadline passes.
func eventually(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

func TestCommittedEventsReachTheSink(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)
	cfg := setupWAL(t, pool, "e2e_commit")
	repo := NewEventRepository(db, cfg.MessagePrefix)

	sink := &collectingSink{}
	stop := runRelay(t, cfg, sink)
	defer stop()

	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repo.Save(ctx, []ember.EventEnvelope{env("e1"), env("e2")})
	}))

	require.True(t, eventually(t, 10*time.Second, func() bool { return len(sink.all()) == 2 }),
		"expected 2 events, got %d", len(sink.all()))

	got := sink.all()
	require.Equal(t, "e1", got[0].ID)
	require.Equal(t, "e2", got[1].ID)
	require.Equal(t, "c-e1", got[0].Metadata[ember.MetadataKey("correlation_id")])
}

// transactional := true means a rolled back write emits nothing.
func TestRolledBackEventsNeverReachTheSink(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)
	cfg := setupWAL(t, pool, "e2e_rollback")
	repo := NewEventRepository(db, cfg.MessagePrefix)

	sink := &collectingSink{}
	stop := runRelay(t, cfg, sink)
	defer stop()

	wantErr := errors.New("domain failure")
	err := db.WithinTx(context.Background(), func(ctx context.Context) error {
		if err := repo.Save(ctx, []ember.EventEnvelope{env("rolled-back")}); err != nil {
			return err
		}
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)

	// Commit a marker afterwards so we know the relay was streaming.
	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repo.Save(ctx, []ember.EventEnvelope{env("marker")})
	}))

	require.True(t, eventually(t, 10*time.Second, func() bool { return len(sink.all()) == 1 }))
	require.Equal(t, "marker", sink.all()[0].ID)
}

// The regression test for pg-logrepl's ident.XLogPos start position.
func TestEventsWrittenWhileRelayIsDownAreDelivered(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)
	cfg := setupWAL(t, pool, "e2e_resume")
	repo := NewEventRepository(db, cfg.MessagePrefix)

	first := &collectingSink{}
	stop := runRelay(t, cfg, first)
	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repo.Save(ctx, []ember.EventEnvelope{env("before")})
	}))
	require.True(t, eventually(t, 10*time.Second, func() bool { return len(first.all()) == 1 }))
	stop()

	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repo.Save(ctx, []ember.EventEnvelope{env("while-down")})
	}))

	second := &collectingSink{}
	stop2 := runRelay(t, cfg, second)
	defer stop2()

	require.True(t, eventually(t, 15*time.Second, func() bool {
		for _, e := range second.all() {
			if e.ID == "while-down" {
				return true
			}
		}
		return false
	}), "event written while the relay was down must be delivered on restart")
}

// The regression test for the keepalive cursor advance: unrelated WAL must not
// pin the slot.
func TestUnrelatedTrafficAdvancesTheCursor(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)
	cfg := setupWAL(t, pool, "e2e_cursor")
	repo := NewEventRepository(db, cfg.MessagePrefix)

	sink := &collectingSink{}
	stop := runRelay(t, cfg, sink)
	defer stop()

	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repo.Save(ctx, []ember.EventEnvelope{env("e1")})
	}))
	require.True(t, eventually(t, 10*time.Second, func() bool { return len(sink.all()) == 1 }))

	lag := func() int64 {
		var v int64
		require.NoError(t, pool.QueryRow(
			`SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
			 FROM pg_replication_slots WHERE slot_name = $1`, cfg.SlotName).Scan(&v))
		return v
	}

	// Generate WAL this relay does not care about.
	for i := 0; i < 20; i++ {
		_, err := pool.Exec(`SELECT pg_logical_emit_message(true, 'someone_else', 'x')`)
		require.NoError(t, err)
	}

	require.True(t, eventually(t, 15*time.Second, func() bool { return lag() < 10_000 }),
		"slot lag stayed at %d; the cursor is not advancing past unrelated WAL", lag())
}

// Two services on one database keep independent cursors and see only their own
// events.
func TestTwoRelaysWithDistinctSlotsStayIndependent(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)

	cfgA := setupWAL(t, pool, "e2e_svc_a")
	cfgB := setupWAL(t, pool, "e2e_svc_b")

	sinkA, sinkB := &collectingSink{}, &collectingSink{}
	stopA := runRelay(t, cfgA, sinkA)
	defer stopA()
	stopB := runRelay(t, cfgB, sinkB)
	defer stopB()

	repoA := NewEventRepository(db, cfgA.MessagePrefix)
	repoB := NewEventRepository(db, cfgB.MessagePrefix)

	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repoA.Save(ctx, []ember.EventEnvelope{env("a1")})
	}))
	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repoB.Save(ctx, []ember.EventEnvelope{env("b1")})
	}))

	require.True(t, eventually(t, 15*time.Second, func() bool {
		return len(sinkA.all()) == 1 && len(sinkB.all()) == 1
	}))
	require.Equal(t, "a1", sinkA.all()[0].ID)
	require.Equal(t, "b1", sinkB.all()[0].ID)
}

// A second relay on the same slot stands by rather than double-publishing.
func TestSecondRelayStandsByOnTheSameSlot(t *testing.T) {
	pool := connectTestPostgres(t)
	db := postgres.NewDB(pool)
	cfg := setupWAL(t, pool, "e2e_standby")
	repo := NewEventRepository(db, cfg.MessagePrefix)

	leader, standby := &collectingSink{}, &collectingSink{}
	stopLeader := runRelay(t, cfg, leader)
	defer stopLeader()
	require.True(t, eventually(t, 10*time.Second, func() bool {
		var active bool
		require.NoError(t, pool.QueryRow(
			`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, cfg.SlotName).Scan(&active))
		return active
	}), "leader never acquired the slot")

	stopStandby := runRelay(t, cfg, standby)
	defer stopStandby()

	require.NoError(t, db.WithinTx(context.Background(), func(ctx context.Context) error {
		return repo.Save(ctx, []ember.EventEnvelope{env("only-once")})
	}))

	require.True(t, eventually(t, 10*time.Second, func() bool { return len(leader.all()) == 1 }))
	time.Sleep(time.Second)
	require.Empty(t, standby.all(), "a standby must not publish")
}
```

Update the import block of `integration_test.go` to include `"errors"`, `"strings"`, `"sync"`, `"time"`, and `"github.com/klemen-forstneric/ember"`.

- [ ] **Step 2: Run the integration suite**

Ensure Postgres is running (Task 5, Step 2), then run:

Run: `go test ./postgres/wal/ -run 'TestCommitted|TestRolledBack|TestEventsWritten|TestUnrelated|TestTwoRelays|TestSecondRelay' -v -timeout 5m`
Expected: PASS, 6 tests

If `TestSecondRelayStandsByOnTheSameSlot` is flaky because the standby wins the initial race, increase the leader's head start in the `eventually` that waits for `active`.

- [ ] **Step 3: Run everything**

Run: `go build ./... && go vet ./... && go test ./... -race -timeout 10m`
Expected: PASS

- [ ] **Step 4: Sync the spec with the implemented constructor**

In `docs/superpowers/specs/2026-07-26-ember-wal-relay-design.md`, in the "Wiring" section, replace the line

```go
relay := wal.NewRelay(cfg, replConn, sink, log)
```

with

```go
relay, err := wal.NewRelay(cfg, replConnString, sink, log) // DSN carries replication=database
```

and append this paragraph immediately after the code block:

```markdown
`NewRelay` takes a connection string rather than a connection because the relay
re-dials: after connection loss, and after losing a slot-acquisition race where
the standby closes its connection before sleeping.
```

- [ ] **Step 5: Commit**

```bash
git add postgres/wal/integration_test.go docs/superpowers/specs/2026-07-26-ember-wal-relay-design.md
git commit -m "test(wal): add end-to-end integration tests

Covers commit delivery with metadata intact, rollback emitting nothing,
resume-after-downtime (the regression test for the ident.XLogPos start
position), cursor advance past unrelated WAL, two services with independent
slots on one database, and a standby that does not double-publish.

Also syncs the spec: NewRelay takes a DSN, not a connection, because the relay
must re-dial after connection loss and after losing the slot race."
```

- [ ] **Step 6: Tear down the test database**

```bash
docker rm -f ember-pg
```

---

## Notes for the implementer

**There is no CI in this repository** (`.github/` does not exist). The integration tests
skip silently when Postgres is unavailable, so they will not run unless someone starts a
server. Until CI exists, run Task 5 Step 2's docker command and the full integration suite
before merging — those tests carry the regression coverage for both pg-logrepl bugs this
design exists to avoid.

**Adoption is not part of this plan.** No service is wired to `wal.Relay` here. That is
separate work, gated on the Postgres prerequisites in the spec: `wal_level = logical`, the
`REPLICATION` role attribute, and `max_slot_wal_keep_size`.

**The `seq` ordering flaw is deliberately untouched.** `postgres/event_repository.go:45`
and mongo's equivalent still sort by `Timestamp.UnixNano()`. The spec records this as out
of scope with its rationale. Do not fix it here.
