# WAL-backed Relay for Postgres

Date: 2026-07-26
Status: approved, ready for implementation planning

## Problem

Ember delivers domain events at-least-once through an outbox: `AtLeastOnce` persists
envelopes inside the caller's transaction, and `PollingRelay` drains them to a `Sink`.
On Postgres that costs an outbox table, a Redis lock for leader election, a 200 ms idle
poll, a `MarkPublished` round trip, and a TTL cleanup column — plus an ordering column,
`seq = e.Timestamp.UnixNano()` (`postgres/event_repository.go:45`), that is unsafe under
ties and clock skew.

Postgres already has a durable, totally-ordered, transactional log with an exclusive
cursor: the WAL. The `pg-logrepl` prototype demonstrates using it directly —
`pg_logical_emit_message` on the write side, a logical replication slot on the read side,
and no table at all.

This design brings that into ember as a second relay implementation alongside
`PollingRelay`.

## Decisions

| Fork | Decision |
|---|---|
| Outbox table | Dropped for the WAL path. Events go straight into WAL via `pg_logical_emit_message`. |
| Interface split | `EventRepository` narrows to `Save` only; the drain side becomes `PollingRelayRepository`. |
| Poison events | Block forever with capped backoff and loud alerting. Never drop, never advance past a failure. |
| Topology | In-process in every replica; the replication slot is the leader election. |
| Placement | New subpackage `ember/postgres/wal`, so `pglogrepl`/`pgx` stay out of existing importers' builds. |
| Table-backed `postgres.EventRepository` | Kept as an alternative backend, not deleted. |

## Architecture

### Interfaces

`ember.EventRepository` currently bundles the write side and the drain side, forcing
`AtLeastOnce` to demand `ListUnpublished`/`MarkPublished` it never calls. A WAL-backed
store has no drain methods — its drain is the slot — so the bundle must split. Each
interface is declared next to its single consumer:

```go
// event.go — the outbox's durable write side. Save runs in the caller's transaction.
type EventRepository interface {
	Save(ctx context.Context, envelopes []EventEnvelope) error
}

// polling_relay.go — the drain side, read by PollingRelay.
type PollingRelayRepository interface {
	ListUnpublished(ctx context.Context, limit int) ([]EventEnvelope, error)
	MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error
}
```

No interface composes the two: nothing consumes the union. `AtLeastOnce(r EventRepository)`
is unchanged as written and needs only a doc-comment edit; `NewPollingRelay` takes
`PollingRelayRepository`.

Naming follows the existing convention — `XxxRepository` is the persistence interface,
`XxxStore` is the facade struct (`EntityRepository` vs `EntityStore`, `store.go:6`).

Blast radius is two compile-time assertions
(`postgres/event_repository_test.go:101`, `mongo/event_repository_test.go:98`), each
becoming one assertion per interface. Service wiring passes concrete
`*postgres.EventRepository` / `*mongo.EventRepository` values, which satisfy the narrower
interfaces automatically, so nothing downstream breaks.

### Package `ember/postgres/wal`

| Component | Responsibility |
|---|---|
| `wal.EventRepository` | `Save` → one `pg_logical_emit_message(true, prefix, json)` per envelope, over `*postgres.DB`.`Conn(ctx)`. `database/sql` only. |
| `wal.Relay` | `Run(ctx)` / `Close()`. Owns the `*pgconn.PgConn` replication connection, slot acquisition, the decode loop, and publishing to `ember.Sink`. |
| `wal.EnsurePublication` | `EnsurePublication(ctx, *postgres.DB, name)`. Called by service wiring at startup over the ordinary `*sql.DB` pool, mirroring how `mongo.EnsureCollection` is invoked; idempotent, swallows `42710 duplicate_object`. Not called by the relay, which only holds a replication connection. |
| `message` | The wire struct, encoded by `EventRepository` and decoded by `Relay` — same package because the two must agree byte-for-byte. |

`wal.EventRepository` and `wal.Relay` must be constructed with the **same message prefix**:
the store stamps it, the relay filters on it, and a mismatch silently delivers nothing.

`wal` imports `postgres`. Nothing imports `wal` except service wiring, so `pglogrepl` and
`pgx` never enter the build graph of existing `ember/postgres` users.

### Wiring

```go
// before
store := postgres.NewEventRepository(db, "events")
pub   := ember.NewPublisher(ider, md, marshaler, ember.AtLeastOnce(store))
relay, _ := ember.NewPollingRelay(store, sink, redisLocker, log, ember.DefaultPollingRelayConfig("svc"))

// after
cfg   := wal.DefaultRelayConfig("svc")           // slot, publication, message prefix
if err := wal.EnsurePublication(ctx, db, cfg.PublicationName); err != nil { ... }
store := wal.NewEventRepository(db, cfg.MessagePrefix)
pub   := ember.NewPublisher(ider, md, marshaler, ember.AtLeastOnce(store))
relay := wal.NewRelay(cfg, replConn, sink, log)
```

Deriving the store's prefix from the same `cfg` is what keeps the two in agreement.

The Redis `Locker` leaves the event path entirely — the slot is the lease.
`postgres.Locker` remains for any other use.

## Data flow

### Write path

Unchanged up to the guarantee: the domain `Emit`s, the store drains, `envelopeBuilder.build`
stamps ID / EntityID / Metadata / Timestamp, `AtLeastOnce.stage` calls
`wal.EventRepository.Save`. `Save` runs `SELECT pg_logical_emit_message(true, $1, $2)` per
envelope on `db.Conn(ctx)`.

Because `Conn(ctx)` returns the ambient `*sql.Tx` and the first argument is
`transactional := true`, the messages become visible in WAL only if the entity write
commits — in emit order, bracketed by that transaction's `BEGIN`/`COMMIT`.

### Wire format

```go
type message struct {
	ID        string          `json:"id"`
	EntityID  string          `json:"entity_id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Metadata  ember.Metadata  `json:"metadata,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}
```

`Metadata` is mandatory. `pulsar.Publisher.Publish` hard-fails on a missing
`MetadataKeyCorrelationID` (`pulsar/publisher.go:47-50`), and `pg-logrepl`'s message struct
has no metadata field — copying it verbatim would make every publish fail.

### Read path

1. `CreateReplicationSlot` (idempotent, swallowing `42710`). The publication is already in
   place — service wiring called `EnsurePublication` at startup over the ordinary pool.
2. `StartReplication(ctx, conn, slot, pglogrepl.LSN(0), opts)` — an explicit zero start LSN,
   so the server resumes from the slot's `confirmed_flush_lsn`.
3. Loop: `ReceiveMessage` → `CopyData` → `XLogData` → `pglogrepl.Parse`.
4. `BeginMessage` resets the buffer. A `LogicalDecodingMessage` whose prefix matches is
   decoded and appended; other prefixes are skipped. `CommitMessage` triggers
   `Sink.Publish(buffer)`, then advances `logPos` to the commit LSN and sends a standby
   status update.
5. `PrimaryKeepaliveMessage`: when **nothing is pending** — the transaction buffer is empty
   and no publish retry is in flight — adopt `ServerWALEnd` if it is ahead of `logPos`.
   `ReplyRequested` additionally triggers an immediate standby update.

Step 5's advance is not an optimisation, it is what keeps the slot from pinning WAL
forever. pgoutput emits `Begin` lazily, only when it has a change to send, so transactions
that produce nothing for this stream — another service's writes, autovacuum, DDL — generate
no messages at all, and there is no `CommitMessage` to advance on. Without adopting
`ServerWALEnd`, `restart_lsn` stays parked at the last published event and WAL accumulates
behind a perfectly healthy relay.

The pending guard is what makes it safe: adopting `ServerWALEnd` mid-transaction or during
a publish retry would advance past our own unpublished events, reintroducing the loss bug
this design exists to avoid.

Plugin arguments are deliberately narrower than `pg-logrepl`'s:

```go
PluginArgs: []string{
	`"proto_version" '1'`,
	fmt.Sprintf(`"publication_names" '%s'`, cfg.PublicationName),
	`"messages" 'true'`,
}
```

`binary 'true'` is dropped — it governs tuple encoding, and message content is `bytea`
passed through untouched. `streaming` is deliberately **not** enabled: it delivers
in-progress transactions as `StreamStart`/`StreamStop`/`StreamCommit`, which would break
the `Begin`→`Commit` buffering this design rests on. Outbox transactions are small, so
server-side reorder buffering is not a concern.

### Ordering

WAL gives a total commit order and, within a transaction, emit order. The relay publishes
one transaction at a time and never advances past a failure, so that order survives to
`Sink.Publish`. `pulsar.Publisher` then keys by `EntityID` (`pulsar/publisher.go:82`),
preserving per-entity order at the broker.

No `seq` column, no tie-breaking, no clock. Because transactions are processed strictly
one at a time, there are no out-of-order acknowledgements and therefore no need for a
contiguous-prefix watermark tracker like `kafka/offset_tracker.go`.

## Lifecycle and failure handling

**Startup and leader election.** After the idempotent publication and slot creation,
`StartReplication` returns `55006 object_in_use` when another replica holds the slot. That
is not an error but a "you are a standby" signal: close the connection, sleep on a jittered
interval, retry. Reusing `PollingRelay.interval()`'s jitter keeps replicas from stampeding.

**One wal_sender, not N.** A `replication=database` connection spawns a walsender process
as soon as it is established, before `StartReplication`. Standbys therefore close the
connection before sleeping, so only the leader holds a walsender.

**Publish failure.** Capped exponential backoff, retried indefinitely; `logPos` never
advances. Logged at Error with the envelope IDs, so the blocking event is identifiable
from logs alone. Recovery is a deploy — add the missing producer mapping and the relay
drains and catches up. Nothing is lost or reordered.

**Keepalive during retry.** Postgres drops a replication connection that stops sending
standby status updates. Under a block-forever policy, a relay that only sends updates from
the main receive loop would have its connection killed mid-retry, reconnect, replay, and
block again — a churn loop that masks the real alert. The retry loop must therefore keep
sending standby updates, repeating the *unadvanced* `logPos`. This is both correct (the
watermark genuinely has not moved) and safe single-threaded; `pgconn` is not
concurrency-safe, so a separate keepalive goroutine would race the receive loop.

**Shutdown.** `Close()` cancels a context and waits on a `sync.WaitGroup`, guarded by
`sync.Once` — the same shape as `PollingRelay.done`/`closeOnce`.

**Connection loss.** A `ReceiveMessage` error closes the connection and returns to the
acquire loop. The slot retained WAL while the relay was gone, and step 2 resumes from
`confirmed_flush_lsn`, so nothing is lost.

**Duplicates.** A crash between a successful publish and the standby update replays that
transaction. That is at-least-once as designed; `middleware/idempotent.go` already covers
consumers.

**Monitoring.** Given block-forever, slot lag
(`pg_current_wal_lsn() - confirmed_flush_lsn`) is the single health metric that matters: a
wedged relay shows up as unbounded WAL growth on the primary. `max_slot_wal_keep_size` is
the backstop that sacrifices the slot rather than the database.

## Provisioning, configuration, operations

**Postgres prerequisites** (these gate deployment):

- `wal_level = logical`, `max_replication_slots >= 1`, `max_wal_senders >= 1` per service database
- The service role needs the `REPLICATION` attribute; current service users do not have it
- RDS / Aurora: `rds.logical_replication = 1` in the parameter group plus a reboot, and `rds_replication` granted
- Cloud SQL: `cloudsql.logical_decoding = on`
- `max_slot_wal_keep_size` set as the disk backstop

**Publication.** `publication_names` is a required pgoutput parameter, but logical decoding
messages reach the plugin through `message_cb` independently of table filtering, so a
publication with no tables satisfies the requirement without incurring table-level
decoding. This is verified by integration test rather than assumed.

**Config**, mirroring `PollingRelayConfig` conventions — a `DefaultRelayConfig(service string)`
constructor and validation returning `ErrInvalidRelayConfig`:

```go
type RelayConfig struct {
	SlotName          string
	PublicationName   string
	MessagePrefix     string
	KeepAliveInterval time.Duration // standby update cadence, default 10s
	AcquireInterval   time.Duration // slot contention retry, jittered
	MaxRetryBackoff   time.Duration // cap on publish retry backoff
}
```

**Connections per service:** the existing `*sql.DB` pool carries writes
(`pg_logical_emit_message` is ordinary SQL), plus one `*pgconn.PgConn` on
`replication=database`, held only by the leader.

### Multiple services on one Postgres instance

Replication slots are independent objects, each with its own `restart_lsn` and
`confirmed_flush_lsn`, so `order_events_slot` and `wallet_events_slot` coexist without
interfering. Independent cursors per service work, subject to three constraints.

**Prefer a database per service.** Slots are per-database objects, and logical decoding
messages are not filtered by slot or publication — prefix filtering happens client-side in
the relay loop. Services sharing one database therefore each decode and discard every other
service's messages, so decode work and network from the primary scale with the number of
services. A database per service on a shared instance removes this entirely. Where a
database is genuinely shared, each service still needs a distinct slot name *and* a distinct
message prefix.

**Sizing.** `max_replication_slots` and `max_wal_senders` must each be at least the number
of services on the instance, plus any physical replication or other subscribers.

**Privilege isolation.** `REPLICATION` is a cluster-wide role attribute, not a per-database
grant. A service role holding it can open a replication connection against any database it
can connect to and stream that database's events. On a shared instance, constrain this with
per-database roles and restricted `CONNECT` privileges so a service can only reach its own
database.

## Testing

`pglogrepl` parsing, pgoutput, and slot mechanics are third-party and get no unit tests.
What ember owns is the state machine, the failure policy, and the wire codec.

**Unit — decode/buffer state machine.** Extracted with no connection in scope, taking
already-parsed messages:

```go
type decoder struct{ prefix string }
// apply reports a batch when m closes a transaction that produced events.
func (d *decoder) apply(m pglogrepl.Message) (batch []ember.EventEnvelope, commitLSN pglogrepl.LSN, ready bool)
```

Tests construct `*pglogrepl.BeginMessage` / `*pglogrepl.LogicalDecodingMessage` /
`*pglogrepl.CommitMessage` as plain structs — no parsing, no connection, no fake Postgres.
Cases: prefix mismatch skipped; empty transaction yields no batch; multi-event transaction
emits in emit order; interleaved foreign-prefix messages do not corrupt the buffer;
malformed JSON content.

**Unit — failure policy**, behind a narrow seam. `SendStandbyStatusUpdate` is a package
function over `*pgconn.PgConn`, so a small interface plus a thin adapter:

```go
type replConn interface {
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
	SendStandbyStatusUpdate(ctx context.Context, u pglogrepl.StandbyStatusUpdate) error
	Close(ctx context.Context) error
}
```

The seam is justified because the assertions are about ember's policy, not the driver's: a
failing `Sink` never advances `logPos`; standby updates keep flowing during retry; backoff
caps; `Close` during a retry exits promptly. It also covers the keepalive pending-guard — a
keepalive carrying a higher `ServerWALEnd` advances `logPos` when idle, and must **not**
advance it mid-transaction or while a publish is being retried.

**Unit — codec round-trip.** `EventRepository`'s encode against the relay's decode,
asserting `Metadata` survives.

**Integration — real Postgres**, via `connectTestPostgres(t)` which skips when the backend
is unavailable, mirroring `mongo/sort_test.go:19`:

- Save in a transaction, commit → relay delivers the envelope intact and in order
- Save in a transaction, roll back → relay delivers nothing, proving `transactional := true` atomicity
- A publication with no tables delivers messages
- Stop relay → save → restart → events written while down are delivered
- Two relays: the second gets `55006` and stands by; kill the first, the second takes over
- Unrelated write traffic in the same database advances the relay's `confirmed_flush_lsn`
  rather than pinning it — the regression test for the keepalive cursor-advance bug
- Two relays with distinct slots and prefixes on one database: each delivers only its own
  events, and each cursor advances independently

Placement follows existing conventions: `postgres/wal/relay_test.go` and
`event_repository_test.go`, `suite.Suite`, `testify/mock` doubles kept unexported.

**Risk.** The mongo precedent skips silently when the backend is absent, so this suite may
never run in CI and would give false confidence — while carrying the regression tests for
both bugs below. CI needs a Postgres service with `wal_level=logical`, or these tests are
decorative.

## Bugs in pg-logrepl not carried over

1. **Start LSN skips missed events.** `log_repl_notifier.go:78` passes `ident.XLogPos` —
   the current WAL head from `IdentifySystem` — as the start position. pglogrepl's own
   example does this correctly because it creates a `Temporary: true` slot fresh each run,
   where head *is* the start. `pg-logrepl` copied the line with `Temporary: false`, turning
   "start at head" into "skip everything committed while the relay was down." We pass `0`
   so the server resumes from `confirmed_flush_lsn`.

2. **Failed publishes are stepped over.** On publish error the loop `continue`s without
   advancing `logPos`, but a later successful message advances past it, silently losing the
   failed event. Processing one transaction at a time and never advancing past a failure
   makes this structurally impossible.

3. **Shutdown never exits.** `break` inside a `select` (`log_repl_notifier.go:92-95`) breaks
   the select, not the `for`. Separately, `n.shutdown <- struct{}{}` on an unbuffered
   channel deadlocks if the goroutine has already returned. We use context cancellation
   plus `sync.Once`, as `PollingRelay` already does.

4. **Metadata is dropped.** The message struct carries no metadata, which would break
   `pulsar.Publisher`'s correlation-ID requirement.

5. **Vestigial `Notify`.** `LogReplNotifier.Notify` is a no-op that exists only to satisfy
   an interface. `wal.Relay` exposes `Run`/`Close` and nothing else.

6. **Keepalives never advance the cursor.** The keepalive branch
   (`log_repl_notifier.go:140-142`) only resets the deadline, discarding `ServerWALEnd`.
   Any WAL the relay does not care about therefore pins `restart_lsn` indefinitely. We adopt
   `ServerWALEnd` whenever nothing is pending, as pglogrepl's own example does.

## Out of scope

- **The `seq` ordering flaw.** `postgres/event_repository.go:45` and mongo's equivalent use
  `Timestamp.UnixNano()` as the outbox sort key, which is unsafe under ties and clock skew.
  Since the table-backed repository is being kept as an alternative backend, this remains a
  real bug, but fixing it is its own redesign (per-entity `(version, intra-save index)`) and
  folding it in would double this change. The WAL path sidesteps it entirely by inheriting
  WAL commit order, so shipping this does not make it worse.
- **Dead-lettering.** Rejected in favour of block-and-alert; revisit only if an operational
  wedge actually occurs.
- **Migrating existing services.** This adds the capability; adoption per service is
  separate work gated on the Postgres prerequisites above.
