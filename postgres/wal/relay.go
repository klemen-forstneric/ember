// Package wal delivers ember events through Postgres logical decoding: writes
// go into the WAL inside the caller's transaction and Relay tails a replication
// slot, whose exclusivity is the leader election.
//
// Operationally the policy is block-never-drop, so an event the Sink keeps
// rejecting halts the whole service's stream and pins WAL on the primary until
// it is fixed; watch pg_current_wal_lsn() - confirmed_flush_lsn. The
// max_slot_wal_keep_size backstop protects the disk by invalidating the slot,
// which discards everything after its confirmed position — there is no outbox
// table to replay from.
//
// The integration tests here need a live Postgres with wal_level=logical (see
// testConnString) and skip silently without one.
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

const (
	initialRetryBackoff = 100 * time.Millisecond
	drainTimeout        = 5 * time.Second
)

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
	err := pglogrepl.StartReplication(ctx, c.conn, slot, startLSN,
		pglogrepl.StartReplicationOptions{Mode: pglogrepl.LogicalReplication, PluginArgs: args})
	if err != nil && code(err) != "" {
		// StartReplication returns on ErrorResponse without consuming the
		// ReadyForQuery behind it, which desyncs any later command on this
		// connection — including the CreateReplicationSlot retry.
		c.drainToReady(ctx)
	}
	return err
}

func (c *pgReplConn) drainToReady(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, drainTimeout)
	defer cancel()
	for {
		m, err := c.conn.ReceiveMessage(ctx)
		if err != nil {
			return
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			return
		}
	}
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
			r.logSessionFailure(ctx, err)
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

// Session failures are logged at Error, matching PollingRelay: retrying forever
// makes a missing REPLICATION grant, wal_level != logical or a dropped
// publication look exactly like a network blip otherwise.
func (r *Relay) logSessionFailure(ctx context.Context, err error) {
	if IsSlotInvalidated(err) {
		r.logger.Error(ctx, "WAL replication slot is invalidated and unrecoverable; "+
			"every event after its confirmed position is permanently lost and the relay cannot resume",
			err, "slot", r.cfg.SlotName)
		return
	}
	r.logger.Error(ctx, "WAL relay session ended", err, "slot", r.cfg.SlotName)
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

	args := []string{
		`"proto_version" '1'`,
		fmt.Sprintf(`"publication_names" '%s'`, r.cfg.PublicationName),
		`"messages" 'true'`,
	}
	// Start LSN 0: resume from the slot's confirmed_flush_lsn, not the WAL head.
	err = conn.StartReplication(ctx, r.cfg.SlotName, pglogrepl.LSN(0), args)
	if IsUndefinedObject(err) {
		// Creating first instead would make every standby emit a failing
		// CREATE_REPLICATION_SLOT every AcquireInterval, forever, each one an
		// ERROR in the server log.
		if err := conn.CreateReplicationSlot(ctx, r.cfg.SlotName); err != nil {
			return err
		}
		err = conn.StartReplication(ctx, r.cfg.SlotName, pglogrepl.LSN(0), args)
	}
	if err != nil {
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
			if (pgconn.Timeout(err) || errors.Is(err, context.DeadlineExceeded)) && ctx.Err() == nil {
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
			if len(xld.WALData) == 0 {
				continue
			}
			msg, err := r.parse(xld.WALData)
			if err != nil {
				// Returning would redial and resume from the same bytes forever,
				// pinning WAL. Parse rejects any message type it does not know.
				r.logger.Error(ctx, "Could not parse WAL message; skipping", err,
					"wal_start", xld.WALStart.String())
				continue
			}

			batch, endLSN, ready, err := dec.apply(msg)
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
			if endLSN > logPos {
				logPos = endLSN
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
	backoff := min(initialRetryBackoff, r.cfg.MaxRetryBackoff)
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
