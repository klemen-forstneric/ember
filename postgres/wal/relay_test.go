package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
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
	mu      sync.Mutex
	queue   []pgproto3.BackendMessage
	standby []pglogrepl.LSN
	// startErr is returned by every StartReplication call, unless startErrs has
	// entries: those are consumed one per call and take precedence, so a test can
	// script "fails 42704 once, then succeeds".
	startErr  error
	startErrs []error
	starts    int
	creates   int
	createErr error
	sendErr   error
	closed    bool
}

func (c *fakeConn) CreateReplicationSlot(context.Context, string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.creates++
	return c.createErr
}

func (c *fakeConn) StartReplication(context.Context, string, pglogrepl.LSN, []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.starts++
	if len(c.startErrs) > 0 {
		err := c.startErrs[0]
		c.startErrs = c.startErrs[1:]
		return err
	}
	return c.startErr
}

func (c *fakeConn) createCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.creates
}

func (c *fakeConn) startCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.starts
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
	if c.sendErr != nil {
		// A write that failed confirms nothing, so it must not be recorded.
		return c.sendErr
	}
	c.standby = append(c.standby, u.WALWritePosition)
	return nil
}

func (c *fakeConn) failSends(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sendErr = err
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

// recordingLogger records the message of every log call by level, so tests can
// tell the standby branch apart from a session failure.
type recordingLogger struct {
	mu   sync.Mutex
	msgs map[string][]string
}

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{msgs: make(map[string][]string)}
}

func (l *recordingLogger) record(level, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.msgs[level] = append(l.msgs[level], msg)
}

func (l *recordingLogger) Debug(_ context.Context, msg string, _ ...interface{}) {
	l.record("debug", msg)
}
func (l *recordingLogger) Info(_ context.Context, msg string, _ ...interface{}) {
	l.record("info", msg)
}
func (l *recordingLogger) Warn(_ context.Context, msg string, _ ...interface{}) {
	l.record("warn", msg)
}
func (l *recordingLogger) Error(_ context.Context, msg string, _ error, _ ...interface{}) {
	l.record("error", msg)
}

// sessionFailureMsg is the Error the run loop logs when a session ends badly.
const sessionFailureMsg = "WAL relay session ended"

func (l *recordingLogger) at(level string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.msgs[level]...)
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
	sink   *mockSink
	conn   *fakeConn
	logger *recordingLogger
	cfg    RelayConfig
}

func TestRelaySuite(t *testing.T) {
	suite.Run(t, new(RelaySuite))
}

func (s *RelaySuite) SetupTest() {
	s.sink = &mockSink{}
	s.conn = &fakeConn{}
	s.logger = newRecordingLogger()
	s.cfg = DefaultRelayConfig("svc")
	s.cfg.KeepAliveInterval = 20 * time.Millisecond
	s.cfg.AcquireInterval = 10 * time.Millisecond
	s.cfg.MaxRetryBackoff = 20 * time.Millisecond
}

// event builds the logical decoding message the relay would decode for id.
func (s *RelaySuite) event(prefix, id string) *pglogrepl.LogicalDecodingMessage {
	payload, err := encode(env(id))
	s.Require().NoError(err)
	return &pglogrepl.LogicalDecodingMessage{Prefix: prefix, Transactional: true, Content: payload}
}

// newTestRelay builds a relay whose dialer always returns the suite's fakeConn
// and whose parse step resolves scripted messages by index.
func (s *RelaySuite) newTestRelay(msgs ...pglogrepl.Message) *Relay {
	r := newRelay(s.cfg, func(context.Context) (replConn, error) { return s.conn, nil }, s.sink, s.logger)
	r.parse = (&scriptedParse{msgs: msgs}).parse
	return r
}

// txn scripts a Begin, one event, and a Commit ending at endLSN, and queues the
// matching CopyData frames on the fake connection. The commit record's own LSN
// is deliberately lower: the cursor must follow TransactionEndLSN.
func (s *RelaySuite) txn(id string, endLSN pglogrepl.LSN) *Relay {
	msgs := []pglogrepl.Message{
		&pglogrepl.BeginMessage{},
		s.event(s.cfg.MessagePrefix, id),
		commit(endLSN-1, endLSN),
	}
	s.conn.queue = []pgproto3.BackendMessage{copyXLog(0), copyXLog(1), copyXLog(2)}
	return s.newTestRelay(msgs...)
}

// runFor runs the relay until d elapses, then closes it and waits. The context
// stays live so it is Close, not cancellation, that unwinds the run loop.
func (s *RelaySuite) runFor(r *Relay, d time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := s.start(r, ctx)
	time.Sleep(d)
	s.Require().NoError(r.Close())
	s.awaitStop(done)
}

func (s *RelaySuite) start(r *Relay, ctx context.Context) chan struct{} {
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	return done
}

// assertNoSessionFailure guards the paths that must be handled in-stream
// rather than by tearing the session down and redialing.
func (s *RelaySuite) assertNoSessionFailure() {
	s.NotContains(s.logger.at("error"), sessionFailureMsg)
	s.Empty(s.logger.at("warn"))
}

func (s *RelaySuite) awaitStop(done chan struct{}) {
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		s.FailNow("relay did not stop after Close")
	}
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
		commit(99, 100),
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
	// 200ms of retries capped at a 20ms backoff is ~10 attempts; an uncapped
	// 100ms initial backoff would manage 2.
	s.Greater(len(s.sink.Calls), 4, "backoff must not exceed MaxRetryBackoff")
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

// A Sink may block far longer than KeepAliveInterval inside a single call —
// ext.RetryingSink spends its whole budget there. Postgres kills a replication
// connection that goes quiet, so keepalives must flow during the call itself,
// not merely between retries.
func (s *RelaySuite) TestKeepalivesFlowDuringASlowPublish() {
	blockFor := 5 * s.cfg.KeepAliveInterval
	r := s.txn("e1", 100)

	// Sampled on the relay goroutine; read after awaitStop, which happens-after.
	var atStart, atEnd []pglogrepl.LSN
	s.sink.On("Publish", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) {
			atStart = s.conn.positions()
			time.Sleep(blockFor)
			atEnd = s.conn.positions()
		}).
		Return(nil).Once()

	s.runFor(r, blockFor+200*time.Millisecond)

	s.Greater(len(atEnd), len(atStart), "no standby update was sent while the sink was blocked")
	for _, p := range atEnd {
		s.Equal(pglogrepl.LSN(0), p, "a keepalive confirmed a position the sink had not accepted")
	}
	s.Equal(pglogrepl.LSN(100), s.conn.maxPosition(), "cursor must advance once the publish succeeds")
	s.assertNoSessionFailure()
}

// A keepalive write failing mid-publish costs a duplicate, never an event. The
// goroutine only ever reports the frozen pre-batch position, so it confirms
// nothing new; the advance that follows a successful publish is a write on the
// same dead connection, so it fails too and the session ends with
// confirmed_flush_lsn untouched — the batch is redelivered on the next dial.
func (s *RelaySuite) TestKeepaliveFailureMidPublishDoesNotConfirmTheBatch() {
	r := s.txn("e1", 100)
	s.sink.On("Publish", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) {
			s.conn.failSends(errors.New("connection reset by peer"))
			time.Sleep(3 * s.cfg.KeepAliveInterval)
		}).
		Return(nil).Once()

	s.runFor(r, 300*time.Millisecond)

	s.sink.AssertExpectations(s.T())
	s.Equal(pglogrepl.LSN(0), s.conn.maxPosition(),
		"a batch must never count as confirmed through a failed standby write")
	s.Contains(s.logger.at("warn"), "Could not send standby update while publishing")
	s.Contains(s.logger.at("error"), sessionFailureMsg,
		"the dead connection must end the session so the batch is redelivered")
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

// Another replica holds the slot: stand by, do not hold a wal_sender, and do
// not report losing the race as a session failure.
func (s *RelaySuite) TestSlotInUseClosesConnectionAndRetries() {
	s.conn.startErr = &pgconn.PgError{Code: "55006"}

	s.runFor(s.newTestRelay(), 100*time.Millisecond)

	s.True(s.conn.closed, "a standby must close its connection before sleeping")
	attempts := s.logger.at("debug")
	s.Greater(len(attempts), 1, "a standby must keep re-attempting the slot")
	for _, msg := range attempts {
		s.Equal("WAL slot held by another replica; standing by", msg)
	}
	s.assertNoSessionFailure()
	s.Zero(s.conn.createCount(), "a standby must not emit a failing CREATE_REPLICATION_SLOT")
}

// A message pglogrepl.Parse cannot decode must be skipped. Returning would
// redial and resume from the same bytes forever, pinning WAL.
func (s *RelaySuite) TestUnparseableMessageIsSkippedAndStreamContinues() {
	msgs := []pglogrepl.Message{
		&pglogrepl.BeginMessage{},
		s.event(s.cfg.MessagePrefix, "e1"),
		commit(99, 100),
	}
	// Index 9 has no scripted message, so parse fails on the first frame.
	s.conn.queue = []pgproto3.BackendMessage{copyXLog(9), copyXLog(0), copyXLog(1), copyXLog(2)}
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()

	s.runFor(s.newTestRelay(msgs...), 200*time.Millisecond)

	s.sink.AssertExpectations(s.T())
	s.Equal(pglogrepl.LSN(100), s.conn.maxPosition(), "the stream must survive a poison frame")
	s.Contains(s.logger.at("error"), "Could not parse WAL message; skipping")
	s.assertNoSessionFailure()
}

// pglogrepl.Parse indexes data[0] with no bounds check, so an empty payload
// must never reach it.
func (s *RelaySuite) TestEmptyWALDataIsSkipped() {
	header := make([]byte, 1+24)
	header[0] = pglogrepl.XLogDataByteID
	s.conn.queue = []pgproto3.BackendMessage{
		&pgproto3.CopyData{Data: header},
		copyKeepalive(500, true),
	}

	r := s.newTestRelay()
	r.parse = pglogrepl.Parse
	s.runFor(r, 200*time.Millisecond)

	s.Equal(pglogrepl.LSN(500), s.conn.maxPosition())
}

// Close must unwind a blocked publish retry rather than wait for it to land.
func (s *RelaySuite) TestCloseUnwindsPublishRetry() {
	r := s.txn("e1", 100)
	retrying := make(chan struct{})
	var once sync.Once
	s.sink.On("Publish", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { once.Do(func() { close(retrying) }) }).
		Return(errors.New("broker down"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := s.start(r, ctx)

	<-retrying
	s.Require().NoError(r.Close())
	s.awaitStop(done)

	s.Equal(pglogrepl.LSN(0), s.conn.maxPosition(), "an interrupted retry must not advance")
	s.assertNoSessionFailure()
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

// A session that dies for any other reason is retried forever, so it must be
// loud: at Warn it is indistinguishable from a missing REPLICATION grant.
func (s *RelaySuite) TestSessionFailureIsLoggedAtError() {
	s.conn.startErr = errors.New("connection reset")

	s.runFor(s.newTestRelay(), 100*time.Millisecond)

	s.Contains(s.logger.at("error"), sessionFailureMsg)
	s.Empty(s.logger.at("warn"))
}

// An invalidated slot is unrecoverable data loss, not a blip, so it gets its
// own message rather than the generic session failure.
func (s *RelaySuite) TestSlotInvalidationIsLoggedDistinctly() {
	s.conn.startErr = &pgconn.PgError{
		Code:    "55000",
		Message: `can no longer get changes from replication slot "svc_events_slot"`,
	}

	s.runFor(s.newTestRelay(), 100*time.Millisecond)

	msgs := s.logger.at("error")
	s.NotContains(msgs, sessionFailureMsg)
	s.Require().NotEmpty(msgs)
	s.Contains(msgs[0], "invalidated", "the message must name the condition")
	s.Contains(msgs[0], "permanently lost", "the message must name the consequence")
}

// The first ever run has no slot: START_REPLICATION reports 42704, the relay
// creates the slot and starts again, and the stream comes up.
func (s *RelaySuite) TestMissingSlotIsCreatedThenStreamed() {
	r := s.txn("e1", 100)
	s.conn.startErrs = []error{&pgconn.PgError{Code: "42704"}}
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()

	s.runFor(r, 200*time.Millisecond)

	s.Equal(1, s.conn.createCount(), "the slot must be created exactly once")
	s.Equal(2, s.conn.startCount(), "the relay must retry the start after creating")
	s.sink.AssertExpectations(s.T())
	s.Equal(pglogrepl.LSN(100), s.conn.maxPosition(), "a first-ever start must end up streaming")
	s.assertNoSessionFailure()
}

// The steady state: the slot already exists, so no CREATE is attempted at all.
func (s *RelaySuite) TestExistingSlotIsNotRecreated() {
	r := s.txn("e1", 100)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()

	s.runFor(r, 200*time.Millisecond)

	s.Zero(s.conn.createCount())
	s.Equal(1, s.conn.startCount())
	s.sink.AssertExpectations(s.T())
}

// A creation that genuinely fails must end the session rather than stream on a
// slot that is not there.
func (s *RelaySuite) TestSlotCreationFailureEndsTheSession() {
	s.conn.startErrs = []error{&pgconn.PgError{Code: "42704"}}
	s.conn.createErr = errors.New("permission denied")

	s.runFor(s.newTestRelay(), 100*time.Millisecond)

	s.Contains(s.logger.at("error"), sessionFailureMsg)
}
