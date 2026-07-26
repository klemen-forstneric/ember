package wal

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/suite"

	"github.com/klemen-forstneric/ember"
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
	return &pglogrepl.LogicalDecodingMessage{Prefix: prefix, Transactional: true, Content: payload}
}

// commit builds a CommitMessage whose two LSNs deliberately differ. The relay
// must confirm TransactionEndLSN — the position *after* the commit record —
// because Postgres restarts the slot at the first commit record at or after
// confirmed_flush_lsn, so confirming CommitLSN redelivers the transaction.
func commit(commitLSN, endLSN pglogrepl.LSN) *pglogrepl.CommitMessage {
	return &pglogrepl.CommitMessage{CommitLSN: commitLSN, TransactionEndLSN: endLSN}
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

	batch, lsn, ready := s.apply(commit(42, 66))

	s.True(ready)
	s.Equal(pglogrepl.LSN(66), lsn, "the cursor must be the end LSN, not the commit record's own LSN")
	s.Require().Len(batch, 2)
	s.Equal("e1", batch[0].ID)
	s.Equal("e2", batch[1].ID)
}

func (s *DecoderSuite) TestForeignPrefixIsSkipped() {
	s.apply(&pglogrepl.BeginMessage{})
	s.apply(s.msg("other_events", "x1"))
	s.apply(s.msg("svc_events", "e1"))

	batch, _, ready := s.apply(commit(7, 11))

	s.True(ready)
	s.Require().Len(batch, 1)
	s.Equal("e1", batch[0].ID)
}

// A commit that produced nothing for us must still be reported, so the relay
// advances past it instead of pinning the slot.
func (s *DecoderSuite) TestEmptyTransactionIsReadyWithNoBatch() {
	s.apply(&pglogrepl.BeginMessage{})
	s.apply(s.msg("other_events", "x1"))

	batch, lsn, ready := s.apply(commit(9, 13))

	s.True(ready)
	s.Equal(pglogrepl.LSN(13), lsn)
	s.Empty(batch)
}

func (s *DecoderSuite) TestBufferDoesNotLeakAcrossTransactions() {
	s.apply(&pglogrepl.BeginMessage{})
	s.apply(s.msg("svc_events", "e1"))
	s.apply(commit(1, 2))

	s.apply(&pglogrepl.BeginMessage{})
	s.apply(s.msg("svc_events", "e2"))
	batch, _, _ := s.apply(commit(3, 4))

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

	s.apply(commit(5, 6))
	s.False(s.dec.pending())
}

// A non-transactional message arrives outside any Begin/Commit. Buffering it
// strands it: pending() stays false, so the relay may adopt a keepalive's
// ServerWALEnd and confirm a cursor past an event it never published.
//
// Asserting on buf rather than on a later batch is deliberate — the next Begin
// clears the buffer, so every commit-based assertion here passes either way.
func (s *DecoderSuite) TestNonTransactionalMessageIsNotBuffered() {
	nonTx := s.msg("svc_events", "n1")
	nonTx.Transactional = false

	s.apply(nonTx)

	s.False(s.dec.pending(), "a non-transactional message opens no transaction")
	s.Empty(s.dec.buf, "and so must never enter the transaction buffer")
}

func (s *DecoderSuite) TestMalformedContentReturnsError() {
	s.apply(&pglogrepl.BeginMessage{})

	_, _, ready, err := s.dec.apply(&pglogrepl.LogicalDecodingMessage{
		Prefix:        "svc_events",
		Transactional: true,
		Content:       []byte("not json"),
	})

	s.Require().Error(err)
	s.False(ready)
}

func (s *DecoderSuite) TestUnhandledMessageTypeIsIgnored() {
	_, _, ready := s.apply(&pglogrepl.OriginMessage{Name: "x"})
	s.False(ready)
}
