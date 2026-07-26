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
