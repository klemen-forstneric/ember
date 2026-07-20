package mongo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/klemen-forstneric/ember"
)

func evt(id, entityID string) ember.EventEnvelope {
	return ember.EventEnvelope{
		ID:        id,
		EntityID:  entityID,
		Event:     &ember.MarshaledEvent{Type: "T", Data: []byte("{}")},
		Timestamp: time.Unix(0, 1).UTC(),
	}
}

func testConfig() NotifierConfig {
	return NotifierConfig{
		IdleInterval: time.Millisecond,
		BatchSize:    10,
		LockKey:      "outbox:test",
		LockTTL:      time.Minute,
		Retention:    24 * time.Hour,
	}
}

type NotifierSuite struct {
	suite.Suite
	store     *mockStore
	transport *mockTransport
	locker    *mockLocker
	n         *Notifier
}

func TestNotifierSuite(t *testing.T) {
	suite.Run(t, new(NotifierSuite))
}

func (s *NotifierSuite) SetupTest() {
	s.store = &mockStore{}
	s.transport = &mockTransport{}
	s.locker = &mockLocker{}
	s.n = NewNotifier(s.store, s.transport, s.locker, ember.NopLogger, testConfig())
}

func (s *NotifierSuite) TestPublishBatchPublishesInOrderAndMarks() {
	batch := []ember.EventEnvelope{evt("e1", "A"), evt("e2", "A"), evt("e3", "B")}
	s.store.On("ListUnpublished", mock.Anything, 10).Return(batch, nil).Once()

	var order []string
	s.transport.On("Publish", mock.Anything, mock.Anything).Return(nil).Run(func(a mock.Arguments) {
		envs := a.Get(1).([]ember.EventEnvelope)
		order = append(order, envs[0].ID)
	})
	s.store.On("MarkPublished", mock.Anything, []string{"e1", "e2", "e3"}, mock.Anything).Return(nil).Once()

	published, err := s.n.publishBatch(context.Background())

	s.Require().NoError(err)
	s.Equal(3, published)
	s.Equal([]string{"e1", "e2", "e3"}, order, "must publish one-at-a-time in seq order")
	s.store.AssertExpectations(s.T())
}

func (s *NotifierSuite) TestPublishBatchPerEntityHeadOfLine() {
	// Seq order: A/e1, A/e2, B/e3. A/e1 fails → A/e2 must be skipped; B/e3 proceeds.
	batch := []ember.EventEnvelope{evt("e1", "A"), evt("e2", "A"), evt("e3", "B")}
	s.store.On("ListUnpublished", mock.Anything, 10).Return(batch, nil).Once()

	s.transport.On("Publish", mock.Anything, mock.MatchedBy(func(e []ember.EventEnvelope) bool {
		return e[0].ID == "e1"
	})).Return(errors.New("route fail"))
	s.transport.On("Publish", mock.Anything, mock.MatchedBy(func(e []ember.EventEnvelope) bool {
		return e[0].ID == "e3"
	})).Return(nil)
	// Only e3 is marked; e1 (failed) and e2 (blocked behind e1) stay pending.
	s.store.On("MarkPublished", mock.Anything, []string{"e3"}, mock.Anything).Return(nil).Once()

	published, err := s.n.publishBatch(context.Background())

	s.Require().NoError(err)
	s.Equal(1, published)
	s.transport.AssertNotCalled(s.T(), "Publish", mock.Anything, mock.MatchedBy(func(e []ember.EventEnvelope) bool {
		return e[0].ID == "e2"
	}))
	s.store.AssertExpectations(s.T())
}

func (s *NotifierSuite) TestNotifyIsNoOp() {
	s.NotPanics(func() { s.n.Notify(context.Background(), []ember.EventEnvelope{evt("e1", "A")}) })
}
