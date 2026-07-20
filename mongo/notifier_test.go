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

func (s *NotifierSuite) TestPublishBatchLogsPublishedAndRetry() {
	batch := []ember.EventEnvelope{evt("e1", "A"), evt("e2", "B")}
	s.store.On("ListUnpublished", mock.Anything, 10).Return(batch, nil).Once()

	s.transport.On("Publish", mock.Anything, mock.MatchedBy(func(e []ember.EventEnvelope) bool {
		return e[0].ID == "e1"
	})).Return(nil)
	s.transport.On("Publish", mock.Anything, mock.MatchedBy(func(e []ember.EventEnvelope) bool {
		return e[0].ID == "e2"
	})).Return(errors.New("transport down"))
	s.store.On("MarkPublished", mock.Anything, []string{"e1"}, mock.Anything).Return(nil).Once()

	logger := &mockLogger{}
	logger.On("Info", "Published event").Once()
	logger.On("Warn", "Failed to publish event, will retry").Once()
	s.n = NewNotifier(s.store, s.transport, s.locker, logger, testConfig())

	published, err := s.n.publishBatch(context.Background())

	s.Require().NoError(err)
	s.Equal(1, published)
	logger.AssertExpectations(s.T())
}

func (s *NotifierSuite) TestNotifyIsNoOp() {
	s.NotPanics(func() { s.n.Notify(context.Background(), []ember.EventEnvelope{evt("e1", "A")}) })
}

func (s *NotifierSuite) TestTickNotLeaderDoesNothing() {
	// nil lock → someone else is leader this round.
	s.locker.On("TryLock", mock.Anything, "outbox:test", time.Minute).Return(nil, nil).Once()

	s.n.tick(context.Background())

	s.store.AssertNotCalled(s.T(), "ListUnpublished", mock.Anything, mock.Anything)
	s.locker.AssertExpectations(s.T())
}

func (s *NotifierSuite) TestTickDrainsWhileFullBatch() {
	lock := &mockLock{}
	s.locker.On("TryLock", mock.Anything, "outbox:test", time.Minute).Return(lock, nil).Once()
	lock.On("Release", mock.Anything).Return(nil).Once()

	// cfg.BatchSize is 10. First batch: 10 events (all published) → drain again.
	// Second batch: empty → stop.
	full := make([]ember.EventEnvelope, 10)
	for i := range full {
		full[i] = evt("full", "A")
	}
	s.store.On("ListUnpublished", mock.Anything, 10).Return(full, nil).Once()
	s.store.On("ListUnpublished", mock.Anything, 10).Return([]ember.EventEnvelope{}, nil).Once()
	s.transport.On("Publish", mock.Anything, mock.Anything).Return(nil)
	s.store.On("MarkPublished", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	s.n.tick(context.Background())

	s.store.AssertNumberOfCalls(s.T(), "ListUnpublished", 2)
	lock.AssertExpectations(s.T())
}

func (s *NotifierSuite) TestRunStopsOnContextCancel() {
	// Always not-leader so ticks are cheap; Run must still exit on cancel.
	s.locker.On("TryLock", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.n.Run(ctx); close(done) }()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		s.FailNow("Run did not return after context cancel")
	}
}
