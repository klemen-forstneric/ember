package ember

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

func evt(id, entityID string) EventEnvelope {
	return EventEnvelope{
		ID:        id,
		EntityID:  entityID,
		Event:     &MarshaledEvent{Type: "T", Data: []byte("{}")},
		Timestamp: time.Unix(0, 1).UTC(),
	}
}

func testRelayConfig() RelayConfig {
	return RelayConfig{
		IdleInterval: time.Millisecond,
		BatchSize:    10,
		LockKey:      "outbox:test",
		LockTTL:      time.Minute,
		Retention:    24 * time.Hour,
	}
}

type RelaySuite struct {
	suite.Suite
	repository *mockEventRepository
	sink       *mockSink
	locker     *mockLocker
	r          *Relay
}

func TestRelaySuite(t *testing.T) {
	suite.Run(t, new(RelaySuite))
}

func (s *RelaySuite) SetupTest() {
	s.repository = &mockEventRepository{}
	s.sink = &mockSink{}
	s.locker = &mockLocker{}
	r, err := NewRelay(s.repository, s.sink, s.locker, NopLogger, testRelayConfig())
	s.Require().NoError(err)
	s.r = r
}

func (s *RelaySuite) TestPublishBatchPublishesInOrderAndMarks() {
	batch := []EventEnvelope{evt("e1", "A"), evt("e2", "A"), evt("e3", "B")}
	s.repository.On("ListUnpublished", mock.Anything, 10).Return(batch, nil).Once()

	var order []string
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Run(func(a mock.Arguments) {
		envs := a.Get(1).([]EventEnvelope)
		order = append(order, envs[0].ID)
	})
	s.repository.On("MarkPublished", mock.Anything, []string{"e1", "e2", "e3"}, mock.Anything).Return(nil).Once()

	published, err := s.r.publishBatch(context.Background())

	s.Require().NoError(err)
	s.Equal(3, published)
	s.Equal([]string{"e1", "e2", "e3"}, order, "must publish one-at-a-time in seq order")
	s.repository.AssertExpectations(s.T())
}

func (s *RelaySuite) TestPublishBatchPerEntityHeadOfLine() {
	// Seq order: A/e1, A/e2, B/e3. A/e1 fails → A/e2 must be skipped; B/e3 proceeds.
	batch := []EventEnvelope{evt("e1", "A"), evt("e2", "A"), evt("e3", "B")}
	s.repository.On("ListUnpublished", mock.Anything, 10).Return(batch, nil).Once()

	s.sink.On("Publish", mock.Anything, mock.MatchedBy(func(e []EventEnvelope) bool {
		return e[0].ID == "e1"
	})).Return(errors.New("route fail"))
	s.sink.On("Publish", mock.Anything, mock.MatchedBy(func(e []EventEnvelope) bool {
		return e[0].ID == "e3"
	})).Return(nil)
	// Only e3 is marked; e1 (failed) and e2 (blocked behind e1) stay pending.
	s.repository.On("MarkPublished", mock.Anything, []string{"e3"}, mock.Anything).Return(nil).Once()

	published, err := s.r.publishBatch(context.Background())

	s.Require().NoError(err)
	s.Equal(1, published)
	s.sink.AssertNotCalled(s.T(), "Publish", mock.Anything, mock.MatchedBy(func(e []EventEnvelope) bool {
		return e[0].ID == "e2"
	}))
	s.repository.AssertExpectations(s.T())
}

func (s *RelaySuite) TestPublishBatchLogsPublishedAndRetry() {
	batch := []EventEnvelope{evt("e1", "A"), evt("e2", "B")}
	s.repository.On("ListUnpublished", mock.Anything, 10).Return(batch, nil).Once()

	s.sink.On("Publish", mock.Anything, mock.MatchedBy(func(e []EventEnvelope) bool {
		return e[0].ID == "e1"
	})).Return(nil)
	s.sink.On("Publish", mock.Anything, mock.MatchedBy(func(e []EventEnvelope) bool {
		return e[0].ID == "e2"
	})).Return(errors.New("sink down"))
	s.repository.On("MarkPublished", mock.Anything, []string{"e1"}, mock.Anything).Return(nil).Once()

	logger := &mockLogger{}
	logger.On("Info", "Published event").Once()
	logger.On("Warn", "Failed to publish event, will retry").Once()
	r, err := NewRelay(s.repository, s.sink, s.locker, logger, testRelayConfig())
	s.Require().NoError(err)
	s.r = r

	published, err := s.r.publishBatch(context.Background())

	s.Require().NoError(err)
	s.Equal(1, published)
	logger.AssertExpectations(s.T())
}

func (s *RelaySuite) TestTickNotLeaderDoesNothing() {
	// nil lock → someone else is leader this round.
	s.locker.On("TryLock", mock.Anything, "outbox:test", time.Minute).Return(nil, nil).Once()

	s.r.tick(context.Background())

	s.repository.AssertNotCalled(s.T(), "ListUnpublished", mock.Anything, mock.Anything)
	s.locker.AssertExpectations(s.T())
}

func (s *RelaySuite) TestTickDrainsWhileFullBatch() {
	lock := &mockLock{}
	s.locker.On("TryLock", mock.Anything, "outbox:test", time.Minute).Return(lock, nil).Once()
	lock.On("Release", mock.Anything).Return(nil).Once()

	// cfg.BatchSize is 10. First batch: 10 events (all published) → drain again.
	// Second batch: empty → stop.
	full := make([]EventEnvelope, 10)
	for i := range full {
		full[i] = evt("full", "A")
	}
	s.repository.On("ListUnpublished", mock.Anything, 10).Return(full, nil).Once()
	s.repository.On("ListUnpublished", mock.Anything, 10).Return([]EventEnvelope{}, nil).Once()
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil)
	s.repository.On("MarkPublished", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	s.r.tick(context.Background())

	s.repository.AssertNumberOfCalls(s.T(), "ListUnpublished", 2)
	lock.AssertExpectations(s.T())
}

func (s *RelaySuite) TestRunStopsOnContextCancel() {
	// Always not-leader so ticks are cheap; Run must still exit on cancel.
	s.locker.On("TryLock", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.r.Run(ctx); close(done) }()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		s.FailNow("Run did not return after context cancel")
	}
}

func (s *RelaySuite) TestRunStopsOnClose() {
	// Always not-leader so ticks are cheap; Run must exit once Close is called.
	s.locker.On("TryLock", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	done := make(chan struct{})
	go func() { s.r.Run(context.Background()); close(done) }()

	s.Require().NoError(s.r.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		s.FailNow("Run did not return after Close")
	}
}

func (s *RelaySuite) TestCloseIsIdempotent() {
	s.NotPanics(func() {
		s.NoError(s.r.Close())
		s.NoError(s.r.Close())
	})
}

func (s *RelaySuite) TestDefaultRelayConfig() {
	cfg := DefaultRelayConfig("k")

	s.Equal(200*time.Millisecond, cfg.IdleInterval)
	s.Equal(500, cfg.BatchSize)
	s.Equal("k", cfg.LockKey)
	s.Equal(30*time.Second, cfg.LockTTL)
	s.Equal(7*24*time.Hour, cfg.Retention)
}

func (s *RelaySuite) TestNewRelayWithDefaultConfigSucceeds() {
	r, err := NewRelay(s.repository, s.sink, s.locker, NopLogger, DefaultRelayConfig("k"))

	s.Require().NoError(err)
	s.NotNil(r)
}

func (s *RelaySuite) TestNewRelayEmptyConfigFailsOnLockKey() {
	r, err := NewRelay(s.repository, s.sink, s.locker, NopLogger, RelayConfig{})

	s.Nil(r)
	s.Require().ErrorIs(err, ErrInvalidRelayConfig)
	s.Contains(err.Error(), "LockKey")
}

func (s *RelaySuite) TestNewRelayInvalidConfig() {
	valid := func() RelayConfig {
		cfg := DefaultRelayConfig("k")
		cfg.IdleInterval = time.Millisecond
		cfg.LockTTL = time.Minute
		return cfg
	}

	tests := map[string]RelayConfig{
		"IdleInterval zero": func() RelayConfig { c := valid(); c.IdleInterval = 0; return c }(),
		"BatchSize zero":    func() RelayConfig { c := valid(); c.BatchSize = 0; return c }(),
		"LockTTL zero":      func() RelayConfig { c := valid(); c.LockTTL = 0; return c }(),
		"Retention zero":    func() RelayConfig { c := valid(); c.Retention = 0; return c }(),
		"LockTTL below IdleInterval": func() RelayConfig {
			c := valid()
			c.IdleInterval = time.Minute
			c.LockTTL = time.Second
			return c
		}(),
	}

	for name, cfg := range tests {
		s.Run(name, func() {
			r, err := NewRelay(s.repository, s.sink, s.locker, NopLogger, cfg)

			s.Nil(r)
			s.Require().ErrorIs(err, ErrInvalidRelayConfig)
		})
	}
}

func (s *RelaySuite) TestNewRelayNilLoggerAccepted() {
	r, err := NewRelay(s.repository, s.sink, s.locker, nil, DefaultRelayConfig("k"))

	s.Require().NoError(err)
	s.NotNil(r)
}
