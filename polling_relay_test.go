package ember

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// sameIDs matches a slice of ids regardless of order: publish groups events by
// entity through a map, so the order it accumulates them in is not defined.
func sameIDs(want ...string) any {
	slices.Sort(want)
	return mock.MatchedBy(func(got []string) bool {
		got = slices.Clone(got)
		slices.Sort(got)
		return slices.Equal(got, want)
	})
}

func evt(id, entityID string) EventEnvelope {
	return EventEnvelope{
		ID:        id,
		EntityID:  entityID,
		Event:     &MarshaledEvent{Type: "T", Data: []byte("{}")},
		Timestamp: time.Unix(0, 1).UTC(),
	}
}

func testRelayConfig() PollingRelayConfig {
	return PollingRelayConfig{
		IdleInterval: time.Millisecond,
		BatchSize:    3,
		LockKey:      "outbox:test",
		Retention:    24 * time.Hour,

		PublishConcurrency: 2,
		MaxHold:            time.Second,
	}
}

func versioned(id, entityID string, version uint64, index int) EventEnvelope {
	e := evt(id, entityID)
	e.Version = version
	e.Index = index
	return e
}

type PollingRelaySuite struct {
	suite.Suite
	repository *mockEventRepository
	sink       *mockSink
	locker     *mockLocker
	r          *PollingRelay
}

func TestPollingRelaySuite(t *testing.T) {
	suite.Run(t, new(PollingRelaySuite))
}

func (s *PollingRelaySuite) SetupTest() {
	s.repository = &mockEventRepository{}
	s.repository.On("Partitions").Return(1).Maybe()
	s.sink = &mockSink{}
	s.locker = &mockLocker{}
	r, err := NewPollingRelay(s.repository, s.sink, s.locker, NopLogger, testRelayConfig())
	s.Require().NoError(err)
	s.r = r
}

func (s *PollingRelaySuite) TestPublishBatchOneCallPerEntity() {
	batch := []EventEnvelope{evt("e1", "A"), evt("e2", "A"), evt("e3", "B")}
	s.repository.On("ListUnpublished", mock.Anything, 0, 3).Return(batch, nil).Once()

	s.sink.On("Publish", mock.Anything, []EventEnvelope{batch[0], batch[1]}).Return(nil).Once()
	s.sink.On("Publish", mock.Anything, []EventEnvelope{batch[2]}).Return(nil).Once()
	s.repository.On("MarkPublished", mock.Anything, sameIDs("e1", "e2", "e3"), mock.Anything).Return(nil).Once()

	published, err := s.r.publish(context.Background(), 0)

	s.Require().NoError(err)
	s.Equal(3, published)
	s.sink.AssertNumberOfCalls(s.T(), "Publish", 2)
	s.repository.AssertExpectations(s.T())
}

func (s *PollingRelaySuite) TestPublishBatchFailingGroupDoesNotBlockOtherEntities() {
	batch := []EventEnvelope{evt("e1", "A"), evt("e2", "A"), evt("e3", "B")}
	s.repository.On("ListUnpublished", mock.Anything, 0, 3).Return(batch, nil).Once()

	s.sink.On("Publish", mock.Anything, []EventEnvelope{batch[0], batch[1]}).
		Return(errors.New("route fail")).Once()
	s.sink.On("Publish", mock.Anything, []EventEnvelope{batch[2]}).Return(nil).Once()
	// Only e3 is marked; A's group never succeeded this round.
	s.repository.On("MarkPublished", mock.Anything, []string{"e3"}, mock.Anything).Return(nil).Once()

	published, err := s.r.publish(context.Background(), 0)

	s.Require().NoError(err)
	s.Equal(1, published)
	s.sink.AssertNumberOfCalls(s.T(), "Publish", 2)
	s.repository.AssertExpectations(s.T())
}

func (s *PollingRelaySuite) TestPublishBatchFailingGroupMarksNothingInThatGroup() {
	batch := []EventEnvelope{evt("e1", "A"), evt("e2", "A"), evt("e3", "A")}
	s.repository.On("ListUnpublished", mock.Anything, 0, 3).Return(batch, nil).Once()
	s.sink.On("Publish", mock.Anything, batch).Return(errors.New("broker down")).Once()

	logger := &mockLogger{}
	logger.On("Warn", "Failed to publish events, will retry").Once()
	r, err := NewPollingRelay(s.repository, s.sink, s.locker, logger, testRelayConfig())
	s.Require().NoError(err)
	s.r = r

	published, err := s.r.publish(context.Background(), 0)

	s.Require().NoError(err)
	s.Equal(0, published)
	logger.AssertExpectations(s.T())
	s.repository.AssertNotCalled(s.T(), "MarkPublished", mock.Anything, mock.Anything, mock.Anything)
}

func (s *PollingRelaySuite) TestPublishSendsEntitiesConcurrently() {
	batch := []EventEnvelope{evt("e1", "A"), evt("e2", "B")}
	s.repository.On("ListUnpublished", mock.Anything, 0, 3).Return(batch, nil).Once()
	// Each Publish waits until the other has started: a relay that sends one entity
	// at a time never gets past the first.
	var bothStarted sync.WaitGroup
	bothStarted.Add(2)
	s.sink.On("Publish", mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		bothStarted.Done()
		bothStarted.Wait()
	}).Return(nil).Twice()
	s.repository.On("MarkPublished", mock.Anything, sameIDs("e1", "e2"), mock.Anything).Return(nil).Once()

	done := make(chan struct{})
	go func() {
		defer close(done)
		published, err := s.r.publish(context.Background(), 0)
		s.NoError(err)
		s.Equal(2, published)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		s.FailNow("entities were published one at a time")
	}
	s.repository.AssertExpectations(s.T())
}

func (s *PollingRelaySuite) TestPublishConcurrencyIsCapped() {
	batch := []EventEnvelope{evt("e1", "A"), evt("e2", "B"), evt("e3", "C"), evt("e4", "D")}
	cfg := testRelayConfig()
	cfg.BatchSize = 4
	r, err := NewPollingRelay(s.repository, s.sink, s.locker, NopLogger, cfg)
	s.Require().NoError(err)
	s.repository.On("ListUnpublished", mock.Anything, 0, 4).Return(batch, nil).Once()
	var inFlight, peak atomic.Int32
	s.sink.On("Publish", mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		n := inFlight.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(10 * time.Millisecond)
		inFlight.Add(-1)
	}).Return(nil)
	s.repository.On("MarkPublished", mock.Anything, sameIDs("e1", "e2", "e3", "e4"), mock.Anything).Return(nil).Once()

	published, err := r.publish(context.Background(), 0)

	s.Require().NoError(err)
	s.Equal(4, published)
	s.Equal(int32(2), peak.Load(), "PublishConcurrency is 2")
}

func (s *PollingRelaySuite) TestTickNotLeaderDoesNothing() {
	// nil lock → someone else is leader this round.
	s.locker.On("TryLock", mock.Anything, "outbox:test").Return(nil, nil).Once()

	s.r.tick(context.Background(), 0)

	s.repository.AssertNotCalled(s.T(), "ListUnpublished", mock.Anything, mock.Anything, mock.Anything)
	s.locker.AssertExpectations(s.T())
}

func (s *PollingRelaySuite) TestPublishPreservesRepositoryOrderWithinAGroup() {
	batch := []EventEnvelope{
		versioned("e3", "A", 2, 0),
		versioned("e1", "A", 1, 0),
		versioned("e2", "A", 1, 1),
	}
	want := append([]EventEnvelope(nil), batch...)
	s.repository.On("ListUnpublished", mock.Anything, 0, 3).Return(batch, nil).Once()
	s.sink.On("Publish", mock.Anything, want).Return(nil).Once()
	s.repository.On("MarkPublished", mock.Anything, sameIDs("e1", "e2", "e3"), mock.Anything).Return(nil).Once()

	published, err := s.r.publish(context.Background(), 0)

	s.Require().NoError(err)
	s.Equal(3, published)
	s.repository.AssertExpectations(s.T())
}

func (s *PollingRelaySuite) TestTickDrainsWhileRoundsMakeProgress() {
	lock := &mockLock{}
	s.locker.On("TryLock", mock.Anything, "outbox:test").Return(lock, nil).Once()
	lock.On("Release", mock.Anything).Return(nil).Once()

	s.repository.On("ListUnpublished", mock.Anything, 0, 3).
		Return([]EventEnvelope{versioned("e1", "A", 1, 0)}, nil).Once()
	s.repository.On("ListUnpublished", mock.Anything, 0, 3).
		Return([]EventEnvelope{}, nil).Once()
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()
	s.repository.On("MarkPublished", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	s.r.tick(context.Background(), 0)

	s.repository.AssertNumberOfCalls(s.T(), "ListUnpublished", 2)
	lock.AssertExpectations(s.T())
}

func (s *PollingRelaySuite) TestTickStopsWhenARoundPublishesNothing() {
	lock := &mockLock{}
	s.locker.On("TryLock", mock.Anything, "outbox:test").Return(lock, nil).Once()
	lock.On("Release", mock.Anything).Return(nil).Once()

	batch := []EventEnvelope{versioned("e1", "A", 1, 0)}
	s.repository.On("ListUnpublished", mock.Anything, 0, 3).Return(batch, nil).Once()
	s.sink.On("Publish", mock.Anything, batch).Return(errors.New("broker down")).Once()

	s.r.tick(context.Background(), 0)

	s.repository.AssertNumberOfCalls(s.T(), "ListUnpublished", 1)
	s.repository.AssertNotCalled(s.T(), "MarkPublished", mock.Anything, mock.Anything, mock.Anything)
}

func (s *PollingRelaySuite) TestPartitionsUseTheirOwnLocksAndRows() {
	repo := &mockEventRepository{}
	repo.On("Partitions").Return(3)
	r, err := NewPollingRelay(repo, s.sink, s.locker, NopLogger, testRelayConfig())
	s.Require().NoError(err)
	held := &mockLock{}
	held.On("Release", mock.Anything).Return(nil).Once()
	s.locker.On("TryLock", mock.Anything, "outbox:test:1").Return(held, nil).Once()
	s.locker.On("TryLock", mock.Anything, "outbox:test:2").Return(nil, nil).Once() // another relay has it
	repo.On("ListUnpublished", mock.Anything, 1, 3).Return([]EventEnvelope{}, nil).Once()

	r.tick(context.Background(), 1)
	r.tick(context.Background(), 2)

	repo.AssertExpectations(s.T())
	repo.AssertNotCalled(s.T(), "ListUnpublished", mock.Anything, 2, mock.Anything)
	held.AssertExpectations(s.T())
}

func (s *PollingRelaySuite) TestTickReleasesTheLockAfterMaxHold() {
	cfg := testRelayConfig()
	cfg.MaxHold = 20 * time.Millisecond
	r, err := NewPollingRelay(s.repository, s.sink, s.locker, NopLogger, cfg)
	s.Require().NoError(err)
	lock := &mockLock{}
	lock.On("Release", mock.Anything).Return(nil).Once()
	s.locker.On("TryLock", mock.Anything, "outbox:test").Return(lock, nil).Once()
	// A partition that never empties, each round a little slow.
	s.repository.On("ListUnpublished", mock.Anything, 0, 3).Run(func(mock.Arguments) { time.Sleep(5 * time.Millisecond) }).
		Return([]EventEnvelope{versioned("e1", "A", 1, 0)}, nil)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil)
	s.repository.On("MarkPublished", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	more := r.tick(context.Background(), 0)

	s.True(more, "rows may remain, so the caller goes again without waiting")
	lock.AssertExpectations(s.T())
}

func (s *PollingRelaySuite) TestRunDrainsEveryPartition() {
	repo := &mockEventRepository{}
	repo.On("Partitions").Return(2)
	r, err := NewPollingRelay(repo, s.sink, s.locker, NopLogger, testRelayConfig())
	s.Require().NoError(err)
	drained := make(chan int, 2)
	for _, p := range []int{0, 1} {
		lock := &mockLock{}
		lock.On("Release", mock.Anything).Return(nil)
		s.locker.On("TryLock", mock.Anything, fmt.Sprintf("outbox:test:%d", p)).Return(lock, nil)
		repo.On("ListUnpublished", mock.Anything, p, 3).Run(func(mock.Arguments) {
			select {
			case drained <- p:
			default:
			}
		}).Return([]EventEnvelope{}, nil)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	got := map[int]bool{}
	for len(got) < 2 {
		select {
		case p := <-drained:
			got[p] = true
		case <-time.After(time.Second):
			s.FailNow("not every partition was drained", "drained: %v", got)
		}
	}
	cancel()
	<-done
}

func TestPartitionIsStablePerEntityAndInRange(t *testing.T) {
	assert.Equal(t, 0, Partition("conversation-1", 1))
	assert.Equal(t, 0, Partition("conversation-1", 0))
	seen := map[int]bool{}
	for i := range 1000 {
		id := fmt.Sprintf("conversation-%d", i)
		p := Partition(id, 16)
		require.GreaterOrEqual(t, p, 0)
		require.Less(t, p, 16)
		require.Equal(t, p, Partition(id, 16), "an entity always maps to the same partition")
		seen[p] = true
	}
	assert.Len(t, seen, 16, "entities spread over every partition")
}

func (s *PollingRelaySuite) TestRunStopsOnContextCancel() {
	// Always not-leader so ticks are cheap; Run must still exit on cancel.
	s.locker.On("TryLock", mock.Anything, mock.Anything).Return(nil, nil)

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

func (s *PollingRelaySuite) TestRunStopsOnClose() {
	// Always not-leader so ticks are cheap; Run must exit once Close is called.
	s.locker.On("TryLock", mock.Anything, mock.Anything).Return(nil, nil)

	done := make(chan struct{})
	go func() { s.r.Run(context.Background()); close(done) }()

	s.Require().NoError(s.r.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		s.FailNow("Run did not return after Close")
	}
}

func (s *PollingRelaySuite) TestCloseIsIdempotent() {
	s.NotPanics(func() {
		s.NoError(s.r.Close())
		s.NoError(s.r.Close())
	})
}

func (s *PollingRelaySuite) TestDefaultRelayConfig() {
	cfg := DefaultPollingRelayConfig("k")

	s.Equal(200*time.Millisecond, cfg.IdleInterval)
	s.Equal(500, cfg.BatchSize)
	s.Equal("k", cfg.LockKey)
	s.Equal(7*24*time.Hour, cfg.Retention)
	s.Equal(32, cfg.PublishConcurrency)
	s.Equal(10*time.Second, cfg.MaxHold)
}

func (s *PollingRelaySuite) TestNewRelayWithDefaultConfigSucceeds() {
	r, err := NewPollingRelay(s.repository, s.sink, s.locker, NopLogger, DefaultPollingRelayConfig("k"))

	s.Require().NoError(err)
	s.NotNil(r)
}

func (s *PollingRelaySuite) TestNewRelayEmptyConfigFailsOnLockKey() {
	r, err := NewPollingRelay(s.repository, s.sink, s.locker, NopLogger, PollingRelayConfig{})

	s.Nil(r)
	s.Require().ErrorIs(err, ErrInvalidRelayConfig)
	s.Contains(err.Error(), "LockKey")
}

func (s *PollingRelaySuite) TestNewRelayInvalidConfig() {
	valid := func() PollingRelayConfig {
		cfg := DefaultPollingRelayConfig("k")
		cfg.IdleInterval = time.Millisecond
		return cfg
	}

	tests := map[string]PollingRelayConfig{
		"IdleInterval zero": func() PollingRelayConfig { c := valid(); c.IdleInterval = 0; return c }(),
		"BatchSize zero":    func() PollingRelayConfig { c := valid(); c.BatchSize = 0; return c }(),
		"Retention zero":    func() PollingRelayConfig { c := valid(); c.Retention = 0; return c }(),
		"Concurrency zero":  func() PollingRelayConfig { c := valid(); c.PublishConcurrency = 0; return c }(),
		"MaxHold zero":      func() PollingRelayConfig { c := valid(); c.MaxHold = 0; return c }(),
	}

	for name, cfg := range tests {
		s.Run(name, func() {
			r, err := NewPollingRelay(s.repository, s.sink, s.locker, NopLogger, cfg)

			s.Nil(r)
			s.Require().ErrorIs(err, ErrInvalidRelayConfig)
		})
	}
}

func (s *PollingRelaySuite) TestNewRelayNilLoggerAccepted() {
	r, err := NewPollingRelay(s.repository, s.sink, s.locker, nil, DefaultPollingRelayConfig("k"))

	s.Require().NoError(err)
	s.NotNil(r)
}
