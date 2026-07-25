package ext

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v7"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/klemen-forstneric/ember"
)

func envelopes() []ember.EventEnvelope {
	return []ember.EventEnvelope{{
		ID:        "e1",
		EntityID:  "A",
		Event:     &ember.MarshaledEvent{Type: "T", Data: []byte("{}")},
		Timestamp: time.Unix(0, 1).UTC(),
	}}
}

func twoEnvelopes() []ember.EventEnvelope {
	e := envelopes()
	e = append(e, ember.EventEnvelope{
		ID:        "e2",
		EntityID:  "A",
		Event:     &ember.MarshaledEvent{Type: "T", Data: []byte("{}")},
		Timestamp: time.Unix(0, 2).UTC(),
	})
	return e
}

// fastConfig keeps waits sub-millisecond so exhaustion tests stay quick.
func fastConfig(tries int) RetryingSinkConfig {
	return RetryingSinkConfig{
		InitialInterval: time.Millisecond,
		MaxInterval:     time.Millisecond,
		MaxTries:        tries,
	}
}

type RetryingSinkSuite struct {
	suite.Suite
	ctx  context.Context
	sink *mockSink
}

func TestRetryingSinkSuite(t *testing.T) { suite.Run(t, new(RetryingSinkSuite)) }

func (s *RetryingSinkSuite) SetupTest() {
	s.ctx = context.Background()
	s.sink = &mockSink{}
}

func (s *RetryingSinkSuite) TearDownTest() {
	s.sink.AssertExpectations(s.T())
}

func (s *RetryingSinkSuite) TestPublishSucceedsWithoutRetry() {
	envs := envelopes()
	s.sink.On("Publish", mock.Anything, envs).Return(1, nil).Once()
	r := NewRetryingSink(fastConfig(3), s.sink, ember.NopLogger)

	n, err := r.Publish(s.ctx, envs)

	s.Require().NoError(err)
	s.Equal(1, n)
}

func (s *RetryingSinkSuite) TestPublishRetriesThenSucceeds() {
	envs := envelopes()
	// testify consumes Once() expectations in declaration order.
	s.sink.On("Publish", mock.Anything, envs).Return(0, errors.New("down")).Once()
	s.sink.On("Publish", mock.Anything, envs).Return(1, nil).Once()
	r := NewRetryingSink(fastConfig(3), s.sink, ember.NopLogger)

	n, err := r.Publish(s.ctx, envs)

	s.Require().NoError(err)
	s.Equal(1, n)
	s.sink.AssertNumberOfCalls(s.T(), "Publish", 2)
}

func (s *RetryingSinkSuite) TestPublishRetriesRemainingSuffix() {
	envs := twoEnvelopes()
	s.sink.On("Publish", mock.Anything, envs).Return(1, errors.New("down")).Once()
	s.sink.On("Publish", mock.Anything, envs[1:]).Return(1, nil).Once()
	r := NewRetryingSink(fastConfig(3), s.sink, ember.NopLogger)

	n, err := r.Publish(s.ctx, envs)

	s.Require().NoError(err)
	s.Equal(2, n, "returned count must be the total across both attempts")
	s.sink.AssertNumberOfCalls(s.T(), "Publish", 2)
}

func (s *RetryingSinkSuite) TestPublishStopsAtMaxTries() {
	envs := envelopes()
	sinkErr := errors.New("down")
	s.sink.On("Publish", mock.Anything, envs).Return(0, sinkErr)
	logger := &mockLogger{}
	logger.On("Warn", "Failed to publish events, retrying...")
	logger.On("Error", "Failed to publish events, tries exhausted", mock.Anything).Once()
	r := NewRetryingSink(fastConfig(3), s.sink, logger)

	n, err := r.Publish(s.ctx, envs)

	s.Require().ErrorIs(err, sinkErr, "the sink error stays in the chain via RetryError.LastErr")
	s.Require().ErrorIs(err, backoff.ErrExhausted)
	s.Equal(0, n)
	s.sink.AssertNumberOfCalls(s.T(), "Publish", 3)
	logger.AssertExpectations(s.T())
}

func (s *RetryingSinkSuite) TestMaxTriesOneDisablesRetrying() {
	envs := envelopes()
	s.sink.On("Publish", mock.Anything, envs).Return(0, errors.New("down")).Once()
	r := NewRetryingSink(fastConfig(1), s.sink, ember.NopLogger)

	n, err := r.Publish(s.ctx, envs)

	s.Require().Error(err)
	s.Equal(0, n)
	s.sink.AssertNumberOfCalls(s.T(), "Publish", 1)
}

func (s *RetryingSinkSuite) TestPublishStopsOnContextCancel() {
	envs := envelopes()
	ctx, cancel := context.WithCancel(context.Background())
	s.sink.On("Publish", mock.Anything, envs).Return(0, errors.New("down")).Once().
		Run(func(mock.Arguments) { cancel() })
	// MaxTries of 100 would keep going for a long time if cancellation were ignored.
	r := NewRetryingSink(fastConfig(100), s.sink, ember.NopLogger)

	n, err := r.Publish(ctx, envs)

	s.Require().ErrorIs(err, context.Canceled)
	s.Equal(0, n)
	s.sink.AssertNumberOfCalls(s.T(), "Publish", 1)
}

func (s *RetryingSinkSuite) TestZeroConfigAppliesDefaults() {
	r := NewRetryingSink(RetryingSinkConfig{}, s.sink, ember.NopLogger)

	s.Equal(100*time.Millisecond, r.config.InitialInterval)
	s.Equal(time.Second, r.config.MaxInterval)
	s.Equal(3, r.config.MaxTries)
}
