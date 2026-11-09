package pulsar

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type ProducerRegistrySuite struct {
	suite.Suite
	client *mockPulsarClient
}

func TestProducerRegistrySuite(t *testing.T) {
	suite.Run(t, new(ProducerRegistrySuite))
}

func (s *ProducerRegistrySuite) SetupTest() {
	s.client = &mockPulsarClient{}
}

func (s *ProducerRegistrySuite) TestCreatesKeyBasedBatchingProducers() {
	s.client.On("CreateProducer", mock.Anything).Return(nil, nil)

	r := NewProducerRegistry(s.client, map[string]string{"Created": "topic-a"})
	_, err := r.Get(context.Background(), "Created", "")
	s.Require().NoError(err)

	opts := s.client.createProducerOptions()
	s.Require().Len(opts, 1)
	s.Equal("topic-a", opts[0].Topic)
	s.Equal(pulsar.KeyBasedBatchBuilder, opts[0].BatcherBuilderType)
}

func (s *ProducerRegistrySuite) TestCachesOneProducerPerTopic() {
	s.client.On("CreateProducer", mock.Anything).Return(nil, nil)

	r := NewProducerRegistry(s.client, map[string]string{
		"Created": "topic-a",
		"Updated": "topic-a",
		"Removed": "topic-b",
	})

	ctx := context.Background()
	for _, t := range []string{"Created", "Updated", "Created", "Removed"} {
		_, err := r.Get(ctx, t, "")
		s.Require().NoError(err)
	}

	var topics []string
	for _, o := range s.client.createProducerOptions() {
		topics = append(topics, o.Topic)
	}
	s.Equal([]string{"topic-a", "topic-b"}, topics)
}

func (s *ProducerRegistrySuite) TestUnmappedEventTypeIsAnError() {
	r := NewProducerRegistry(s.client, map[string]string{"Created": "topic-a"})

	_, err := r.Get(context.Background(), "Unknown", "")
	s.Require().Error(err)
	s.Contains(err.Error(), "Unknown")
	s.client.AssertNotCalled(s.T(), "CreateProducer", mock.Anything)
}

func (s *ProducerRegistrySuite) TestCreateFailureIsNotCached() {
	s.client.On("CreateProducer", mock.Anything).Return(nil, errors.New("broker down")).Once()
	s.client.On("CreateProducer", mock.Anything).Return(nil, nil).Once()

	r := NewProducerRegistry(s.client, map[string]string{"Created": "topic-a"})
	ctx := context.Background()

	_, err := r.Get(ctx, "Created", "")
	s.Require().Error(err)

	_, err = r.Get(ctx, "Created", "")
	s.Require().NoError(err)
	s.Len(s.client.createProducerOptions(), 2)
}

func (s *ProducerRegistrySuite) TestEntityRouteGivesEachEntityItsOwnTopic() {
	s.client.On("CreateProducer", mock.Anything).Return(nil, nil)
	r := NewProducerRegistry(s.client, map[string]string{"Delta": "ws." + EntityPlaceholder})
	ctx := context.Background()

	for _, entity := range []string{"c1", "c2", "c1"} {
		_, err := r.Get(ctx, "Delta", entity)
		s.Require().NoError(err)
	}

	var topics []string
	for _, o := range s.client.createProducerOptions() {
		topics = append(topics, o.Topic)
	}
	s.Equal([]string{"ws.c1", "ws.c2"}, topics)
}

func (s *ProducerRegistrySuite) TestEntityRouteWithoutEntityIDIsAnError() {
	r := NewProducerRegistry(s.client, map[string]string{"Delta": "ws." + EntityPlaceholder})

	_, err := r.Get(context.Background(), "Delta", "")

	s.Require().ErrorContains(err, "Delta")
	s.client.AssertNotCalled(s.T(), "CreateProducer", mock.Anything)
}

// closableProducer stands in for a *pulsar.Producer the registry creates; only Close is called.
type closableProducer struct {
	pulsar.Producer
	closed int
}

func (p *closableProducer) Close() { p.closed++ }

func (s *ProducerRegistrySuite) TestCloseIdleClosesOnlyIdleProducers() {
	idle, busy := &closableProducer{}, &closableProducer{}
	s.client.On("CreateProducer", mock.MatchedBy(func(o pulsar.ProducerOptions) bool { return o.Topic == "ws.idle" })).Return(idle, nil).Once()
	s.client.On("CreateProducer", mock.MatchedBy(func(o pulsar.ProducerOptions) bool { return o.Topic == "ws.busy" })).Return(busy, nil).Once()
	r := NewProducerRegistry(s.client, map[string]string{"Delta": "ws." + EntityPlaceholder})
	ctx := context.Background()
	_, err := r.Get(ctx, "Delta", "idle")
	s.Require().NoError(err)
	_, err = r.Get(ctx, "Delta", "busy")
	s.Require().NoError(err)
	r.producers["ws.idle"].lastUsed = time.Now().Add(-2 * time.Minute)

	r.closeIdle(time.Now(), time.Minute)

	s.Equal(1, idle.closed)
	s.Zero(busy.closed)
	s.client.On("CreateProducer", mock.Anything).Return(nil, nil).Once()
	_, err = r.Get(ctx, "Delta", "idle")
	s.Require().NoError(err)
	s.Len(s.client.createProducerOptions(), 3, "a closed topic gets a fresh producer")
}

func (s *ProducerRegistrySuite) TestSlowCreationDoesNotHoldUpOtherTopics() {
	started, release := make(chan struct{}), make(chan struct{})
	s.client.On("CreateProducer", mock.MatchedBy(func(o pulsar.ProducerOptions) bool { return o.Topic == "ws.slow" })).
		Run(func(mock.Arguments) { close(started); <-release }).Return(nil, nil).Once()
	s.client.On("CreateProducer", mock.MatchedBy(func(o pulsar.ProducerOptions) bool { return o.Topic == "ws.fast" })).Return(nil, nil).Once()
	r := NewProducerRegistry(s.client, map[string]string{"Delta": "ws." + EntityPlaceholder})
	ctx := context.Background()

	slowDone := make(chan error, 1)
	go func() {
		_, err := r.Get(ctx, "Delta", "slow")
		slowDone <- err
	}()
	<-started

	fastDone := make(chan error, 1)
	go func() {
		_, err := r.Get(ctx, "Delta", "fast")
		fastDone <- err
	}()
	select {
	case err := <-fastDone:
		s.NoError(err)
	case <-time.After(time.Second):
		s.FailNow("a slow creation held up another topic")
	}

	close(release)
	s.NoError(<-slowDone)
}

func (s *ProducerRegistrySuite) TestConcurrentGetsForOneTopicCreateOneProducer() {
	started, release := make(chan struct{}), make(chan struct{})
	s.client.On("CreateProducer", mock.Anything).Run(func(mock.Arguments) { close(started); <-release }).Return(nil, nil).Once()
	r := NewProducerRegistry(s.client, map[string]string{"Delta": "ws." + EntityPlaceholder})
	ctx := context.Background()

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := r.Get(ctx, "Delta", "c1")
			s.NoError(err)
		}()
	}
	<-started
	time.Sleep(20 * time.Millisecond) // let the other callers reach the registry while it creates
	close(release)
	wg.Wait()

	s.client.AssertNumberOfCalls(s.T(), "CreateProducer", 1)
}
