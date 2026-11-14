package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// mockSource is a Source without Unsubscribe.
type mockSource struct{ mock.Mock }

func (m *mockSource) Subscribe(ctx context.Context, name string) (<-chan AckableEventEnvelope, error) {
	args := m.Called(ctx, name)
	ch, _ := args.Get(0).(<-chan AckableEventEnvelope)
	return ch, args.Error(1)
}

func (m *mockSource) Stop() { m.Called() }

// mockUnsubscribingSource is a Source that can end one subscription.
type mockUnsubscribingSource struct{ mockSource }

func (m *mockUnsubscribingSource) Unsubscribe(name string) error { return m.Called(name).Error(0) }

func TestUnsubscribeDelegatesToTheSource(t *testing.T) {
	src := &mockUnsubscribingSource{}
	src.On("Unsubscribe", "ws.c1").Return(errors.New("not subscribed"))
	s := NewSubscriber(nil, src, nil, NopLogger)

	err := s.Unsubscribe("ws.c1")

	assert.EqualError(t, err, "not subscribed")
	src.AssertExpectations(t)
}

func TestUnsubscribeWithoutSourceSupportErrors(t *testing.T) {
	s := NewSubscriber(nil, &mockSource{}, nil, NopLogger)

	assert.ErrorIs(t, s.Unsubscribe("ws.c1"), ErrUnsubscribeUnsupported)
}

// mockConsumer records Stop.
type mockConsumer struct{ mock.Mock }

func (m *mockConsumer) Run(ctx context.Context, name string, ch <-chan AckableEventEnvelope, consume ConsumeFunc) {
	m.Called(ctx, name, ch, consume)
}

func (m *mockConsumer) Stop() { m.Called() }

// Handlers ack through the source's consumers, so the source closes only after they finish.
func TestStopDrainsTheConsumerBeforeStoppingTheSource(t *testing.T) {
	var order []string
	src := &mockSource{}
	src.On("Stop").Run(func(mock.Arguments) { order = append(order, "source") })
	consumer := &mockConsumer{}
	consumer.On("Stop").Run(func(mock.Arguments) { order = append(order, "consumer") })
	s := NewSubscriber(nil, src, consumer, NopLogger)

	s.Stop()

	assert.Equal(t, []string{"consumer", "source"}, order)
}
