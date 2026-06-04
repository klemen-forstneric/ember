package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
)

// mockWriter is a testify mock for the writer interface. WriteMessages records
// every call through mock.Mock so tests can assert routing and that a
// multi-topic publish is a single batched call via written().
type mockWriter struct {
	mock.Mock
}

func (m *mockWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	return m.Called(ctx, msgs).Error(0)
}

func (m *mockWriter) Close() error {
	return m.Called().Error(0)
}

// written flattens every kafka.Message passed to WriteMessages, in call order.
func (m *mockWriter) written() []kafka.Message {
	var out []kafka.Message
	for _, c := range m.Calls {
		if c.Method == "WriteMessages" {
			out = append(out, c.Arguments.Get(1).([]kafka.Message)...)
		}
	}
	return out
}

// mockReader is a testify mock for the reader interface. FetchMessage is
// channel-backed (the standard testify pattern for a blocking, streaming
// method): tests push onto in to drive delivery. The non-streaming methods go
// through mock.Mock; committed() reconstructs the commit watermark history from
// the recorded CommitMessages calls.
type mockReader struct {
	mock.Mock
	in chan kafka.Message
}

func newMockReader() *mockReader {
	return &mockReader{in: make(chan kafka.Message, 16)}
}

func (r *mockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case m := <-r.in:
		return m, nil
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	}
}

func (r *mockReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return r.Called(ctx, msgs).Error(0)
}

func (r *mockReader) Close() error {
	return r.Called().Error(0)
}

func (r *mockReader) MaxDeliveries() (int, bool) {
	args := r.Called()
	return args.Int(0), args.Bool(1)
}

func (r *mockReader) RetryBackoff() time.Duration {
	return r.Called().Get(0).(time.Duration)
}

// committed flattens every kafka.Message passed to CommitMessages, in call
// order. Safe to read once the session goroutines have stopped (after Stop).
func (r *mockReader) committed() []kafka.Message {
	var out []kafka.Message
	for _, c := range r.Calls {
		if c.Method == "CommitMessages" {
			out = append(out, c.Arguments.Get(1).([]kafka.Message)...)
		}
	}
	return out
}

// mockConsumerRegistry is a testify mock for the consumerRegistry interface.
type mockConsumerRegistry struct {
	mock.Mock
}

func (m *mockConsumerRegistry) Get(ctx context.Context, subscription string) (reader, error) {
	args := m.Called(ctx, subscription)
	var r reader
	if v := args.Get(0); v != nil {
		r = v.(reader)
	}
	return r, args.Error(1)
}

func (m *mockConsumerRegistry) Close() error {
	return m.Called().Error(0)
}
