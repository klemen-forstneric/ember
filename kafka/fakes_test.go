package kafka

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

// fakeWriter records every WriteMessages call so tests can assert routing and
// that a multi-topic publish is a single batched call.
type fakeWriter struct {
	mu      sync.Mutex
	written []kafka.Message
	calls   int
	err     error
	closed  bool
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if w.err != nil {
		return w.err
	}
	w.written = append(w.written, msgs...)
	return nil
}

func (w *fakeWriter) Close() error {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	return nil
}
