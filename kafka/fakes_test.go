package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

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
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

// fakeReader drives the Subscriber: push messages onto in, and the manager
// goroutine reads them off FetchMessage. CommitMessages are recorded so tests
// can assert the cumulative-commit watermark.
type fakeReader struct {
	in        chan kafka.Message
	mu        sync.Mutex
	committed []kafka.Message
	closed    bool
	maxDel    int
	capped    bool
	backoff   time.Duration
}

func newFakeReader(maxDeliveries int, capped bool) *fakeReader {
	return &fakeReader{
		in:      make(chan kafka.Message, 16),
		maxDel:  maxDeliveries,
		capped:  capped,
		backoff: time.Millisecond,
	}
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case m := <-r.in:
		return m, nil
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	}
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	r.committed = append(r.committed, msgs...)
	r.mu.Unlock()
	return nil
}

func (r *fakeReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

func (r *fakeReader) MaxDeliveries() (int, bool)  { return r.maxDel, r.capped }
func (r *fakeReader) RetryBackoff() time.Duration { return r.backoff }

func (r *fakeReader) commits() []kafka.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]kafka.Message, len(r.committed))
	copy(out, r.committed)
	return out
}

// fakeConsumerRegistry maps subscription name -> reader.
type fakeConsumerRegistry struct {
	mu         sync.Mutex
	readers    map[string]reader
	getErr     error
	closeCalls int
}

func (f *fakeConsumerRegistry) Get(_ context.Context, subscription string) (reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	r, ok := f.readers[subscription]
	if !ok {
		return nil, fmt.Errorf("unknown subscription %q", subscription)
	}
	return r, nil
}

func (f *fakeConsumerRegistry) Close() error {
	f.mu.Lock()
	f.closeCalls++
	f.mu.Unlock()
	return nil
}
