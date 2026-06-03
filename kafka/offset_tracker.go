package kafka

import (
	"sync"

	"github.com/segmentio/kafka-go"
)

type partitionKey struct {
	topic     string
	partition int
}

// partition tracks, for one Kafka partition, the per-offset delivery attempts
// and which offsets are finished (acked or given up), plus the cursor — the
// lowest offset not yet committed. Kafka commits are cumulative, so the
// committed offset can only advance across a contiguous run of finished offsets.
type partition struct {
	cursor   int64
	started  bool
	done     map[int64]bool
	attempts map[int64]int
}

// offsetTracker owns the per-partition state for one reader. All methods are
// safe for concurrent use: Ack/Nack closures run on downstream worker
// goroutines while the fetch loop registers new offsets.
//
// The tracker assumes a partition's offsets are delivered contiguously and in
// increasing order from the committed baseline — true for kafka-go consumer
// groups reading standard topics in the default ReadUncommitted mode. Gaps
// (e.g. transaction markers or log compaction, neither used here) would stall a
// partition's watermark at the first missing offset. Callers must only complete
// offsets they registered; completing an unregistered offset records a done
// entry the cursor can never reach.
type offsetTracker struct {
	mu    sync.Mutex
	parts map[partitionKey]*partition
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{parts: map[partitionKey]*partition{}}
}

// part returns the partition state for m, creating it on first use. Caller holds mu.
func (t *offsetTracker) part(m kafka.Message) *partition {
	k := partitionKey{topic: m.Topic, partition: m.Partition}
	p, ok := t.parts[k]
	if !ok {
		p = &partition{done: map[int64]bool{}, attempts: map[int64]int{}}
		t.parts[k] = p
	}
	return p
}

// register records the first delivery of a freshly fetched message. It sets the
// partition cursor on the first message ever seen for that partition (Kafka
// delivers a partition's offsets in increasing order from the committed point,
// so the first fetched offset is the commit baseline) and marks attempt 1.
func (t *offsetTracker) register(m kafka.Message) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.part(m)
	if !p.started {
		p.cursor = m.Offset
		p.started = true
	}
	p.attempts[m.Offset] = 1
	return 1
}

// attempt returns the current 1-based attempt number for m's offset, or 0 if
// the offset was never registered. The Subscriber only calls this for a message
// it has already registered, so it always sees a value >= 1.
func (t *offsetTracker) attempt(m kafka.Message) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.part(m).attempts[m.Offset]
}

// retry increments and returns the attempt number for a redelivery.
func (t *offsetTracker) retry(m kafka.Message) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.part(m)
	p.attempts[m.Offset]++
	return p.attempts[m.Offset]
}

// complete marks m's offset finished (acked or given up) and advances the
// cursor across any contiguous run of finished offsets. It returns the message
// to commit (carrying the highest contiguous finished offset) and true when the
// cursor advanced; otherwise ok is false.
func (t *offsetTracker) complete(m kafka.Message) (kafka.Message, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.part(m)
	p.done[m.Offset] = true
	delete(p.attempts, m.Offset)

	advanced := false
	for p.done[p.cursor] {
		delete(p.done, p.cursor)
		p.cursor++
		advanced = true
	}
	if !advanced {
		return kafka.Message{}, false
	}
	return kafka.Message{Topic: m.Topic, Partition: m.Partition, Offset: p.cursor - 1}, true
}
