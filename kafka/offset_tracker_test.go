package kafka

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func msgAt(partition int, offset int64) kafka.Message {
	return kafka.Message{Topic: "orders", Partition: partition, Offset: offset}
}

func TestTrackerRegisterAndRetryCountAttempts(t *testing.T) {
	tr := newOffsetTracker()
	m := msgAt(0, 5)

	if got := tr.register(m); got != 1 {
		t.Errorf("register attempt: got %d, want 1", got)
	}
	if got := tr.attempt(m); got != 1 {
		t.Errorf("attempt: got %d, want 1", got)
	}
	if got := tr.retry(m); got != 2 {
		t.Errorf("first retry: got %d, want 2", got)
	}
	if got := tr.retry(m); got != 3 {
		t.Errorf("second retry: got %d, want 3", got)
	}
}

func TestTrackerCommitsSingleOffset(t *testing.T) {
	tr := newOffsetTracker()
	m := msgAt(0, 5)
	tr.register(m)

	cm, ok := tr.complete(m)
	if !ok {
		t.Fatal("expected a commit after completing the only outstanding offset")
	}
	if cm.Offset != 5 || cm.Partition != 0 || cm.Topic != "orders" {
		t.Errorf("commit message: got %+v, want offset 5 / partition 0 / orders", cm)
	}
}

func TestTrackerHoldsCommitUntilContiguous(t *testing.T) {
	tr := newOffsetTracker()
	m1, m2, m3 := msgAt(0, 1), msgAt(0, 2), msgAt(0, 3)
	tr.register(m1)
	tr.register(m2)
	tr.register(m3)

	// Completing 3 first must NOT advance the watermark (1 and 2 still pending).
	if _, ok := tr.complete(m3); ok {
		t.Fatal("did not expect a commit while offset 1 and 2 are pending")
	}

	// Completing 1 advances the watermark to 1 only (2 still pending).
	cm, ok := tr.complete(m1)
	if !ok || cm.Offset != 1 {
		t.Fatalf("expected commit at offset 1, got ok=%v offset=%d", ok, cm.Offset)
	}

	// Completing 2 now jumps the watermark across 2 and the already-done 3.
	cm, ok = tr.complete(m2)
	if !ok || cm.Offset != 3 {
		t.Fatalf("expected commit to jump to offset 3, got ok=%v offset=%d", ok, cm.Offset)
	}
}

func TestTrackerPartitionsAreIndependent(t *testing.T) {
	tr := newOffsetTracker()
	p0, p1 := msgAt(0, 10), msgAt(1, 20)
	tr.register(p0)
	tr.register(p1)

	cm, ok := tr.complete(p1)
	if !ok || cm.Partition != 1 || cm.Offset != 20 {
		t.Fatalf("partition 1 commit: ok=%v %+v", ok, cm)
	}
	cm, ok = tr.complete(p0)
	if !ok || cm.Partition != 0 || cm.Offset != 10 {
		t.Fatalf("partition 0 commit: ok=%v %+v", ok, cm)
	}
}
