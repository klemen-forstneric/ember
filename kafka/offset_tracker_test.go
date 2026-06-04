package kafka

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func msgAt(partition int, offset int64) kafka.Message {
	return kafka.Message{Topic: "orders", Partition: partition, Offset: offset}
}

func TestTrackerRegisterAndRetryCountAttempts(t *testing.T) {
	tr := newOffsetTracker()
	m := msgAt(0, 5)

	assert.Equal(t, 1, tr.register(m), "register attempt")
	assert.Equal(t, 1, tr.attempt(m), "attempt")
	assert.Equal(t, 2, tr.retry(m), "first retry")
	assert.Equal(t, 3, tr.retry(m), "second retry")
}

func TestTrackerCommitsSingleOffset(t *testing.T) {
	tr := newOffsetTracker()
	m := msgAt(0, 5)
	tr.register(m)

	cm, ok := tr.complete(m)
	require.True(t, ok, "expected a commit after completing the only outstanding offset")
	assert.Equal(t, int64(5), cm.Offset)
	assert.Equal(t, 0, cm.Partition)
	assert.Equal(t, "orders", cm.Topic)
}

func TestTrackerHoldsCommitUntilContiguous(t *testing.T) {
	tr := newOffsetTracker()
	m1, m2, m3 := msgAt(0, 1), msgAt(0, 2), msgAt(0, 3)
	tr.register(m1)
	tr.register(m2)
	tr.register(m3)

	// Completing 3 first must NOT advance the watermark (1 and 2 still pending).
	_, ok := tr.complete(m3)
	assert.False(t, ok, "did not expect a commit while offset 1 and 2 are pending")

	// Completing 1 advances the watermark to 1 only (2 still pending).
	cm, ok := tr.complete(m1)
	require.True(t, ok)
	assert.Equal(t, int64(1), cm.Offset)

	// Completing 2 now jumps the watermark across 2 and the already-done 3.
	cm, ok = tr.complete(m2)
	require.True(t, ok)
	assert.Equal(t, int64(3), cm.Offset)
}

func TestTrackerPartitionsAreIndependent(t *testing.T) {
	tr := newOffsetTracker()
	p0, p1 := msgAt(0, 10), msgAt(1, 20)
	tr.register(p0)
	tr.register(p1)

	cm, ok := tr.complete(p1)
	require.True(t, ok)
	assert.Equal(t, 1, cm.Partition)
	assert.Equal(t, int64(20), cm.Offset)

	cm, ok = tr.complete(p0)
	require.True(t, ok)
	assert.Equal(t, 0, cm.Partition)
	assert.Equal(t, int64(10), cm.Offset)
}
