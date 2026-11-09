package ember

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// gatedHandler records the events it starts and holds each one until its gate opens.
type gatedHandler struct {
	mu      sync.Mutex
	started []string
	gates   map[string]chan struct{}
}

func newGatedHandler() *gatedHandler {
	return &gatedHandler{gates: map[string]chan struct{}{}}
}

func (h *gatedHandler) consume(_ context.Context, e AckableEventEnvelope) {
	h.mu.Lock()
	h.started = append(h.started, e.ID)
	gate := h.gates[e.ID]
	h.mu.Unlock()
	if gate != nil {
		<-gate
	}
}

// hold makes the handler block on id until release is called.
func (h *gatedHandler) hold(ids ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, id := range ids {
		h.gates[id] = make(chan struct{})
	}
}

func (h *gatedHandler) release(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	close(h.gates[id])
}

func (h *gatedHandler) startedIDs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.started)
}

func (h *gatedHandler) hasStarted(id string) bool {
	return slices.Contains(h.startedIDs(), id)
}

type ConflatingConsumerSuite struct {
	suite.Suite

	handler  *gatedHandler
	ch       chan AckableEventEnvelope
	consumer *ConflatingConsumer

	mu    sync.Mutex
	acked []string
}

func TestConflatingConsumerSuite(t *testing.T) { suite.Run(t, new(ConflatingConsumerSuite)) }

func (s *ConflatingConsumerSuite) SetupTest() {
	s.handler = newGatedHandler()
	s.acked = nil
	s.start(4)
}

func (s *ConflatingConsumerSuite) TearDownTest() {
	s.consumer.Stop()
}

// start replaces the suite's consumer with one handling up to limit entities at once.
func (s *ConflatingConsumerSuite) start(limit int) {
	if s.consumer != nil {
		s.consumer.Stop()
	}
	s.ch = make(chan AckableEventEnvelope)
	s.consumer = NewConflatingConsumer(limit, NopLogger)
	s.consumer.Run(context.Background(), "sub", s.ch, s.handler.consume)
}

// send delivers an event whose Ack the suite records. The consumer itself acks only
// superseded events; handled ones are acked by the subscriber's consume, not here.
func (s *ConflatingConsumerSuite) send(id, entityID string) {
	s.ch <- AckableEventEnvelope{
		EventEnvelope: EventEnvelope{ID: id, EntityID: entityID},
		Ack: func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.acked = append(s.acked, id)
		},
		Nack: func() {},
	}
}

func (s *ConflatingConsumerSuite) ackedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.acked)
}

func (s *ConflatingConsumerSuite) awaitStarted(id string) {
	s.Require().Eventually(func() bool { return s.handler.hasStarted(id) }, time.Second, time.Millisecond, "%s never started", id)
}

func (s *ConflatingConsumerSuite) TestHandlesOnlyTheNewestEventThatArrivedWhileBusy() {
	s.handler.hold("a1")
	s.send("a1", "A")
	s.awaitStarted("a1")

	s.send("a2", "A")
	s.send("a3", "A")
	s.Eventually(func() bool { return slices.Equal(s.ackedIDs(), []string{"a2"}) }, time.Second, time.Millisecond)
	s.Never(func() bool { return s.handler.hasStarted("a3") }, 50*time.Millisecond, time.Millisecond, "one event per entity at a time")

	s.handler.release("a1")
	s.awaitStarted("a3")
	s.Equal([]string{"a1", "a3"}, s.handler.startedIDs())
	s.Equal([]string{"a2"}, s.ackedIDs(), "the newest event is left for the subscriber to ack")
}

func (s *ConflatingConsumerSuite) TestBusyEntityDoesNotHoldUpOthers() {
	s.handler.hold("a1")
	s.send("a1", "A")
	s.awaitStarted("a1")

	s.send("a2", "A")
	s.send("b1", "B")

	s.awaitStarted("b1")
	s.handler.release("a1")
	s.awaitStarted("a2")
}

func (s *ConflatingConsumerSuite) TestEntityIsFreeAgainAfterItsQueueDrains() {
	s.send("a1", "A")
	s.awaitStarted("a1")

	s.send("a2", "A")

	s.awaitStarted("a2")
	s.Empty(s.ackedIDs())
}

func (s *ConflatingConsumerSuite) TestLimitCapsEntitiesInFlight() {
	s.start(2)
	s.handler.hold("a1", "b1")
	s.send("a1", "A")
	s.send("b1", "B")
	s.awaitStarted("a1")
	s.awaitStarted("b1")

	go s.send("c1", "C")
	s.Never(func() bool { return s.handler.hasStarted("c1") }, 50*time.Millisecond, time.Millisecond)

	s.handler.release("a1")
	s.awaitStarted("c1")
	s.handler.release("b1")
}

func (s *ConflatingConsumerSuite) TestEventsWithoutEntityAreNeitherSerializedNorConflated() {
	s.handler.hold("x1", "x2")
	s.send("x1", "")
	s.send("x2", "")

	s.awaitStarted("x1")
	s.awaitStarted("x2")
	s.Empty(s.ackedIDs())
	s.handler.release("x1")
	s.handler.release("x2")
}

func (s *ConflatingConsumerSuite) TestStopWaitsForRunningHandlers() {
	s.handler.hold("a1")
	s.send("a1", "A")
	s.awaitStarted("a1")

	stopped := make(chan struct{})
	go func() {
		s.consumer.Stop()
		close(stopped)
	}()
	s.Never(func() bool {
		select {
		case <-stopped:
			return true
		default:
			return false
		}
	}, 50*time.Millisecond, time.Millisecond)

	s.handler.release("a1")
	s.Eventually(func() bool {
		select {
		case <-stopped:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}
