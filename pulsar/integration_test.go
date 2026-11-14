package pulsar

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
	emberjson "github.com/klemen-forstneric/ember/json"
)

const testPulsarTimeout = 5 * time.Second

func testPulsarURL() string {
	if v := os.Getenv("EMBER_TEST_PULSAR"); v != "" {
		return v
	}
	return "pulsar://localhost:6650"
}

var dialTestPulsar = sync.OnceValues(func() (pulsar.Client, error) {
	return pulsar.NewClient(pulsar.ClientOptions{
		URL:               testPulsarURL(),
		ConnectionTimeout: testPulsarTimeout,
		OperationTimeout:  testPulsarTimeout,
		Logger:            plog.DefaultNopLogger(),
	})
})

func connectTestPulsar(t *testing.T) pulsar.Client {
	t.Helper()

	if testing.Short() {
		t.Skip("pulsar integration test skipped by -short")
	}

	client, err := dialTestPulsar()
	if err != nil {
		t.Skipf("pulsar unavailable at %s: %v", testPulsarURL(), err)
	}

	probe, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: fmt.Sprintf("persistent://public/default/ember-probe-%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Skipf("pulsar unavailable at %s: %v", testPulsarURL(), err)
	}
	probe.Close()

	return client
}

func seqEnvelope(entityID string, n int) ember.EventEnvelope {
	return ember.EventEnvelope{
		ID:          fmt.Sprintf("%s-%d", entityID, n),
		EntityID:    entityID,
		OrderingKey: entityID,
		Event:       &ember.MarshaledEvent{Type: "Bumped", Data: []byte(fmt.Sprintf(`{"n":%d}`, n))},
		Metadata:    ember.Metadata{MetadataKeyCorrelationID: "corr-" + entityID},
		Timestamp:   time.Now().UTC(),
	}
}

type received struct {
	consumer    int
	entityID    string
	orderingKey string
	n           int
}

// keySharedReader
type keySharedReader struct {
	out chan received
	wg  sync.WaitGroup
}

func newKeySharedReader(t *testing.T, consumers []pulsar.Consumer) *keySharedReader {
	t.Helper()

	r := &keySharedReader{out: make(chan received, 1024)}
	for i, c := range consumers {
		r.wg.Add(1)
		go func(i int, c pulsar.Consumer) {
			defer r.wg.Done()
			for cm := range c.Chan() {
				var m message
				if err := json.Unmarshal(cm.Payload(), &m); err != nil {
					continue
				}
				var payload struct {
					N int `json:"n"`
				}
				if err := json.Unmarshal(m.Data, &payload); err != nil {
					continue
				}
				_ = c.Ack(cm.Message)
				r.out <- received{
					consumer:    i,
					entityID:    m.EntityID,
					orderingKey: cm.Key(),
					n:           payload.N,
				}
			}
		}(i, c)
	}
	return r
}

func (r *keySharedReader) collect(t *testing.T, want int, timeout time.Duration) []received {
	t.Helper()

	var got []received
	deadline := time.After(timeout)
	for len(got) < want {
		select {
		case m := <-r.out:
			got = append(got, m)
		case <-deadline:
			t.Fatalf("received %d of %d messages before the deadline", len(got), want)
		}
	}
	return got
}

func TestKeySharedDeliveryKeepsEachEntityOnOneConsumer(t *testing.T) {
	const (
		entities        = 16
		eventsPerEntity = 5
		consumers       = 2
	)

	client := connectTestPulsar(t)
	ctx := context.Background()

	unique := time.Now().UnixNano()
	topic := fmt.Sprintf("persistent://public/default/ember-keyshared-%d", unique)
	subscription := fmt.Sprintf("ember-keyshared-sub-%d", unique)

	cs := make([]pulsar.Consumer, consumers)
	for i := range cs {
		c, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            topic,
			SubscriptionName: subscription,
			Type:             pulsar.KeyShared,
		})
		require.NoError(t, err)
		t.Cleanup(c.Close)
		cs[i] = c
	}

	time.Sleep(2 * time.Second)

	registry := NewProducerRegistry(client, map[string]string{"Bumped": topic})
	t.Cleanup(func() { _ = registry.Close() })
	publisher := NewPublisher(registry, 0)

	reader := newKeySharedReader(t, cs)

	for e := range entities {
		entityID := fmt.Sprintf("entity-%02d", e)
		group := make([]ember.EventEnvelope, 0, eventsPerEntity)
		for n := range eventsPerEntity {
			group = append(group, seqEnvelope(entityID, n))
		}
		require.NoError(t, publisher.Publish(ctx, group))
	}

	got := reader.collect(t, entities*eventsPerEntity, 30*time.Second)

	owners := map[string]map[int]bool{}
	order := map[string][]int{}
	active := map[int]bool{}
	for _, m := range got {
		if owners[m.entityID] == nil {
			owners[m.entityID] = map[int]bool{}
		}
		owners[m.entityID][m.consumer] = true
		order[m.entityID] = append(order[m.entityID], m.n)
		active[m.consumer] = true
	}

	require.Len(t, owners, entities)
	require.Greater(t, len(active), 1,
		"every key landed on one consumer; the hash ranges never split, so this run proves nothing")

	for entityID, seen := range owners {
		require.Len(t, seen, 1, "entity %s was split across consumers %v", entityID, seen)

		want := make([]int, eventsPerEntity)
		for i := range want {
			want[i] = i
		}
		require.Equal(t, want, order[entityID], "entity %s arrived out of order", entityID)
	}
}

func TestKeySharedDeliveryFollowsTheOrderingKey(t *testing.T) {
	const (
		orderingKeys = 16
		eventsPerKey = 5
		consumers    = 2
	)

	client := connectTestPulsar(t)
	ctx := context.Background()

	unique := time.Now().UnixNano()
	topic := fmt.Sprintf("persistent://public/default/ember-ordering-key-%d", unique)
	subscription := fmt.Sprintf("ember-ordering-key-sub-%d", unique)

	cs := make([]pulsar.Consumer, consumers)
	for i := range cs {
		c, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            topic,
			SubscriptionName: subscription,
			Type:             pulsar.KeyShared,
		})
		require.NoError(t, err)
		t.Cleanup(c.Close)
		cs[i] = c
	}

	time.Sleep(2 * time.Second)

	registry := NewProducerRegistry(client, map[string]string{"Bumped": topic})
	t.Cleanup(func() { _ = registry.Close() })
	publisher := NewPublisher(registry, 0)

	reader := newKeySharedReader(t, cs)

	// Every event is its own entity, so only the ordering key can keep a key's events together.
	for n := range eventsPerKey {
		for k := range orderingKeys {
			orderingKey := fmt.Sprintf("chat-%02d", k)
			e := seqEnvelope(fmt.Sprintf("%s-message-%d", orderingKey, n), n)
			e.OrderingKey = orderingKey
			require.NoError(t, publisher.Publish(ctx, []ember.EventEnvelope{e}))
		}
	}

	got := reader.collect(t, orderingKeys*eventsPerKey, 30*time.Second)

	owners := map[string]map[int]bool{}
	order := map[string][]int{}
	active := map[int]bool{}
	for _, m := range got {
		if owners[m.orderingKey] == nil {
			owners[m.orderingKey] = map[int]bool{}
		}
		owners[m.orderingKey][m.consumer] = true
		order[m.orderingKey] = append(order[m.orderingKey], m.n)
		active[m.consumer] = true
	}

	require.Len(t, owners, orderingKeys)
	require.Greater(t, len(active), 1,
		"every key landed on one consumer; the hash ranges never split, so this run proves nothing")

	for orderingKey, seen := range owners {
		require.Len(t, seen, 1, "ordering key %s was split across consumers %v", orderingKey, seen)

		want := make([]int, eventsPerKey)
		for i := range want {
			want[i] = i
		}
		require.Equal(t, want, order[orderingKey], "ordering key %s arrived out of order", orderingKey)
	}
}

func TestKeySharedSurvivesConcurrentPublishers(t *testing.T) {
	const (
		entities        = 16
		eventsPerEntity = 5
		consumers       = 2
	)

	client := connectTestPulsar(t)
	ctx := context.Background()

	unique := time.Now().UnixNano()
	topic := fmt.Sprintf("persistent://public/default/ember-keyshared-concurrent-%d", unique)
	subscription := fmt.Sprintf("ember-keyshared-concurrent-sub-%d", unique)

	cs := make([]pulsar.Consumer, consumers)
	for i := range cs {
		c, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            topic,
			SubscriptionName: subscription,
			Type:             pulsar.KeyShared,
		})
		require.NoError(t, err)
		t.Cleanup(c.Close)
		cs[i] = c
	}

	time.Sleep(2 * time.Second)

	registry := NewProducerRegistry(client, map[string]string{"Bumped": topic})
	t.Cleanup(func() { _ = registry.Close() })
	publisher := NewPublisher(registry, 0)

	reader := newKeySharedReader(t, cs)

	var wg sync.WaitGroup
	for e := range entities {
		wg.Add(1)
		go func(e int) {
			defer wg.Done()
			entityID := fmt.Sprintf("entity-%02d", e)
			for n := range eventsPerEntity {
				if err := publisher.Publish(ctx, []ember.EventEnvelope{seqEnvelope(entityID, n)}); err != nil {
					t.Errorf("publish %s/%d: %v", entityID, n, err)
					return
				}
			}
		}(e)
	}
	wg.Wait()

	got := reader.collect(t, entities*eventsPerEntity, 30*time.Second)

	owners := map[string]map[int]bool{}
	active := map[int]bool{}
	for _, m := range got {
		if owners[m.entityID] == nil {
			owners[m.entityID] = map[int]bool{}
		}
		owners[m.entityID][m.consumer] = true
		active[m.consumer] = true
	}

	require.Len(t, owners, entities)
	require.Greater(t, len(active), 1,
		"all %d keys funnelled to one consumer: mixed-key batches are dispatched by the batch key", entities)

	for entityID, seen := range owners {
		require.Len(t, seen, 1, "entity %s was split across consumers %v", entityID, seen)
	}
}

// bumped is the event seqEnvelope publishes.
type bumped struct {
	N int `json:"n"`
}

func (bumped) Type() string     { return "Bumped" }
func (bumped) EntityID() string { return "" }

// blockingSubscription closes started when it begins handling, then waits for release.
type blockingSubscription struct {
	started chan struct{}
	release chan struct{}
}

func (blockingSubscription) Name() string { return "stop-drain" }

func (b blockingSubscription) Handle(context.Context, *ember.ReceivedEvent) error {
	close(b.started)
	<-b.release
	return nil
}

func TestStopAcksTheEventItWasHandling(t *testing.T) {
	client := connectTestPulsar(t)
	topic := fmt.Sprintf("persistent://public/default/ember-stop-%d", time.Now().UnixNano())
	consumers := NewConsumerRegistry(client, map[string][]pulsar.ConsumerOptions{
		"stop-drain": {{
			Topic:                       topic,
			SubscriptionName:            "stop-drain",
			Type:                        pulsar.Shared,
			SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		}},
	})
	sub := blockingSubscription{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	s := ember.NewSubscriber(emberjson.NewEventMarshaler(&bumped{}), NewSubscriber(consumers, ember.NopLogger), ember.NewSerialConsumer(), ember.NopLogger)
	require.NoError(t, s.Subscribe(context.Background(), sub))
	producers := NewProducerRegistry(client, map[string]string{"Bumped": topic})
	defer producers.Close()
	require.NoError(t, NewPublisher(producers, 0).Publish(context.Background(), []ember.EventEnvelope{seqEnvelope("e1", 1)}))

	select {
	case <-sub.started:
	case <-time.After(testPulsarTimeout):
		t.Fatal("the event never reached the handler")
	}
	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	time.Sleep(200 * time.Millisecond) // Stop is waiting on the handler
	close(sub.release)
	select {
	case <-stopped:
	case <-time.After(testPulsarTimeout):
		t.Fatal("Stop did not return")
	}

	again, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "stop-drain",
		Type:             pulsar.Shared,
	})
	require.NoError(t, err)
	defer again.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	msg, err := again.Receive(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the event came back: %v", msg)
}
