package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/klemen-forstneric/ember"
	emberjson "github.com/klemen-forstneric/ember/json"
)

// counter
type counter struct {
	ember.EntityRoot
	N int
}

func newCounter(id string) *counter {
	return &counter{EntityRoot: ember.NewEntityRoot(id)}
}

func (c *counter) Type() string { return "counter" }

func (c *counter) bump() {
	c.N++
	c.Emit(
		&bumped{Entity: c.ID(), N: c.N, Half: "first"},
		&bumped{Entity: c.ID(), N: c.N, Half: "second"},
	)
}

// bumped
type bumped struct {
	Entity string `json:"entity"`
	N      int    `json:"n"`
	Half   string `json:"half"`
}

func (b *bumped) EntityID() string { return b.Entity }
func (b *bumped) Type() string     { return "bumped" }

type counterMarshaler struct{}

func (counterMarshaler) Marshal(_ context.Context, c *counter) (*ember.MarshaledEntity, error) {
	data, err := json.Marshal(struct {
		N int `json:"n"`
	}{N: c.N})
	if err != nil {
		return nil, err
	}
	return &ember.MarshaledEntity{ID: c.ID(), Type: c.Type(), Version: c.Version(), Data: data}, nil
}

func (counterMarshaler) Unmarshal(_ context.Context, m *ember.MarshaledEntity) (*counter, error) {
	var dto struct {
		N int `json:"n"`
	}
	if err := json.Unmarshal(m.Data, &dto); err != nil {
		return nil, err
	}
	c := newCounter(m.ID)
	c.SetVersion(m.Version)
	c.N = dto.N
	return c, nil
}

type seqIDer struct {
	mu sync.Mutex
	n  int
}

func (i *seqIDer) ID() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.n++
	return "evt-" + strconv.Itoa(i.n)
}

type recordingSink struct {
	mu        sync.Mutex
	published []ember.EventEnvelope
}

func (s *recordingSink) Publish(_ context.Context, envelopes []ember.EventEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published = append(s.published, envelopes...)
	return nil
}

func (s *recordingSink) snapshot() []ember.EventEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ember.EventEnvelope(nil), s.published...)
}

type openLocker struct{}

func (openLocker) TryLock(context.Context, string) (ember.Lock, error) { return openLock{}, nil }

type openLock struct{}

func (openLock) Release(context.Context) error { return nil }

func testCollections(t *testing.T) (entities, outbox *mongo.Collection) {
	t.Helper()

	if testing.Short() {
		t.Skip("mongo integration test skipped by -short")
	}

	client, err := dialTestMongo()
	if err != nil {
		t.Skipf("mongo unavailable at %s: %v", testMongoURI(), err)
	}

	db := client.Database("ember_test")
	entities = db.Collection(fmt.Sprintf("e2e_entities_%s", t.Name()))
	outbox = db.Collection(fmt.Sprintf("e2e_outbox_%s", t.Name()))
	t.Cleanup(func() {
		_ = entities.Drop(context.Background())
		_ = outbox.Drop(context.Background())
	})
	return entities, outbox
}

func TestSaveThroughRelayPreservesPerEntityOrder(t *testing.T) {
	entitiesCol, outboxCol := testCollections(t)
	ctx := context.Background()

	require.NoError(t, EnsureOutbox(ctx, outboxCol))
	entityRepo, err := NewEntityRepository(ctx, entitiesCol)
	require.NoError(t, err)
	eventRepo := NewEventRepository(outboxCol)

	client, err := dialTestMongo()
	require.NoError(t, err)

	publisher := ember.NewPublisher(
		&seqIDer{},
		ember.NopMetadataGetter{},
		emberjson.NewEventMarshaler(&bumped{}),
		ember.AtLeastOnce(eventRepo),
	)
	saver := ember.NewEntitySaver(publisher, NewTransactor(client), nil,
		ember.Bind[*counter](entityRepo, counterMarshaler{}))

	a, b := newCounter("a"), newCounter("b")
	for range 5 {
		a.bump()
		require.NoError(t, saver.Save(ctx, a))
		b.bump()
		require.NoError(t, saver.Save(ctx, b))
		a.bump()
		require.NoError(t, saver.Save(ctx, a))
	}

	sink := &recordingSink{}
	cfg := ember.DefaultPollingRelayConfig("e2e:outbox:relay")
	cfg.IdleInterval = 10 * time.Millisecond
	relay, err := ember.NewPollingRelay(eventRepo, sink, openLocker{}, ember.NopLogger, cfg)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { relay.Run(runCtx); close(done) }()

	const want = 30 // (10 saves on a + 5 on b) x 2 events
	require.Eventually(t, func() bool { return len(sink.snapshot()) >= want },
		10*time.Second, 20*time.Millisecond, "relay did not drain the outbox")

	cancel()
	require.NoError(t, relay.Close())
	<-done

	got := sink.snapshot()
	require.Len(t, got, want, "no duplicates and nothing dropped")

	perEntity := map[string][][2]int{}
	for _, e := range got {
		perEntity[e.EntityID] = append(perEntity[e.EntityID], [2]int{int(e.Version), e.Index})
	}
	require.Len(t, perEntity, 2)

	for _, entity := range []string{"a", "b"} {
		keys := perEntity[entity]
		for i := 1; i < len(keys); i++ {
			require.Truef(t, keys[i-1][0] < keys[i][0] || (keys[i-1][0] == keys[i][0] && keys[i-1][1] < keys[i][1]),
				"entity %s published out of order at position %d: %v then %v", entity, i, keys[i-1], keys[i])
		}
		require.Equal(t, [2]int{1, 0}, keys[0], "entity %s must start at its first version", entity)
	}

	require.Len(t, perEntity["a"], 20)
	require.Len(t, perEntity["b"], 10)

	var unpublished int64
	unpublished, err = outboxCol.CountDocuments(ctx, map[string]any{"published": false})
	require.NoError(t, err)
	require.Zero(t, unpublished, "relay must mark everything it published")
}
