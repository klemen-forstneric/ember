package ember

import (
	"context"
	"hash/fnv"
	"sync"
)

// ConsumeFunc
type ConsumeFunc func(context.Context, AckableEventEnvelope)

// ConsumeMiddleware
type ConsumeMiddleware func(next ConsumeFunc) ConsumeFunc

// ConsumeMiddlewares
type ConsumeMiddlewares []ConsumeMiddleware

func (a ConsumeMiddlewares) Apply(c ConsumeFunc) ConsumeFunc {
	fn := c
	for _, m := range a {
		fn = m(fn)
	}
	return fn
}

// Consumer
type Consumer interface {
	Run(ctx context.Context, name string, ch <-chan AckableEventEnvelope, consume ConsumeFunc)
	Stop()
}

// MiddlewareConsumer
type MiddlewareConsumer struct {
	inner       Consumer
	middlewares ConsumeMiddlewares
}

func NewMiddlewareConsumer(c Consumer, m ...ConsumeMiddleware) *MiddlewareConsumer {
	return &MiddlewareConsumer{inner: c, middlewares: m}
}

func (c *MiddlewareConsumer) Run(ctx context.Context, name string, ch <-chan AckableEventEnvelope, consume ConsumeFunc) {
	c.inner.Run(ctx, name, ch, c.middlewares.Apply(consume))
}

func (c *MiddlewareConsumer) Stop() {
	c.inner.Stop()
}

// SerialConsumer
type SerialConsumer struct {
	shutdown chan struct{}
	once     sync.Once
	wg       sync.WaitGroup
}

func NewSerialConsumer() *SerialConsumer {
	return &SerialConsumer{
		shutdown: make(chan struct{}),
	}
}

func (c *SerialConsumer) Run(ctx context.Context, _ string, ch <-chan AckableEventEnvelope, consume ConsumeFunc) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case envelope, ok := <-ch:
				if !ok {
					return
				}
				consume(ctx, envelope)
			case <-ctx.Done():
				return
			case <-c.shutdown:
				return
			}
		}
	}()
}

func (c *SerialConsumer) Stop() {
	c.once.Do(func() {
		close(c.shutdown)
	})
	c.wg.Wait()
}

// StickyEntityConsumer
type StickyEntityConsumer struct {
	concurrency map[string]int
	logger      LoggerCtx

	shutdown chan struct{}
	once     sync.Once
	wg       sync.WaitGroup
}

func NewStickyEntityConsumer(concurrency map[string]int, l LoggerCtx) *StickyEntityConsumer {
	return &StickyEntityConsumer{
		concurrency: concurrency,
		logger:      l,
		shutdown:    make(chan struct{}),
	}
}

func (c *StickyEntityConsumer) Run(ctx context.Context, name string, ch <-chan AckableEventEnvelope, consume ConsumeFunc) {
	numWorkers := c.concurrency[name]
	if numWorkers < 1 {
		numWorkers = 1
	}

	workerCh := make([]chan AckableEventEnvelope, numWorkers)
	for i := range workerCh {
		workerCh[i] = make(chan AckableEventEnvelope)
	}

	for i := range workerCh {
		c.wg.Add(1)
		go func(in <-chan AckableEventEnvelope) {
			defer c.wg.Done()
			for envelope := range in {
				consume(ctx, envelope)
			}
		}(workerCh[i])
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer func() {
			for _, wch := range workerCh {
				close(wch)
			}
		}()

		for {
			select {
			case envelope, ok := <-ch:
				if !ok {
					c.logger.Info(ctx, "Subscription channel closed, stopping router", "subscription", name)
					return
				}

				idx := hashEntityID(envelope.OrderingKey) % uint32(numWorkers)
				select {
				case workerCh[idx] <- envelope:
				case <-ctx.Done():
					return
				case <-c.shutdown:
					return
				}
			case <-ctx.Done():
				c.logger.Info(ctx, "Context done, stopping router", "subscription", name)
				return
			case <-c.shutdown:
				c.logger.Info(ctx, "Shutdown triggered, stopping router", "subscription", name)
				return
			}
		}
	}()
}

func (c *StickyEntityConsumer) Stop() {
	c.once.Do(func() {
		close(c.shutdown)
	})
	c.wg.Wait()
}

// ConflatingConsumer handles at most one event per ordering key at a time. Of the events that
// arrive meanwhile it keeps only the newest and acks the rest unhandled, so use it only
// when the handler acts on current state and handling the newest event covers the others.
type ConflatingConsumer struct {
	limit  int
	logger LoggerCtx

	shutdown chan struct{}
	once     sync.Once
	wg       sync.WaitGroup
}

// NewConflatingConsumer handles up to limit ordering keys at once per subscription.
func NewConflatingConsumer(limit int, l LoggerCtx) *ConflatingConsumer {
	return &ConflatingConsumer{
		limit:    max(limit, 1),
		logger:   l,
		shutdown: make(chan struct{}),
	}
}

func (c *ConflatingConsumer) Run(ctx context.Context, name string, ch <-chan AckableEventEnvelope, consume ConsumeFunc) {
	r := &conflation{
		consumer: c,
		ctx:      ctx,
		name:     name,
		consume:  consume,
		slots:    make(chan struct{}, c.limit),
		waiting:  map[string]*AckableEventEnvelope{},
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case envelope, ok := <-ch:
				if !ok {
					c.logger.Info(ctx, "Subscription channel closed, stopping router", "subscription", name)
					return
				}
				if !r.dispatch(envelope) {
					return
				}
			case <-ctx.Done():
				c.logger.Info(ctx, "Context done, stopping router", "subscription", name)
				return
			case <-c.shutdown:
				c.logger.Info(ctx, "Shutdown triggered, stopping router", "subscription", name)
				return
			}
		}
	}()
}

func (c *ConflatingConsumer) Stop() {
	c.once.Do(func() {
		close(c.shutdown)
	})
	c.wg.Wait()
}

// conflation is one subscription's state: the busy ordering keys and the newest event waiting for each.
type conflation struct {
	consumer *ConflatingConsumer
	ctx      context.Context
	name     string
	consume  ConsumeFunc
	slots    chan struct{}

	mu      sync.Mutex
	waiting map[string]*AckableEventEnvelope // busy ordering key → newest waiting event, nil when none
}

// dispatch starts e, or parks it behind its busy ordering key. It returns false once stopped.
func (r *conflation) dispatch(e AckableEventEnvelope) bool {
	if e.OrderingKey != "" {
		r.mu.Lock()
		prev, busy := r.waiting[e.OrderingKey]
		if busy {
			r.waiting[e.OrderingKey] = &e
			r.mu.Unlock()
			if prev != nil {
				r.consumer.logger.Debug(r.ctx, "Event superseded", "event_id", prev.ID, "by", e.ID, "subscription", r.name)
				prev.Ack()
			}
			return true
		}
		r.waiting[e.OrderingKey] = nil
		r.mu.Unlock()
	}

	select {
	case r.slots <- struct{}{}:
	case <-r.ctx.Done():
		return false
	case <-r.consumer.shutdown:
		return false
	}

	r.consumer.wg.Add(1)
	go r.run(e)
	return true
}

// run handles e, then whatever event is waiting for its ordering key, until none is.
func (r *conflation) run(e AckableEventEnvelope) {
	defer r.consumer.wg.Done()
	defer func() { <-r.slots }()

	for {
		r.consume(r.ctx, e)
		if e.OrderingKey == "" {
			return
		}

		r.mu.Lock()
		next := r.waiting[e.OrderingKey]
		if next == nil || r.stopped() {
			// A waiting event left behind on shutdown stays unacked, so the broker redelivers it.
			delete(r.waiting, e.OrderingKey)
			r.mu.Unlock()
			return
		}
		r.waiting[e.OrderingKey] = nil
		r.mu.Unlock()
		e = *next
	}
}

func (r *conflation) stopped() bool {
	select {
	case <-r.ctx.Done():
		return true
	case <-r.consumer.shutdown:
		return true
	default:
		return false
	}
}

func hashEntityID(id string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(id))
	return h.Sum32()
}
