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

				idx := hashEntityID(envelope.EntityID) % uint32(numWorkers)
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

func hashEntityID(id string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(id))
	return h.Sum32()
}
