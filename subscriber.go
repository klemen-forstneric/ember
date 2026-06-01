package ember

import (
	"context"
	"errors"
	"time"
)

var (
	ErrUnknownEvent = errors.New("unknown event")
)

// HandleFunc
type HandleFunc func(context.Context, *ReceivedEvent) error

// SubscriptionMiddleware
type SubscriptionMiddleware func(next HandleFunc) HandleFunc

// SubscriptionMiddlewares
type SubscriptionMiddlewares []SubscriptionMiddleware

func (a SubscriptionMiddlewares) Apply(sub Subscription) HandleFunc {
	fn := sub.Handle
	for _, m := range a {
		fn = m(fn)
	}
	return fn
}

// Subscription
type Subscription interface {
	Name() string
	Handle(ctx context.Context, e *ReceivedEvent) error
}

// Transport
type Transport interface {
	Subscribe(ctx context.Context, name string) (<-chan AckableEventEnvelope, error)
	Stop()
}

// Subscriber
type Subscriber struct {
	marshaler EventMarshaler
	transport Transport
	consumer  Consumer
	logger    LoggerCtx
}

func NewSubscriber(m EventMarshaler, t Transport, c Consumer, l LoggerCtx) *Subscriber {
	return &Subscriber{
		marshaler: m,
		transport: t,
		consumer:  c,
		logger:    l,
	}
}

func (s *Subscriber) Subscribe(ctx context.Context, sub Subscription, m ...SubscriptionMiddleware) error {
	ch, err := s.transport.Subscribe(ctx, sub.Name())
	if err != nil {
		return err
	}

	handle := SubscriptionMiddlewares(m).Apply(sub)

	consume := func(ctx context.Context, envelope AckableEventEnvelope) {
		startTime := time.Now()

		event, err := s.marshaler.Unmarshal(ctx, envelope.Event)

		if err == ErrUnknownEvent {
			s.logger.Debug(ctx, "Event skipped", "error", err, "subscription", sub.Name())
			envelope.Ack()
			return
		} else if err != nil {
			s.logger.Error(ctx, "Failed to unmarshal the event", err)
			envelope.Ack()
			return
		}

		s.logger.Info(ctx, "Forwarding event to the handler", "event_id", envelope.ID,
			"type", envelope.Event.Type, "entity_id", envelope.EntityID, "handler", sub.Name())

		e := &ReceivedEvent{
			ID:        envelope.ID,
			Event:     event,
			Metadata:  envelope.Metadata,
			Timestamp: envelope.Timestamp,
		}

		err = handle(ctx, e)

		if err == nil {
			s.logger.Debug(ctx, "Event acked", "event_id", envelope.ID)
			envelope.Ack()
		} else {
			s.logger.Debug(ctx, "Event nacked", "event_id", envelope.ID, "error", err)
			envelope.Nack()
		}

		s.logger.Info(ctx, "Event processed", "event_id", envelope.ID,
			"type", envelope.Event.Type, "entity_id", envelope.EntityID, "handler", sub.Name(),
			"elapsed_time", time.Since(startTime), "error", err)
	}

	s.consumer.Run(ctx, sub.Name(), ch, consume)
	return nil
}

func (s *Subscriber) Stop() {
	s.transport.Stop()
	s.consumer.Stop()
}
