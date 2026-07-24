package ember

import (
	"context"
	"errors"
	"fmt"
)

var ErrUnregisteredEntity = errors.New("ember: no binding registered for entity type")

// EntitySaver persists any registered entity(ies) plus the events they produced,
// atomically. It holds no notifier: delivery is the outbox relay's job.
type EntitySaver struct {
	bindings map[string]binding
	events   *EventStore
	tx       Transactor
}

func NewEntitySaver(ev *EventStore, tx Transactor, bindings ...binder) *EntitySaver {
	m := make(map[string]binding, len(bindings))
	for _, b := range bindings {
		bd := b.binding()
		m[bd.typ] = bd
	}
	return &EntitySaver{bindings: m, events: ev, tx: tx}
}

func (s *EntitySaver) Save(ctx context.Context, entities ...Entity) error {
	if len(entities) == 0 {
		return nil
	}

	var events []Event
	for _, e := range entities {
		events = append(events, e.events().All()...)
	}

	type pending struct {
		entity  Entity
		version Version
	}
	var pend []pending

	work := func(ctx context.Context) error {
		pend = pend[:0]
		for _, e := range entities {
			v, err := s.persist(ctx, e)
			if err != nil {
				return err
			}
			pend = append(pend, pending{e, v})
		}
		if len(events) > 0 {
			return s.events.Save(ctx, events...)
		}
		return nil
	}

	var err error
	if len(entities) == 1 && len(events) == 0 {
		err = work(ctx)
	} else {
		err = s.tx.WithinTx(ctx, work)
	}
	if err != nil {
		return err
	}

	for _, p := range pend {
		p.entity.SetVersion(p.version)
		p.entity.events().Clear()
	}
	return nil
}

// persist marshals the snapshot at the next version and writes it without
// permanently mutating e; it returns the version to adopt once the write is durable.
func (s *EntitySaver) persist(ctx context.Context, e Entity) (Version, error) {
	b, ok := s.bindings[e.Type()]
	if !ok {
		return Version{}, fmt.Errorf("%w: %s", ErrUnregisteredEntity, e.Type())
	}
	prev := e.Version()
	next := prev.Inc()
	e.SetVersion(next)
	m, err := b.marshal(ctx, e)
	e.SetVersion(prev)
	if err != nil {
		return prev, err
	}
	if err := b.repo.Save(ctx, m); err != nil {
		return prev, err
	}
	return NewVersion(next.Value()), nil
}
