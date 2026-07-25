package ember

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrUnregisteredEntity = errors.New("ember: no binding registered for entity type")
)

// EntitySaver
type EntitySaver struct {
	bindings  map[string]binding
	publisher *Publisher
	tx        Transactor
}

func NewEntitySaver(p *Publisher, tx Transactor, bindings ...binder) *EntitySaver {
	m := make(map[string]binding, len(bindings))
	for _, b := range bindings {
		bd := b.binding()
		m[bd.typ] = bd
	}
	return &EntitySaver{bindings: m, publisher: p, tx: tx}
}

// Save returning an error wrapping ErrDeliveryFailed means the write
// committed and only delivery failed — do not retry, or the entity is saved twice.
func (s *EntitySaver) Save(ctx context.Context, es ...Entity) error {
	if len(es) == 0 {
		return nil
	}

	var events []Event
	for _, e := range es {
		events = append(events, e.events().All()...)
	}

	type entities struct {
		e Entity
		v Version
	}

	var (
		saved   []entities
		deliver delivery
	)

	fn := func(ctx context.Context) error {
		saved = nil
		deliver = nil

		for _, e := range es {
			v, err := s.save(ctx, e)
			if err != nil {
				return err
			}

			saved = append(saved, entities{e: e, v: v})
		}

		var err error
		deliver, err = s.publisher.stage(ctx, events...)
		return err
	}

	var err error
	if len(es) == 1 && len(events) == 0 {
		err = fn(ctx)
	} else {
		err = s.tx.WithinTx(ctx, fn)
	}

	if err != nil {
		return err
	}

	for _, p := range saved {
		p.e.SetVersion(p.v)
		p.e.events().Clear()
	}

	if deliver == nil {
		return nil
	}
	if err := deliver(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("%w: %w", ErrDeliveryFailed, err)
	}
	return nil
}

func (s *EntitySaver) save(ctx context.Context, e Entity) (Version, error) {
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
