package ember

import "context"

// Store is a unit of work: it persists an entity and the domain events it
// produced in one transaction. Delivery is the outbox relay's job — Store holds
// no notifier, so it cannot deliver mid-transaction.
type Store[E Entity] struct {
	entities *EntityStore[E]
	events   *EventStore
	tx       Transactor
}

func NewStore[E Entity](es *EntityStore[E], ev *EventStore, tx Transactor) *Store[E] {
	return &Store[E]{entities: es, events: ev, tx: tx}
}

func (s *Store[E]) Get(ctx context.Context, id string) (E, error) {
	return s.entities.Get(ctx, id)
}

func (s *Store[E]) List(ctx context.Context, f Filter, sort Sort) ([]E, error) {
	return s.entities.List(ctx, f, sort)
}

func (s *Store[E]) Save(ctx context.Context, e E) error {
	version := e.Version() // restore point: the DB rolls back on failure but the in-memory version bump does not
	err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		if err := s.entities.save(ctx, e); err != nil {
			return err
		}
		evs := e.events().All()
		if len(evs) == 0 {
			return nil
		}
		return s.events.Save(ctx, evs...)
	})
	if err != nil {
		e.SetVersion(version)
		return err
	}
	e.events().Clear()
	return nil
}
