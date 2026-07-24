package ember

import "slices"

// Events is a transient buffer of the domain events an entity has produced. It
// is never serialized; the store drains it when the entity is persisted.
type Events []Event

func (e *Events) Emit(events ...Event) { *e = append(*e, events...) }
func (e *Events) All() []Event         { return slices.Clone(*e) }
func (e *Events) Clear()               { *e = nil }
