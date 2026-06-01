package json

import (
	"context"
	"encoding/json"

	"github.com/klemen-forstneric/ember"
)

// EntityMarshaler
type EntityMarshaler[E ember.Entity] struct {
	entityFn func(id string) E
}

func NewEntityMarshaler[E ember.Entity](entityFn func(id string) E) *EntityMarshaler[E] {
	return &EntityMarshaler[E]{entityFn: entityFn}
}

func (m *EntityMarshaler[E]) Marshal(_ context.Context, e E) (*ember.MarshaledEntity, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}

	return &ember.MarshaledEntity{
		ID:      e.ID(),
		Type:    e.Type(),
		Version: e.Version(),
		Data:    data,
	}, nil
}

func (m *EntityMarshaler[E]) Unmarshal(_ context.Context, me *ember.MarshaledEntity) (E, error) {
	var empty E

	e := m.entityFn(me.ID)
	if err := json.Unmarshal(me.Data, e); err != nil {
		return empty, err
	}

	e.SetVersion(me.Version)
	return e, nil
}
