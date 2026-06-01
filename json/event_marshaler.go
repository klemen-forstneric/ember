package json

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/klemen-forstneric/ember"
)

// EventMarshaler
type EventMarshaler struct {
	types map[string]reflect.Type
}

func NewEventMarshaler(events ...ember.Event) *EventMarshaler {
	types := make(map[string]reflect.Type)
	for _, e := range events {
		types[e.Type()] = reflect.TypeOf(e).Elem()
	}

	return &EventMarshaler{types: types}
}

func (m *EventMarshaler) Marshal(_ context.Context, e ember.Event) (*ember.MarshaledEvent, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}

	return &ember.MarshaledEvent{
		Type: e.Type(),
		Data: data,
	}, nil
}

func (m *EventMarshaler) Unmarshal(_ context.Context, e *ember.MarshaledEvent) (ember.Event, error) {
	typ, ok := m.types[e.Type]
	if !ok {
		return nil, ember.ErrUnknownEvent
	}

	event := reflect.New(typ).Interface().(ember.Event)
	if err := json.Unmarshal(e.Data, event); err != nil {
		return nil, err
	}

	return event, nil
}
