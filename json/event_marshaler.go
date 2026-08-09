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

// NewEventMarshaler registers events by prototype, given as either a value or a
// pointer. Unmarshal always returns a pointer.
func NewEventMarshaler(events ...ember.Event) *EventMarshaler {
	types := make(map[string]reflect.Type, len(events))
	for _, e := range events {
		typ := reflect.TypeOf(e)
		if typ == nil {
			continue
		}
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		types[e.Type()] = typ
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
