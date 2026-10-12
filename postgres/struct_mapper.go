package postgres

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/klemen-forstneric/ember"
)

// StructMapper copies matching exported fields between a domain entity and a
// separate SQL row. Use a pointer to its zero value; NewRepository validates and
// compiles the mapping at startup. It is safe to share across repositories.
//
// E must point to a struct directly embedding ember.EntityRoot. R is a row struct.
// Row fields match entity fields by Go name, with an optional entity:"FieldName"
// tag on the row to rename a mapping. db:"-" excludes a row field. Extra entity
// fields are left at their zero values on reconstruction.
//
// Scalar values, time.Time and matching nullable pointers are supported. Named
// scalar types may differ if their underlying kinds match; numeric width changes
// and pointer/value conversions are rejected. Pointers are copied, not shared.
// Use RowMapper directly for private fields, custom construction or conversions.
type StructMapper[E ember.Entity, R any] struct {
	once sync.Once
	plan *structMapping
	err  error
}

type structFieldMapping struct{ entity, row int }
type structMapping struct {
	entity reflect.Type
	root   int
	fields []structFieldMapping
}

// Validate prepares the immutable field mapping once. NewRepository calls this
// before accepting the mapper, so invalid mappings fail during service startup.
func (m *StructMapper[E, R]) Validate() error {
	m.once.Do(func() { m.plan, m.err = compileStructMapping(reflect.TypeFor[E](), reflect.TypeFor[R]()) })
	return m.err
}

func compileStructMapping(entity, row reflect.Type) (*structMapping, error) {
	if entity.Kind() != reflect.Pointer || entity.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("postgres: StructMapper entity must be a pointer to a struct")
	}
	if row.Kind() != reflect.Struct {
		return nil, fmt.Errorf("postgres: StructMapper row must be a struct")
	}
	entity = entity.Elem()
	root, ok := entity.FieldByName("EntityRoot")
	if !ok || !root.Anonymous || root.Type != reflect.TypeFor[ember.EntityRoot]() || len(root.Index) != 1 {
		return nil, fmt.Errorf("postgres: StructMapper entity must directly embed ember.EntityRoot")
	}
	plan := &structMapping{entity: entity, root: root.Index[0]}
	used := make(map[int]bool)
	for i := 0; i < row.NumField(); i++ {
		rf := row.Field(i)
		if rf.Tag.Get("db") == "-" {
			continue
		}
		name := rf.Tag.Get("entity")
		if name == "" {
			name = rf.Name
		}
		ef, ok := entity.FieldByName(name)
		if !rf.IsExported() || !ok || !ef.IsExported() || len(ef.Index) != 1 || ef.Index[0] == plan.root {
			return nil, fmt.Errorf("postgres: row field %s needs a matching exported entity field %q; use a custom RowMapper for other shapes", rf.Name, name)
		}
		if used[ef.Index[0]] {
			return nil, fmt.Errorf("postgres: multiple row fields map to entity field %s", name)
		}
		if !compatibleStructFields(ef.Type, rf.Type) {
			return nil, fmt.Errorf("postgres: cannot map entity field %s (%s) to row field %s (%s); use a custom RowMapper", name, ef.Type, rf.Name, rf.Type)
		}
		used[ef.Index[0]] = true
		plan.fields = append(plan.fields, structFieldMapping{entity: ef.Index[0], row: i})
	}
	return plan, nil
}

func compatibleStructFields(a, b reflect.Type) bool {
	if a.Kind() == reflect.Pointer && b.Kind() == reflect.Pointer {
		a, b = a.Elem(), b.Elem()
	}
	return scalarType(a) && scalarType(b) && a.Kind() == b.Kind() && a.ConvertibleTo(b) && b.ConvertibleTo(a)
}

func copyStructField(dst, src reflect.Value) {
	if dst.Kind() == reflect.Pointer {
		if src.IsNil() {
			dst.SetZero()
			return
		}
		dst.Set(reflect.New(dst.Type().Elem()))
		copyStructField(dst.Elem(), src.Elem())
		return
	}
	dst.Set(src.Convert(dst.Type()))
}

func (m *StructMapper[E, R]) Marshal(_ context.Context, e E) (R, error) {
	var zero R
	if err := m.Validate(); err != nil {
		return zero, err
	}
	if nilValue(e) {
		return zero, fmt.Errorf("postgres: cannot map a nil entity")
	}
	src := reflect.ValueOf(e).Elem()
	dst := reflect.New(reflect.TypeFor[R]()).Elem()
	for _, f := range m.plan.fields {
		copyStructField(dst.Field(f.row), src.Field(f.entity))
	}
	return dst.Interface().(R), nil
}

func (m *StructMapper[E, R]) Unmarshal(_ context.Context, row Row[R]) (E, error) {
	var zero E
	if err := m.Validate(); err != nil {
		return zero, err
	}
	dst := reflect.New(m.plan.entity)
	dst.Elem().Field(m.plan.root).Set(reflect.ValueOf(ember.NewEntityRoot(row.ID)))
	src := reflect.ValueOf(row.Data)
	for _, f := range m.plan.fields {
		copyStructField(dst.Elem().Field(f.entity), src.Field(f.row))
	}
	e := dst.Interface().(E)
	e.SetVersion(row.Version)
	return e, nil
}
