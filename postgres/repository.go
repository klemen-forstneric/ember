package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/klemen-forstneric/ember"
)

var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
var timeType = reflect.TypeFor[time.Time]()

func quotedIdentifier(name string) (string, error) {
	if !identifier.MatchString(name) || len(name) > 63 {
		return "", fmt.Errorf("postgres: invalid identifier %q", name)
	}
	return `"` + name + `"`, nil
}

type mappedField struct {
	index    int
	column   string
	nullZero bool
}

// Repository stores a domain entity through a separate row representation and
// mapper. SQL generation, scanning and concurrency are shared; the domain type
// has no SQL mapping requirements. Tables and indexes are provisioned separately.
type Repository[E ember.Entity] struct {
	db        *DB
	table     string
	rowType   reflect.Type
	fields    []mappedField
	columns   []string
	paths     map[string]string
	nullable  map[string]bool
	marshal   func(context.Context, E) (reflect.Value, error)
	unmarshal func(context.Context, string, ember.Version, reflect.Value) (E, error)
}

// NewRepository validates R's mapping once at startup. R must be a struct with
// db:"column" tags, independently of E. Supported fields are scalar Go values
// (including named types), time.Time, and pointers to those types. Use
// db:"column,nullzero" to map a zero value to NULL, or db:"-" to omit a row field.
func NewRepository[E ember.Entity, R any](db *DB, table string, mapper RowMapper[E, R]) (*Repository[E], error) {
	name, err := quotedIdentifier(table)
	if err != nil {
		return nil, err
	}
	typ := reflect.TypeFor[R]()
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("postgres: row type must be a struct")
	}
	if nilValue(mapper) {
		return nil, fmt.Errorf("postgres: row mapper is nil")
	}
	if validator, ok := mapper.(interface{ Validate() error }); ok {
		if err := validator.Validate(); err != nil {
			return nil, err
		}
	}
	r := &Repository[E]{db: db, table: name, rowType: typ,
		nullable: make(map[string]bool), columns: []string{`"id"`, `"version"`},
		paths: map[string]string{"id": `"id"`, "version": `"version"`},
		marshal: func(ctx context.Context, e E) (reflect.Value, error) {
			row, err := mapper.Marshal(ctx, e)
			return reflect.ValueOf(row), err
		},
		unmarshal: func(ctx context.Context, id string, version ember.Version, data reflect.Value) (E, error) {
			return mapper.Unmarshal(ctx, Row[R]{ID: id, Version: version, Data: data.Interface().(R)})
		},
	}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := f.Tag.Get("db")
		if tag == "-" {
			continue
		}
		if !f.IsExported() || tag == "" {
			return nil, fmt.Errorf("postgres: row field %s needs a db column tag or db:\"-\"", f.Name)
		}
		parts := strings.Split(tag, ",")
		nullZero := len(parts) == 2 && parts[1] == "nullzero"
		if len(parts) > 1 && !nullZero {
			return nil, fmt.Errorf("postgres: invalid db tag %q", tag)
		}
		col, err := quotedIdentifier(parts[0])
		if err != nil {
			return nil, err
		}
		if _, exists := r.paths[parts[0]]; exists {
			return nil, fmt.Errorf("postgres: duplicate column %q", parts[0])
		}
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if !scalarType(ft) {
			return nil, fmt.Errorf("postgres: unsupported field %s (%s)", f.Name, f.Type)
		}
		r.fields = append(r.fields, mappedField{i, col, nullZero})
		r.columns = append(r.columns, col)
		r.paths[parts[0]] = col
		r.nullable[parts[0]] = nullZero || f.Type.Kind() == reflect.Pointer
	}
	return r, nil
}

func nilValue(value any) bool {
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return v.IsNil()
	}
	return false
}

func scalarType(t reflect.Type) bool {
	if t == timeType {
		return true
	}
	switch t.Kind() {
	case reflect.String, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

// nativeValue preserves SQL parameter types and normalizes timestamps without
// changing the entity. Postgres timestamps have microsecond precision.
func nativeValue(v reflect.Value) any {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if v.Type() == timeType {
		return v.Interface().(time.Time).UTC().Truncate(time.Microsecond)
	}
	return v.Interface()
}

func (r *Repository[E]) Save(ctx context.Context, e E, version ember.Version) error {
	if version.Initial() >= math.MaxInt64 || version.Value() != version.Initial()+1 {
		return fmt.Errorf("postgres: invalid version transition %d -> %d", version.Initial(), version.Value())
	}
	if nilValue(e) {
		return fmt.Errorf("postgres: cannot save a nil entity")
	}
	v, err := r.marshal(ctx, e)
	if err != nil {
		return fmt.Errorf("postgres: marshal row: %w", err)
	}
	args := []any{e.ID(), int64(version.Value())}
	for _, f := range r.fields {
		field := v.Field(f.index)
		if f.nullZero && field.IsZero() {
			args = append(args, nil)
		} else {
			args = append(args, nativeValue(field))
		}
	}
	var query string
	var params []any
	if version.Initial() == 0 {
		query, params, err = psql.Insert(r.table).Columns(r.columns...).Values(args...).Suffix(`ON CONFLICT ("id") DO NOTHING`).ToSql()
	} else {
		qb := psql.Update(r.table)
		for i, col := range r.columns[1:] {
			qb = qb.Set(col, args[i+1])
		}
		query, params, err = qb.Where(sq.Eq{`"id"`: e.ID(), `"version"`: int64(version.Initial())}).ToSql()
	}
	if err != nil {
		return err
	}
	result, err := r.db.Conn(ctx).ExecContext(ctx, query, params...)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ember.ErrVersionConflict
	}
	return nil
}

type rowScanner interface{ Scan(...any) error }

func (r *Repository[E]) scan(ctx context.Context, row rowScanner) (E, error) {
	var zero E
	v := reflect.New(r.rowType)
	var id string
	var version uint64
	args := []any{&id, &version}
	nulls := make(map[int]reflect.Value)
	for _, f := range r.fields {
		field := v.Elem().Field(f.index)
		if f.nullZero && field.Kind() != reflect.Pointer {
			target := reflect.New(reflect.PointerTo(field.Type()))
			nulls[f.index] = target
			args = append(args, target.Interface())
		} else {
			args = append(args, field.Addr().Interface())
		}
	}
	if err := row.Scan(args...); err == sql.ErrNoRows {
		return zero, ember.ErrEntityNotFound
	} else if err != nil {
		return zero, err
	}
	for index, target := range nulls {
		if !target.Elem().IsNil() {
			v.Elem().Field(index).Set(target.Elem().Elem())
		}
	}
	for _, f := range r.fields {
		field := v.Elem().Field(f.index)
		if field.Kind() == reflect.Pointer {
			if field.IsNil() {
				continue
			}
			field = field.Elem()
		}
		if field.Type() == timeType {
			field.Set(reflect.ValueOf(nativeValue(field)))
		}
	}
	persistedVersion := ember.NewVersion(version)
	e, err := r.unmarshal(ctx, id, persistedVersion, v.Elem())
	if err != nil {
		return zero, fmt.Errorf("postgres: unmarshal row: %w", err)
	}
	if nilValue(e) {
		return zero, fmt.Errorf("postgres: row mapper returned a nil entity")
	}
	if e.ID() != id {
		return zero, fmt.Errorf("postgres: row mapper changed identity %q to %q", id, e.ID())
	}
	e.SetVersion(persistedVersion)
	return e, nil
}

func (r *Repository[E]) Get(ctx context.Context, id string) (E, error) {
	query, args, err := psql.Select(r.columns...).From(r.table).Where(sq.Eq{`"id"`: id}).ToSql()
	if err != nil {
		var zero E
		return zero, err
	}
	return r.scan(ctx, r.db.Conn(ctx).QueryRowContext(ctx, query, args...))
}

func (r *Repository[E]) List(ctx context.Context, filter ember.Filter, sort ember.Sort) ([]E, error) {
	return r.ListPage(ctx, filter, sort, ember.Unpaged())
}

func (r *Repository[E]) ListPage(ctx context.Context, filter ember.Filter, sort ember.Sort, page ember.Page) ([]E, error) {
	query, args, err := r.listQuery(filter, sort, page)
	if err != nil {
		return nil, err
	}
	rows, err := r.db.Conn(ctx).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []E
	for rows.Next() {
		e, err := r.scan(ctx, rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
