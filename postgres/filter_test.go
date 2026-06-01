package postgres

import (
	"errors"
	"reflect"
	"testing"

	"github.com/klemen-forstneric/ember"
)

func TestBuildWhere(t *testing.T) {
	tests := []struct {
		name     string
		filter   ember.Filter
		wantExpr string
		wantArgs []any
	}{
		{"nil matches all", nil, "", nil},
		{"eq string data path", ember.Eq("status", "open"), "data#>>'{status}' = $1", []any{"open"}},
		{"nested data path", ember.Eq("address.city", "NYC"), "data#>>'{address,city}' = $1", []any{"NYC"}},
		{"reserved id", ember.Eq("id", "x"), "id = $1", []any{"x"}},
		{"reserved version", ember.Gt("version", 5), "version > $1", []any{5}},
		{"gt numeric cast", ember.Gt("total", 4200), "(data#>>'{total}')::numeric > $1", []any{4200}},
		{"ne", ember.Ne("status", "open"), "data#>>'{status}' <> $1", []any{"open"}},
		{"in", ember.In("region", "EU", "UK"), "data#>>'{region}' IN ($1, $2)", []any{"EU", "UK"}},
		{"in empty is false", ember.In("region"), "FALSE", nil},
		{"exists true", ember.Exists("status", true), "data#>>'{status}' IS NOT NULL", nil},
		{"exists false", ember.Exists("status", false), "data#>>'{status}' IS NULL", nil},
		{"not", ember.Not(ember.Eq("status", "open")), "NOT (data#>>'{status}' = $1)", []any{"open"}},
		{
			"and",
			ember.And(ember.Eq("status", "open"), ember.Gt("total", 100)),
			"(data#>>'{status}' = $1) AND ((data#>>'{total}')::numeric > $2)",
			[]any{"open", 100},
		},
		{
			"or",
			ember.Or(ember.Eq("a", "1"), ember.Eq("b", "2")),
			"(data#>>'{a}' = $1) OR (data#>>'{b}' = $2)",
			[]any{"1", "2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, args, err := buildWhere(tt.filter)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if expr != tt.wantExpr {
				t.Errorf("expr = %q, want %q", expr, tt.wantExpr)
			}
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("args = %#v, want %#v", args, tt.wantArgs)
			}
		})
	}
}

func TestBuildWhereUnsupportedValue(t *testing.T) {
	_, _, err := buildWhere(ember.Eq("status", []string{"nope"}))
	if !errors.Is(err, ember.ErrUnsupportedFilter) {
		t.Errorf("got %v, want ErrUnsupportedFilter", err)
	}
}
