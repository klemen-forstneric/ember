package dynamo

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/klemen-forstneric/ember"
)

// build is a test helper: it translates a non-nil filter and renders it as a
// standalone FilterExpression, returning the expression string, names, values.
func build(t *testing.T, f ember.Filter) (string, map[string]string, map[string]types.AttributeValue) {
	t.Helper()
	cb, ok, err := buildFilter(f)
	if err != nil {
		t.Fatalf("buildFilter: %v", err)
	}
	if !ok {
		t.Fatalf("buildFilter: expected a filter, got none")
	}
	expr, err := expression.NewBuilder().WithFilter(cb).Build()
	if err != nil {
		t.Fatalf("build expression: %v", err)
	}
	return aws.ToString(expr.Filter()), expr.Names(), expr.Values()
}

func nN(s string) types.AttributeValue { return &types.AttributeValueMemberN{Value: s} }
func sS(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }

func TestBuildFilterNilMatchesAll(t *testing.T) {
	cb, ok, err := buildFilter(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Errorf("nil filter should report no filter, got %#v", cb)
	}
}

func TestBuildFilterComparison(t *testing.T) {
	tests := []struct {
		name      string
		filter    ember.Filter
		wantExpr  string
		wantNames map[string]string
		wantVals  map[string]types.AttributeValue
	}{
		{
			"eq data path",
			ember.Eq("status", "open"),
			"#0.#1 = :0",
			map[string]string{"#0": "data", "#1": "status"},
			map[string]types.AttributeValue{":0": sS("open")},
		},
		{
			"nested data path",
			ember.Eq("address.city", "NYC"),
			"#0.#1.#2 = :0",
			map[string]string{"#0": "data", "#1": "address", "#2": "city"},
			map[string]types.AttributeValue{":0": sS("NYC")},
		},
		{
			"reserved version gt",
			ember.Gt("version", 5),
			"#0 > :0",
			map[string]string{"#0": "version"},
			map[string]types.AttributeValue{":0": nN("5")},
		},
		{
			"gte",
			ember.Gte("total", 100),
			"#0.#1 >= :0",
			map[string]string{"#0": "data", "#1": "total"},
			map[string]types.AttributeValue{":0": nN("100")},
		},
		{
			"lt",
			ember.Lt("total", 100),
			"#0.#1 < :0",
			map[string]string{"#0": "data", "#1": "total"},
			map[string]types.AttributeValue{":0": nN("100")},
		},
		{
			"lte",
			ember.Lte("total", 100),
			"#0.#1 <= :0",
			map[string]string{"#0": "data", "#1": "total"},
			map[string]types.AttributeValue{":0": nN("100")},
		},
		{
			"time normalized",
			ember.Eq("createdAt", time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)),
			"#0.#1 = :0",
			map[string]string{"#0": "data", "#1": "createdAt"},
			map[string]types.AttributeValue{":0": sS("2024-01-02T03:04:05Z")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotExpr, gotNames, gotVals := build(t, tt.filter)
			if gotExpr != tt.wantExpr {
				t.Errorf("expr: got %q, want %q", gotExpr, tt.wantExpr)
			}
			if !reflect.DeepEqual(gotNames, tt.wantNames) {
				t.Errorf("names: got %#v, want %#v", gotNames, tt.wantNames)
			}
			if !reflect.DeepEqual(gotVals, tt.wantVals) {
				t.Errorf("values: got %#v, want %#v", gotVals, tt.wantVals)
			}
		})
	}
}

func TestBuildFilterMembership(t *testing.T) {
	gotExpr, gotNames, gotVals := build(t, ember.In("region", "EU", "UK"))
	if gotExpr != "#0.#1 IN (:0, :1)" {
		t.Errorf("expr: got %q", gotExpr)
	}
	if !reflect.DeepEqual(gotNames, map[string]string{"#0": "data", "#1": "region"}) {
		t.Errorf("names: got %#v", gotNames)
	}
	if !reflect.DeepEqual(gotVals, map[string]types.AttributeValue{":0": sS("EU"), ":1": sS("UK")}) {
		t.Errorf("values: got %#v", gotVals)
	}
}

func TestBuildFilterEmptyMembershipMatchesNone(t *testing.T) {
	// Empty IN is always-false: attribute_not_exists(version).
	gotExpr, gotNames, _ := build(t, ember.In("region"))
	if gotExpr != "attribute_not_exists (#0)" {
		t.Errorf("expr: got %q", gotExpr)
	}
	if !reflect.DeepEqual(gotNames, map[string]string{"#0": "version"}) {
		t.Errorf("names: got %#v", gotNames)
	}
}

func TestBuildFilterNeGuarded(t *testing.T) {
	// Ne on a data path is the two-valued complement: present, non-null, and !=.
	gotExpr, gotNames, gotVals := build(t, ember.Ne("status", "open"))
	want := "(attribute_exists (#0.#1)) AND (NOT (attribute_type (#0.#1, :0))) AND (#0.#1 <> :1)"
	if gotExpr != want {
		t.Errorf("expr: got %q, want %q", gotExpr, want)
	}
	if !reflect.DeepEqual(gotNames, map[string]string{"#0": "data", "#1": "status"}) {
		t.Errorf("names: got %#v", gotNames)
	}
	if !reflect.DeepEqual(gotVals, map[string]types.AttributeValue{":0": sS("NULL"), ":1": sS("open")}) {
		t.Errorf("values: got %#v", gotVals)
	}
}

func TestBuildFilterNeReservedVersionUnguarded(t *testing.T) {
	gotExpr, gotNames, gotVals := build(t, ember.Ne("version", 3))
	if gotExpr != "#0 <> :0" {
		t.Errorf("expr: got %q", gotExpr)
	}
	if !reflect.DeepEqual(gotNames, map[string]string{"#0": "version"}) {
		t.Errorf("names: got %#v", gotNames)
	}
	if !reflect.DeepEqual(gotVals, map[string]types.AttributeValue{":0": nN("3")}) {
		t.Errorf("values: got %#v", gotVals)
	}
}

func TestBuildFilterExistence(t *testing.T) {
	t.Run("true on data path", func(t *testing.T) {
		gotExpr, gotNames, gotVals := build(t, ember.Exists("status", true))
		want := "(attribute_exists (#0.#1)) AND (NOT (attribute_type (#0.#1, :0)))"
		if gotExpr != want {
			t.Errorf("expr: got %q, want %q", gotExpr, want)
		}
		if !reflect.DeepEqual(gotNames, map[string]string{"#0": "data", "#1": "status"}) {
			t.Errorf("names: got %#v", gotNames)
		}
		if !reflect.DeepEqual(gotVals, map[string]types.AttributeValue{":0": sS("NULL")}) {
			t.Errorf("values: got %#v", gotVals)
		}
	})
	t.Run("false on data path", func(t *testing.T) {
		gotExpr, _, gotVals := build(t, ember.Exists("status", false))
		want := "(attribute_not_exists (#0.#1)) OR (attribute_type (#0.#1, :0))"
		if gotExpr != want {
			t.Errorf("expr: got %q, want %q", gotExpr, want)
		}
		if !reflect.DeepEqual(gotVals, map[string]types.AttributeValue{":0": sS("NULL")}) {
			t.Errorf("values: got %#v", gotVals)
		}
	})
}

func TestBuildFilterBoolean(t *testing.T) {
	t.Run("and", func(t *testing.T) {
		gotExpr, _, _ := build(t, ember.And(ember.Eq("a", "1"), ember.Eq("b", "2")))
		if gotExpr != "(#0.#1 = :0) AND (#0.#2 = :1)" {
			t.Errorf("expr: got %q", gotExpr)
		}
	})
	t.Run("or", func(t *testing.T) {
		gotExpr, _, _ := build(t, ember.Or(ember.Eq("a", "1"), ember.Eq("b", "2")))
		if gotExpr != "(#0.#1 = :0) OR (#0.#2 = :1)" {
			t.Errorf("expr: got %q", gotExpr)
		}
	})
	t.Run("not", func(t *testing.T) {
		gotExpr, _, _ := build(t, ember.Not(ember.Eq("status", "open")))
		if gotExpr != "NOT (#0.#1 = :0)" {
			t.Errorf("expr: got %q", gotExpr)
		}
	})
	t.Run("single-child and collapses", func(t *testing.T) {
		gotExpr, _, _ := build(t, ember.And(ember.Eq("a", "1")))
		if gotExpr != "#0.#1 = :0" {
			t.Errorf("expr: got %q", gotExpr)
		}
	})
	t.Run("empty and matches all", func(t *testing.T) {
		gotExpr, gotNames, _ := build(t, ember.And())
		if gotExpr != "attribute_exists (#0)" || !reflect.DeepEqual(gotNames, map[string]string{"#0": "version"}) {
			t.Errorf("expr: got %q names %#v", gotExpr, gotNames)
		}
	})
	t.Run("empty or matches none", func(t *testing.T) {
		gotExpr, gotNames, _ := build(t, ember.Or())
		if gotExpr != "attribute_not_exists (#0)" || !reflect.DeepEqual(gotNames, map[string]string{"#0": "version"}) {
			t.Errorf("expr: got %q names %#v", gotExpr, gotNames)
		}
	})
}

func TestBuildFilterUnsupportedValue(t *testing.T) {
	_, _, err := buildFilter(ember.Eq("status", []string{"nope"}))
	if !errors.Is(err, ember.ErrUnsupportedFilter) {
		t.Errorf("got %v, want ErrUnsupportedFilter", err)
	}
}

func TestBuildFilterRejectsKeyAttribute(t *testing.T) {
	for _, path := range []string{"id", "type"} {
		_, _, err := buildFilter(ember.Eq(path, "x"))
		if !errors.Is(err, ember.ErrUnsupportedFilter) {
			t.Errorf("path %q: got %v, want ErrUnsupportedFilter", path, err)
		}
	}
}

// Compile-time assertion that the repository satisfies the interface.
var _ ember.EntityRepository = (*EntityRepository)(nil)
