package dynamo

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
)

// build is a test helper: it translates a non-nil filter and renders it as a
// standalone FilterExpression, returning the expression string, names, values.
func build(t *testing.T, f ember.Filter) (string, map[string]string, map[string]types.AttributeValue) {
	t.Helper()
	cb, ok, err := buildFilter(f)
	require.NoError(t, err, "buildFilter")
	require.True(t, ok, "buildFilter: expected a filter, got none")
	expr, err := expression.NewBuilder().WithFilter(cb).Build()
	require.NoError(t, err, "build expression")
	return aws.ToString(expr.Filter()), expr.Names(), expr.Values()
}

func nN(s string) types.AttributeValue { return &types.AttributeValueMemberN{Value: s} }
func sS(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }

func TestBuildFilterNilMatchesAll(t *testing.T) {
	cb, ok, err := buildFilter(nil)
	require.NoError(t, err)
	assert.False(t, ok, "nil filter should report no filter, got %#v", cb)
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
			assert.Equal(t, tt.wantExpr, gotExpr, "expr")
			assert.Equal(t, tt.wantNames, gotNames, "names")
			assert.Equal(t, tt.wantVals, gotVals, "values")
		})
	}
}

func TestBuildFilterMembership(t *testing.T) {
	gotExpr, gotNames, gotVals := build(t, ember.In("region", "EU", "UK"))
	assert.Equal(t, "#0.#1 IN (:0, :1)", gotExpr)
	assert.Equal(t, map[string]string{"#0": "data", "#1": "region"}, gotNames)
	assert.Equal(t, map[string]types.AttributeValue{":0": sS("EU"), ":1": sS("UK")}, gotVals)
}

func TestBuildFilterEmptyMembershipMatchesNone(t *testing.T) {
	// Empty IN is always-false: attribute_not_exists(version).
	gotExpr, gotNames, _ := build(t, ember.In("region"))
	assert.Equal(t, "attribute_not_exists (#0)", gotExpr)
	assert.Equal(t, map[string]string{"#0": "version"}, gotNames)
}

func TestBuildFilterNeGuarded(t *testing.T) {
	// Ne on a data path is the two-valued complement: present, non-null, and !=.
	gotExpr, gotNames, gotVals := build(t, ember.Ne("status", "open"))
	assert.Equal(t, "(attribute_exists (#0.#1)) AND (NOT (attribute_type (#0.#1, :0))) AND (#0.#1 <> :1)", gotExpr)
	assert.Equal(t, map[string]string{"#0": "data", "#1": "status"}, gotNames)
	assert.Equal(t, map[string]types.AttributeValue{":0": sS("NULL"), ":1": sS("open")}, gotVals)
}

func TestBuildFilterNeReservedVersionUnguarded(t *testing.T) {
	gotExpr, gotNames, gotVals := build(t, ember.Ne("version", 3))
	assert.Equal(t, "#0 <> :0", gotExpr)
	assert.Equal(t, map[string]string{"#0": "version"}, gotNames)
	assert.Equal(t, map[string]types.AttributeValue{":0": nN("3")}, gotVals)
}

func TestBuildFilterExistence(t *testing.T) {
	t.Run("true on data path", func(t *testing.T) {
		gotExpr, gotNames, gotVals := build(t, ember.Exists("status", true))
		assert.Equal(t, "(attribute_exists (#0.#1)) AND (NOT (attribute_type (#0.#1, :0)))", gotExpr)
		assert.Equal(t, map[string]string{"#0": "data", "#1": "status"}, gotNames)
		assert.Equal(t, map[string]types.AttributeValue{":0": sS("NULL")}, gotVals)
	})
	t.Run("false on data path", func(t *testing.T) {
		gotExpr, _, gotVals := build(t, ember.Exists("status", false))
		assert.Equal(t, "(attribute_not_exists (#0.#1)) OR (attribute_type (#0.#1, :0))", gotExpr)
		assert.Equal(t, map[string]types.AttributeValue{":0": sS("NULL")}, gotVals)
	})
}

func TestBuildFilterBoolean(t *testing.T) {
	t.Run("and", func(t *testing.T) {
		gotExpr, _, _ := build(t, ember.And(ember.Eq("a", "1"), ember.Eq("b", "2")))
		assert.Equal(t, "(#0.#1 = :0) AND (#0.#2 = :1)", gotExpr)
	})
	t.Run("or", func(t *testing.T) {
		gotExpr, _, _ := build(t, ember.Or(ember.Eq("a", "1"), ember.Eq("b", "2")))
		assert.Equal(t, "(#0.#1 = :0) OR (#0.#2 = :1)", gotExpr)
	})
	t.Run("not", func(t *testing.T) {
		gotExpr, _, _ := build(t, ember.Not(ember.Eq("status", "open")))
		assert.Equal(t, "NOT (#0.#1 = :0)", gotExpr)
	})
	t.Run("single-child and collapses", func(t *testing.T) {
		gotExpr, _, _ := build(t, ember.And(ember.Eq("a", "1")))
		assert.Equal(t, "#0.#1 = :0", gotExpr)
	})
	t.Run("empty and matches all", func(t *testing.T) {
		gotExpr, gotNames, _ := build(t, ember.And())
		assert.Equal(t, "attribute_exists (#0)", gotExpr)
		assert.Equal(t, map[string]string{"#0": "version"}, gotNames)
	})
	t.Run("empty or matches none", func(t *testing.T) {
		gotExpr, gotNames, _ := build(t, ember.Or())
		assert.Equal(t, "attribute_not_exists (#0)", gotExpr)
		assert.Equal(t, map[string]string{"#0": "version"}, gotNames)
	})
}

func TestBuildFilterUnsupportedValue(t *testing.T) {
	_, _, err := buildFilter(ember.Eq("status", []string{"nope"}))
	assert.ErrorIs(t, err, ember.ErrUnsupportedFilter)
}

func TestBuildFilterRejectsKeyAttribute(t *testing.T) {
	for _, path := range []string{"id", "type"} {
		_, _, err := buildFilter(ember.Eq(path, "x"))
		assert.ErrorIs(t, err, ember.ErrUnsupportedFilter, "path %q", path)
	}
}

func TestBuildFilterMembershipReservedVersion(t *testing.T) {
	// In on the reserved version attribute: no data. prefix, no guard.
	gotExpr, gotNames, gotVals := build(t, ember.In("version", 1, 2))
	assert.Equal(t, "#0 IN (:0, :1)", gotExpr)
	assert.Equal(t, map[string]string{"#0": "version"}, gotNames)
	assert.Equal(t, map[string]types.AttributeValue{":0": nN("1"), ":1": nN("2")}, gotVals)
}

func TestBuildFilterExistenceReservedVersion(t *testing.T) {
	// version is always present and non-null, so existence is unguarded.
	t.Run("true", func(t *testing.T) {
		gotExpr, gotNames, _ := build(t, ember.Exists("version", true))
		assert.Equal(t, "attribute_exists (#0)", gotExpr)
		assert.Equal(t, map[string]string{"#0": "version"}, gotNames)
	})
	t.Run("false", func(t *testing.T) {
		gotExpr, gotNames, _ := build(t, ember.Exists("version", false))
		assert.Equal(t, "attribute_not_exists (#0)", gotExpr)
		assert.Equal(t, map[string]string{"#0": "version"}, gotNames)
	})
}

func TestBuildFilterSingleChildOrCollapses(t *testing.T) {
	gotExpr, _, _ := build(t, ember.Or(ember.Eq("a", "1")))
	assert.Equal(t, "#0.#1 = :0", gotExpr)
}

func TestBuildFilterNotOfNe(t *testing.T) {
	// Not of the compound Ne; delegates to expression.Not over the guarded AND.
	gotExpr, gotNames, gotVals := build(t, ember.Not(ember.Ne("status", "open")))
	assert.Equal(t, "NOT ((attribute_exists (#0.#1)) AND (NOT (attribute_type (#0.#1, :0))) AND (#0.#1 <> :1))", gotExpr)
	assert.Equal(t, map[string]string{"#0": "data", "#1": "status"}, gotNames)
	assert.Equal(t, map[string]types.AttributeValue{":0": sS("NULL"), ":1": sS("open")}, gotVals)
}

// Compile-time assertion that the repository satisfies the interface.
var _ ember.EntityRepository = (*EntityRepository)(nil)
