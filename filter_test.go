package ember

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstructors(t *testing.T) {
	tests := []struct {
		name string
		got  Filter
		want Filter
	}{
		{"Eq", Eq("status", "open"), Comparison{Path: "status", Op: OpEq, Value: "open"}},
		{"Ne", Ne("status", "open"), Comparison{Path: "status", Op: OpNe, Value: "open"}},
		{"Gt", Gt("total", 100), Comparison{Path: "total", Op: OpGt, Value: 100}},
		{"Gte", Gte("total", 100), Comparison{Path: "total", Op: OpGte, Value: 100}},
		{"Lt", Lt("total", 100), Comparison{Path: "total", Op: OpLt, Value: 100}},
		{"Lte", Lte("total", 100), Comparison{Path: "total", Op: OpLte, Value: 100}},
		{"In", In("region", "EU", "UK"), Membership{Path: "region", Values: []any{"EU", "UK"}}},
		{"Exists", Exists("status", true), Existence{Path: "status", Exists: true}},
		{"Not", Not(Eq("a", 1)), Negation{Filter: Comparison{Path: "a", Op: OpEq, Value: 1}}},
		{
			"And",
			And(Eq("a", 1), Eq("b", 2)),
			Conjunction{Filters: []Filter{
				Comparison{Path: "a", Op: OpEq, Value: 1},
				Comparison{Path: "b", Op: OpEq, Value: 2},
			}},
		},
		{
			"Or",
			Or(Eq("a", 1), Eq("b", 2)),
			Disjunction{Filters: []Filter{
				Comparison{Path: "a", Op: OpEq, Value: 1},
				Comparison{Path: "b", Op: OpEq, Value: 2},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}
