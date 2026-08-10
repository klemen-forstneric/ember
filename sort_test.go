package ember

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortConstructors(t *testing.T) {
	assert.Equal(t, Sort{Path: "created_at", Direction: DirectionAscending, Ordering: OrderingLexical}, AscLex("created_at"))
	assert.Equal(t, Sort{Path: "created_at", Direction: DirectionDescending, Ordering: OrderingLexical}, DescLex("created_at"))
	assert.Equal(t, Sort{Path: "seq", Direction: DirectionAscending, Ordering: OrderingNumeric}, AscNum("seq"))
	assert.Equal(t, Sort{Path: "seq", Direction: DirectionDescending, Ordering: OrderingNumeric}, DescNum("seq"))
	assert.Equal(t, Sort{}, Unsorted())
	assert.Equal(t, "", Unsorted().Path)
}

func TestSortOrderingIsUndeclaredByDefault(t *testing.T) {
	assert.NotEqual(t, OrderingLexical, Sort{Path: "seq"}.Ordering)
	assert.NotEqual(t, OrderingNumeric, Sort{Path: "seq"}.Ordering)
	assert.Equal(t, OrderingLexical, AscLex("seq").Ordering)
	assert.Equal(t, OrderingNumeric, AscNum("seq").Ordering)
}
