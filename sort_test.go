package ember

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortConstructors(t *testing.T) {
	assert.Equal(t, Sort{Path: "created_at", Direction: Ascending, Ordering: Lexical}, AscLex("created_at"))
	assert.Equal(t, Sort{Path: "created_at", Direction: Descending, Ordering: Lexical}, DescLex("created_at"))
	assert.Equal(t, Sort{Path: "seq", Direction: Ascending, Ordering: Numeric}, AscNum("seq"))
	assert.Equal(t, Sort{Path: "seq", Direction: Descending, Ordering: Numeric}, DescNum("seq"))
	assert.Equal(t, Sort{}, Unsorted())
	assert.Equal(t, "", Unsorted().Path)
}

func TestSortOrderingIsUndeclaredByDefault(t *testing.T) {
	assert.NotEqual(t, Lexical, Sort{Path: "seq"}.Ordering)
	assert.NotEqual(t, Numeric, Sort{Path: "seq"}.Ordering)
	assert.Equal(t, Lexical, AscLex("seq").Ordering)
	assert.Equal(t, Numeric, AscNum("seq").Ordering)
}
