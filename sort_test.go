package ember

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortConstructors(t *testing.T) {
	assert.Equal(t, Sort{Path: "created_at", Direction: Ascending}, Asc("created_at"))
	assert.Equal(t, Sort{Path: "created_at", Direction: Descending}, Desc("created_at"))
	assert.Equal(t, "", Sort{}.Path) // zero value = unordered
	assert.Equal(t, Sort{}, Unsorted())
	assert.Equal(t, "", Unsorted().Path)
}

func TestSortOrdering(t *testing.T) {
	assert.Equal(t, Lexical, Asc("created_at").Ordering)
	assert.Equal(t, Lexical, Unsorted().Ordering)
	assert.Equal(t, Sort{Path: "seq", Direction: Ascending, Ordering: Numeric}, Asc("seq").Numeric())
	assert.Equal(t, Sort{Path: "seq", Direction: Descending, Ordering: Numeric}, Desc("seq").Numeric())
	assert.Equal(t, Asc("seq"), Asc("seq").Numeric().Lexical())
}
