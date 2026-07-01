package ember

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortConstructors(t *testing.T) {
	assert.Equal(t, Sort{Path: "created_at", Direction: Ascending}, Asc("created_at"))
	assert.Equal(t, Sort{Path: "created_at", Direction: Descending}, Desc("created_at"))
	assert.Equal(t, "", Sort{}.Path) // zero value = unordered
}
