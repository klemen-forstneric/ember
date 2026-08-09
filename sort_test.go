package ember

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortConstructors(t *testing.T) {
	assert.Equal(t, Sort{Path: "created_at", Direction: Ascending}, Asc("created_at"))
	assert.Equal(t, Sort{Path: "created_at", Direction: Descending}, Desc("created_at"))
	assert.Equal(t, "", Sort{}.Path) // zero value = unordered
	assert.Equal(t, Sort{}, Unsorted())
	assert.Equal(t, "", Unsorted().Path)
}

func TestSortOrdering(t *testing.T) {
	assert.Equal(t, Sort{Path: "seq", Direction: Ascending, Ordering: Numeric}, Asc("seq").Numeric())
	assert.Equal(t, Sort{Path: "seq", Direction: Descending, Ordering: Numeric}, Desc("seq").Numeric())
	assert.Equal(t, Asc("seq").Lexical(), Asc("seq").Numeric().Lexical())
}

func TestSortValidate(t *testing.T) {
	require.NoError(t, Unsorted().Validate())
	require.NoError(t, Asc("seq").Numeric().Validate())
	require.NoError(t, Desc("created_at").Lexical().Validate())

	require.NoError(t, Asc("id").Validate())
	require.NoError(t, Asc("type").Validate())
	require.NoError(t, Desc("version").Validate())

	require.ErrorIs(t, Asc("seq").Validate(), ErrInvalidSort)
	require.ErrorIs(t, Desc("created_at").Validate(), ErrInvalidSort)
	require.ErrorIs(t, Asc("job.id").Validate(), ErrInvalidSort)
}

func TestSortOrderingIsUndeclaredByDefault(t *testing.T) {
	assert.NotEqual(t, Lexical, Asc("seq").Ordering)
	assert.NotEqual(t, Numeric, Asc("seq").Ordering)
	assert.Equal(t, Lexical, Asc("seq").Lexical().Ordering)
	assert.Equal(t, Numeric, Asc("seq").Numeric().Ordering)
}
