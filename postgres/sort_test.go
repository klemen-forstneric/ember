package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/klemen-forstneric/ember"
)

func TestOrderBy(t *testing.T) {
	tests := []struct {
		name string
		sort ember.Sort
		want []string
	}{
		{"unsorted", ember.Unsorted(), nil},
		{"lexical asc", ember.Asc("created_at"), []string{"data#>>'{created_at}' ASC"}},
		{"lexical desc", ember.Desc("created_at"), []string{"data#>>'{created_at}' DESC"}},
		{"numeric asc", ember.Asc("seq").Numeric(), []string{"(data#>>'{seq}')::numeric ASC"}},
		{"numeric desc", ember.Desc("seq").Numeric(), []string{"(data#>>'{seq}')::numeric DESC"}},
		{"nested path", ember.Asc("job.id"), []string{"data#>>'{job,id}' ASC"}},
		{"reserved id", ember.Asc("id"), []string{"id ASC"}},
		{"reserved version ignores ordering", ember.Asc("version").Numeric(), []string{"version ASC"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, orderBy(tt.sort))
		})
	}
}
