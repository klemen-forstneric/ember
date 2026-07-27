package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNopTransactorRunsInlineAndReportsNoTx(t *testing.T) {
	ctx := context.Background()
	ran := false

	require.NoError(t, NopTransactor.WithinTx(ctx, func(inner context.Context) error {
		ran = true
		assert.Equal(t, ctx, inner) // no transaction stashed on the context
		assert.False(t, NopTransactor.InTx(inner))
		return nil
	}))

	assert.True(t, ran)
	assert.False(t, NopTransactor.InTx(ctx))
}

func TestNopTransactorPropagatesError(t *testing.T) {
	boom := errors.New("boom")
	assert.ErrorIs(t, NopTransactor.WithinTx(context.Background(), func(context.Context) error { return boom }), boom)
}
