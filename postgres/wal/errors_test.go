package wal

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestIsDuplicateObject(t *testing.T) {
	require.True(t, IsDuplicateObject(&pgconn.PgError{Code: "42710"}))
	require.False(t, IsDuplicateObject(&pgconn.PgError{Code: "55006"}))
	require.False(t, IsDuplicateObject(errors.New("boom")))
	require.False(t, IsDuplicateObject(nil))
}

func TestIsObjectInUse(t *testing.T) {
	require.True(t, IsObjectInUse(&pgconn.PgError{Code: "55006"}))
	require.False(t, IsObjectInUse(&pgconn.PgError{Code: "42710"}))
	require.False(t, IsObjectInUse(errors.New("boom")))
}

// pgconn returns wrapped errors from some call paths, so a bare type assertion
// (as the pg-logrepl prototype used) would miss them.
func TestPredicatesUnwrap(t *testing.T) {
	require.True(t, IsObjectInUse(fmt.Errorf("start replication: %w", &pgconn.PgError{Code: "55006"})))
	require.True(t, IsDuplicateObject(fmt.Errorf("create slot: %w", &pgconn.PgError{Code: "42710"})))
}
