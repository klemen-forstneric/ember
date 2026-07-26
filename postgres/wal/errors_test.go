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

func TestIsUndefinedObject(t *testing.T) {
	require.True(t, IsUndefinedObject(&pgconn.PgError{Code: "42704"}))
	require.False(t, IsUndefinedObject(&pgconn.PgError{Code: "42710"}))
	require.False(t, IsUndefinedObject(errors.New("boom")))
	require.False(t, IsUndefinedObject(nil))
}

func TestIsSlotInvalidated(t *testing.T) {
	require.True(t, IsSlotInvalidated(&pgconn.PgError{
		Code:    "55000",
		Message: `can no longer get changes from replication slot "svc_events_slot"`,
	}))
	require.False(t, IsSlotInvalidated(&pgconn.PgError{Code: "55006"}))
	require.False(t, IsSlotInvalidated(errors.New("boom")))
	require.False(t, IsSlotInvalidated(nil))
}

// pgconn returns wrapped errors from some call paths, so a bare type assertion
// (as the pg-logrepl prototype used) would miss them.
func TestPredicatesUnwrap(t *testing.T) {
	require.True(t, IsObjectInUse(fmt.Errorf("start replication: %w", &pgconn.PgError{Code: "55006"})))
	require.True(t, IsDuplicateObject(fmt.Errorf("create slot: %w", &pgconn.PgError{Code: "42710"})))
	require.True(t, IsUndefinedObject(fmt.Errorf("start replication: %w", &pgconn.PgError{Code: "42704"})))
	require.True(t, IsSlotInvalidated(fmt.Errorf("start replication: %w", &pgconn.PgError{Code: "55000"})))
}
