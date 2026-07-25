package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
)

func TestLockerTryLockAcquired(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT pg_try_advisory_lock").
		WithArgs(lockID("k")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))

	l, err := NewLocker(db).TryLock(context.Background(), "k")

	require.NoError(t, err)
	require.NotNil(t, l)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLockerTryLockNotAcquired(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT pg_try_advisory_lock").
		WithArgs(lockID("k")).
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(false))
	// No unlock expectation: a lock we never acquired must never be released.

	l, err := NewLocker(db).TryLock(context.Background(), "k")

	require.NoError(t, err)
	require.Nil(t, l)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLockerTryLockQueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	wantErr := errors.New("boom")
	mock.ExpectQuery("SELECT pg_try_advisory_lock").
		WithArgs(lockID("k")).
		WillReturnError(wantErr)

	l, err := NewLocker(db).TryLock(context.Background(), "k")

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, l)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLockRelease(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	id := lockID("k")
	mock.ExpectQuery("SELECT pg_try_advisory_lock").
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	mock.ExpectExec("SELECT pg_advisory_unlock").
		WithArgs(id).
		WillReturnResult(sqlmock.NewResult(0, 1))

	l, err := NewLocker(db).TryLock(context.Background(), "k")
	require.NoError(t, err)
	require.NotNil(t, l)

	require.NoError(t, l.Release(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLockReleaseTwiceUnlocksOnce(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	id := lockID("k")
	mock.ExpectQuery("SELECT pg_try_advisory_lock").
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	mock.ExpectExec("SELECT pg_advisory_unlock").
		WithArgs(id).
		WillReturnResult(sqlmock.NewResult(0, 1))

	l, err := NewLocker(db).TryLock(context.Background(), "k")
	require.NoError(t, err)

	require.NoError(t, l.Release(context.Background()))
	require.NoError(t, l.Release(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLockID(t *testing.T) {
	require.Equal(t, lockID("same"), lockID("same"))
	require.NotEqual(t, lockID("a"), lockID("b"))
}

var _ ember.Locker = (*Locker)(nil)
