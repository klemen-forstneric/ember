package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestWithinTxCommitsOnSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tr := NewTransactor(db)
	err = tr.WithinTx(context.Background(), func(ctx context.Context) error {
		_, err := querierFrom(ctx, db).ExecContext(ctx, "INSERT INTO t VALUES (1)")
		return err
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxRollsBackOnError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	boom := errors.New("boom")
	mock.ExpectBegin()
	mock.ExpectRollback()

	tr := NewTransactor(db)
	err = tr.WithinTx(context.Background(), func(ctx context.Context) error { return boom })

	require.ErrorIs(t, err, boom)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxReentrantJoinsExistingTx(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// One begin (ours), the fn's exec, one commit (ours) — the reentrant
	// WithinTx must NOT begin a second transaction.
	mock.ExpectBegin()
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	outer, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	ctx := ctxWithTx(context.Background(), outer)

	tr := NewTransactor(db)
	ran := false
	err = tr.WithinTx(ctx, func(ctx context.Context) error {
		ran = true
		require.NotNil(t, txFromCtx(ctx), "fn keeps the existing tx on ctx")
		_, e := querierFrom(ctx, db).ExecContext(ctx, "INSERT INTO t VALUES (1)")
		return e
	})
	require.NoError(t, err)
	require.True(t, ran)
	require.NoError(t, outer.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxBeginError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	boom := errors.New("begin boom")
	mock.ExpectBegin().WillReturnError(boom)

	tr := NewTransactor(db)
	err = tr.WithinTx(context.Background(), func(ctx context.Context) error { return nil })

	require.ErrorIs(t, err, boom)
	require.NoError(t, mock.ExpectationsWereMet())
}
