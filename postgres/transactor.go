package postgres

import (
	"context"
	"database/sql"

	"github.com/klemen-forstneric/ember"
)

type txKey struct{}

// Conn is the subset of *sql.DB / *sql.Tx the repositories use, so a write
// can run on whichever is active on the ctx.
type Conn interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// DB wraps the connection pool. It runs transactions (satisfying ember.Transactor)
// and hands repositories the handle bound to the current ctx.
type DB struct {
	pool *sql.DB
}

func NewDB(pool *sql.DB) *DB {
	return &DB{pool: pool}
}

var _ ember.Transactor = (*DB)(nil)

// Conn returns the transaction carried on ctx if one is active, else the pool.
func (d *DB) Conn(ctx context.Context) Conn {
	if tx, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return tx
	}
	return d.pool
}

func (d *DB) InTx(ctx context.Context) bool {
	_, ok := ctx.Value(txKey{}).(*sql.Tx)
	return ok
}

// WithinTx runs fn inside a transaction. Reentrant: if the ctx already carries a
// *sql.Tx it joins that transaction rather than beginning a nested one
// (database/sql has no nested transactions).
func (d *DB) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if d.InTx(ctx) {
		return fn(ctx)
	}

	tx, err := d.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}
