package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
)

type transactionPool interface {
	BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error)
}

type readOnlyConnection struct {
	pool transactionPool
}

func rollbackReadOnly(ctx context.Context, tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (c *readOnlyConnection) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	rows, err := c.Query(ctx, sql, args...)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.CommandTag(), rows.Err()
}

//nolint:ireturn
func (c *readOnlyConnection) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, errors.Wrap(err, "failed to start read-only transaction")
	}
	rows, err := tx.Query(ctx, sql, append([]any{pgx.QueryExecModeExec}, args...)...)
	if err != nil {
		rollbackReadOnly(ctx, tx)
		return nil, err
	}
	return &readOnlyRows{Rows: rows, cleanup: func() { rollbackReadOnly(ctx, tx) }}, nil
}

type readOnlyRows struct {
	pgx.Rows
	cleanup   func()
	closeOnce sync.Once
}

func (r *readOnlyRows) Close() {
	r.closeOnce.Do(func() {
		r.Rows.Close()
		r.cleanup()
	})
}

func (r *readOnlyRows) Next() bool {
	if r.Rows.Next() {
		return true
	}
	r.Close()
	return false
}
