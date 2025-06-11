//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type EphemeralConnection struct {
	config DuckDBConfig
}

func NewEphemeralConnection(c DuckDBConfig) (*EphemeralConnection, error) {
	conn, err := sqlx.Open("duckdb", c.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}
	defer func(conn *sqlx.DB) {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}(conn)

	err = conn.Ping()
	if err != nil {
		return nil, err
	}

	return &EphemeralConnection{config: c}, nil
}

func (e *EphemeralConnection) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	conn, err := sqlx.Open("duckdb", e.config.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}
	defer func(conn *sqlx.DB) {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}(conn)

	return conn.QueryContext(ctx, query, args...) //nolint
}

func (e *EphemeralConnection) ExecContext(ctx context.Context, sql string, arguments ...any) (sql.Result, error) {
	conn, err := sqlx.Open("duckdb", e.config.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}
	defer func(conn *sqlx.DB) {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}(conn)

	return conn.ExecContext(ctx, sql, arguments...)
}

func (e *EphemeralConnection) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	conn, err := sqlx.Open("duckdb", e.config.ToDBConnectionURI())
	if err != nil {
		// Cannot return error from this function signature, so we panic.
		// This is not ideal, but it's the best we can do with the current interface.
		panic(err)
	}
	defer func(conn *sqlx.DB) {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}(conn)

	return conn.QueryRowContext(ctx, query, args...)
}
