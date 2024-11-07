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
	defer conn.Close()

	err = conn.Ping()
	if err != nil {
		return nil, err
	}

	return &EphemeralConnection{config: c}, nil
}

func (c *EphemeralConnection) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	conn, err := sqlx.Open("duckdb", c.config.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return conn.QueryContext(ctx, query, args...) //nolint
}

func (c *EphemeralConnection) ExecContext(ctx context.Context, sql string, arguments ...any) (sql.Result, error) {
	conn, err := sqlx.Open("duckdb", c.config.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return conn.ExecContext(ctx, sql, arguments...)
}
