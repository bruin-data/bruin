//go:build bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
)

type EphemeralConnection struct {
	config DuckDBConfig
}

func NewEphemeralConnection(c DuckDBConfig) (*EphemeralConnection, error) {
	return nil, errDuckDBNotSupported
}

func (c *EphemeralConnection) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return nil, errDuckDBNotSupported
}

func (c *EphemeralConnection) ExecContext(ctx context.Context, sql string, arguments ...any) (sql.Result, error) {
	return nil, errDuckDBNotSupported
}
