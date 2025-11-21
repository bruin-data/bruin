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

//nolint:ireturn
func (c *EphemeralConnection) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	return nil, errDuckDBNotSupported
}

func (c *EphemeralConnection) ExecContext(ctx context.Context, sql string, arguments ...any) (sql.Result, error) {
	return nil, errDuckDBNotSupported
}

//nolint:ireturn
func (c *EphemeralConnection) QueryRowContext(ctx context.Context, query string, args ...any) Row {
	return &noopRow{}
}

type noopRow struct{}

func (r *noopRow) Scan(_ ...any) error {
	return errDuckDBNotSupported
}

func (r *noopRow) Err() error {
	return errDuckDBNotSupported
}
