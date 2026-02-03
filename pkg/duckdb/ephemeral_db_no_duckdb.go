//go:build bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"

	"github.com/bruin-data/bruin/pkg/config"
)

// Row interface abstracts sql.Row to allow custom implementations.
type Row interface {
	Scan(dest ...any) error
	Err() error
}

// Rows interface abstracts sql.Rows to allow custom implementations that manage connection lifecycle.
type Rows interface {
	Close() error
	Columns() ([]string, error)
	ColumnTypes() ([]*sql.ColumnType, error)
	Err() error
	Next() bool
	Scan(dest ...any) error
}

type EphemeralConnection struct {
	config DuckDBConfig
}

func NewEphemeralConnection(c DuckDBConfig, lakehouse *config.LakehouseConfig, alias string) (*EphemeralConnection, error) {
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
