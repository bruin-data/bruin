//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// sqlxAdapter adapts a *sqlx.DB to implement the connection interface.
// This is used for testing with sqlmock.
type sqlxAdapter struct {
	db *sqlx.DB
}

func newSQLXAdapter(db *sqlx.DB) *sqlxAdapter {
	return &sqlxAdapter{db: db}
}

//nolint:ireturn // Returning interface type is by design for abstraction
func (s *sqlxAdapter) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return &sqlRowsAdapter{rows: rows}, nil
}

func (s *sqlxAdapter) ExecContext(ctx context.Context, sql string, arguments ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, sql, arguments...)
}

//nolint:ireturn // Returning interface type is by design for abstraction
func (s *sqlxAdapter) QueryRowContext(ctx context.Context, query string, args ...any) Row {
	row := s.db.QueryRowContext(ctx, query, args...)
	return &sqlRowAdapter{row: row}
}

// sqlRowsAdapter adapts *sql.Rows to implement the Rows interface.
type sqlRowsAdapter struct {
	rows *sql.Rows
}

func (s *sqlRowsAdapter) Next() bool {
	return s.rows.Next()
}

func (s *sqlRowsAdapter) Scan(dest ...interface{}) error {
	return s.rows.Scan(dest...)
}

func (s *sqlRowsAdapter) Columns() ([]string, error) {
	return s.rows.Columns()
}

func (s *sqlRowsAdapter) ColumnTypes() ([]ColumnType, error) {
	colTypes, err := s.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	result := make([]ColumnType, len(colTypes))
	for i, ct := range colTypes {
		result[i] = &sqlColumnTypeAdapter{ct: ct}
	}
	return result, nil
}

func (s *sqlRowsAdapter) Err() error {
	return s.rows.Err()
}

func (s *sqlRowsAdapter) Close() error {
	return s.rows.Close()
}

// sqlRowAdapter adapts *sql.Row to implement the Row interface.
type sqlRowAdapter struct {
	row *sql.Row
}

func (s *sqlRowAdapter) Scan(dest ...interface{}) error {
	return s.row.Scan(dest...)
}

// sqlColumnTypeAdapter adapts *sql.ColumnType to implement the ColumnType interface.
type sqlColumnTypeAdapter struct {
	ct *sql.ColumnType
}

func (s *sqlColumnTypeAdapter) DatabaseTypeName() string {
	return s.ct.DatabaseTypeName()
}
