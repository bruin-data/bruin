//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/bruin-data/bruin/pkg/config"
)

type EphemeralConnection struct {
	config              DuckDBConfig
	lakehouseStatements []string
}

func NewEphemeralConnection(c DuckDBConfig) (*EphemeralConnection, error) {
	if err := EnsureADBCDriverInstalled(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ensure ADBC driver is installed: %w", err)
	}

	// Extract lakehouse config if available
	var lakehouse *config.LakehouseConfig
	var alias string
	if cfg, ok := c.(Config); ok && cfg.HasLakehouse() {
		// Validate lakehouse config for DuckDB-specific requirements
		if err := ValidateLakehouseConfig(cfg.Lakehouse); err != nil {
			return nil, fmt.Errorf("invalid lakehouse config: %w", err)
		}
		lakehouse = cfg.Lakehouse
		alias = cfg.GetLakehouseAlias()
	}

	var statements []string
	if lakehouse != nil {
		attacher := NewLakehouseAttacher()
		var err error
		statements, err = attacher.GenerateAttachStatements(lakehouse, alias)
		if err != nil {
			return nil, fmt.Errorf("failed to generate lakehouse statements: %w", err)
		}
	}

	return &EphemeralConnection{
		config:              c,
		lakehouseStatements: statements,
	}, nil
}

func (e *EphemeralConnection) openDB(ctx context.Context) (*sql.DB, error) {
	path := e.config.ToDBConnectionURI()
	db, err := sql.Open("adbc_duckdb", "driver=duckdb;path="+path)
	if err != nil {
		return nil, err
	}
	if err := e.setupLakehouse(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (e *EphemeralConnection) setupLakehouse(ctx context.Context, db *sql.DB) error {
	for _, stmt := range e.lakehouseStatements {
		if err := e.execLakehouseStatement(ctx, db, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (e *EphemeralConnection) execLakehouseStatement(ctx context.Context, db *sql.DB, stmt string) error {
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return fmt.Errorf("failed to execute lakehouse statement: %w", err)
	}
	defer rows.Close()

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to execute lakehouse statement: %w", err)
	}
	return nil
}

//nolint:ireturn
func (e *EphemeralConnection) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	db, err := e.openDB(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return bufferRows(rows)
}

func (e *EphemeralConnection) ExecContext(ctx context.Context, sqlStr string, arguments ...any) (sql.Result, error) {
	db, err := e.openDB(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, sqlStr, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return driver.RowsAffected(0), rows.Err()
}

//nolint:ireturn
func (e *EphemeralConnection) QueryRowContext(ctx context.Context, query string, args ...any) Row {
	db, err := e.openDB(ctx)
	if err != nil {
		return &errorRow{err: err}
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return &errorRow{err: err}
	}
	defer rows.Close()

	buffered, err := bufferRows(rows)
	if err != nil {
		return &errorRow{err: err}
	}

	return &bufferedRow{rows: buffered}
}

// bufferRows reads all rows into memory and returns a bufferedRows that can iterate over them.
func bufferRows(rows *sql.Rows) (*bufferedRows, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var data [][]any
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		// Deep copy values - strings must be copied because they may reference Arrow buffers
		row := make([]any, len(values))
		for i, v := range values {
			row[i] = copyValue(v)
		}
		data = append(data, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &bufferedRows{
		columns:     cols,
		columnTypes: colTypes,
		data:        data,
		index:       -1,
	}, nil
}

// bufferedRows holds rows data in memory and implements the Rows interface.
type bufferedRows struct {
	columns     []string
	columnTypes []*sql.ColumnType
	data        [][]any
	index       int
	err         error
}

func (r *bufferedRows) Close() error {
	return nil
}

func (r *bufferedRows) Columns() ([]string, error) {
	return r.columns, nil
}

func (r *bufferedRows) ColumnTypes() ([]*sql.ColumnType, error) {
	return r.columnTypes, nil
}

func (r *bufferedRows) Err() error {
	return r.err
}

func (r *bufferedRows) Next() bool {
	r.index++
	return r.index < len(r.data)
}

func (r *bufferedRows) Scan(dest ...any) error {
	if r.index < 0 || r.index >= len(r.data) {
		return sql.ErrNoRows
	}
	row := r.data[r.index]
	if len(dest) != len(row) {
		return fmt.Errorf("sql: expected %d destination arguments in Scan, got %d", len(row), len(dest))
	}
	for i, v := range row {
		if err := convertAssign(dest[i], v); err != nil {
			return err
		}
	}
	return nil
}

// bufferedRow wraps bufferedRows to implement the Row interface.
type bufferedRow struct {
	rows    *bufferedRows
	scanned bool
}

func (r *bufferedRow) Scan(dest ...any) error {
	if r.scanned {
		return sql.ErrNoRows
	}
	r.scanned = true

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	return r.rows.Scan(dest...)
}

func (r *bufferedRow) Err() error {
	return r.rows.Err()
}

type errorRow struct {
	err error
}

func (r *errorRow) Scan(_ ...any) error {
	return r.err
}

func (r *errorRow) Err() error {
	return r.err
}

// copyValue creates a deep copy of a value, ensuring strings and byte slices
// don't reference Arrow buffer memory that may be freed.
func copyValue(v any) any {
	switch val := v.(type) {
	case string:
		// Copy string to new backing array
		b := make([]byte, len(val))
		copy(b, val)
		return string(b)
	case []byte:
		if val == nil {
			return nil
		}
		cp := make([]byte, len(val))
		copy(cp, val)
		return cp
	default:
		return v
	}
}

// convertAssign copies src to dest, handling pointer destinations.
// Uses reflection as a fallback for types not explicitly handled.
func convertAssign(dest, src any) error {
	switch d := dest.(type) {
	case *any:
		*d = src
		return nil
	case *string:
		if src == nil {
			*d = ""
		} else if s, ok := src.(string); ok {
			*d = s
		} else {
			*d = fmt.Sprintf("%v", src)
		}
		return nil
	case *int:
		if src == nil {
			*d = 0
		} else if i, ok := src.(int64); ok {
			*d = int(i)
		} else if i, ok := src.(int); ok {
			*d = i
		}
		return nil
	case *int64:
		if src == nil {
			*d = 0
		} else if i, ok := src.(int64); ok {
			*d = i
		}
		return nil
	case *float64:
		if src == nil {
			*d = 0
		} else if f, ok := src.(float64); ok {
			*d = f
		}
		return nil
	case *bool:
		if src == nil {
			*d = false
		} else if b, ok := src.(bool); ok {
			*d = b
		}
		return nil
	case *[]byte:
		if src == nil {
			*d = nil
		} else if b, ok := src.([]byte); ok {
			*d = b
		}
		return nil
	}

	// Use reflection for any other pointer type
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return fmt.Errorf("destination must be a pointer, got %T", dest)
	}

	if src == nil {
		destVal.Elem().Set(reflect.Zero(destVal.Elem().Type()))
		return nil
	}

	srcVal := reflect.ValueOf(src)
	destElem := destVal.Elem()

	// Direct assignment if types match
	if srcVal.Type().AssignableTo(destElem.Type()) {
		destElem.Set(srcVal)
		return nil
	}

	// Try conversion if types are convertible
	if srcVal.Type().ConvertibleTo(destElem.Type()) {
		destElem.Set(srcVal.Convert(destElem.Type()))
		return nil
	}

	return fmt.Errorf("cannot assign %T to %T", src, dest)
}
