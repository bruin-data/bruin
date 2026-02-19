//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/bruin-data/bruin/pkg/query"
)

type EphemeralConnection struct {
	config DuckDBConfig
}

func NewEphemeralConnection(c DuckDBConfig) (*EphemeralConnection, error) {
	if err := EnsureADBCDriverInstalled(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ensure ADBC driver is installed: %w", err)
	}

	return &EphemeralConnection{
		config: c,
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
	cfg, ok := e.config.(Config)
	if !ok || !cfg.HasLakehouse() {
		return nil
	}

	attacher := NewLakehouseAttacher()
	if err := ValidateLakehouseConfig(cfg.Lakehouse); err != nil {
		return fmt.Errorf("invalid lakehouse config: %w", err)
	}

	statements, err := attacher.GenerateAttachStatements(cfg.Lakehouse, cfg.GetLakehouseAlias())
	if err != nil {
		return fmt.Errorf("failed to generate lakehouse statements: %w", err)
	}

	for _, stmt := range statements {
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

// Opens a direct core-ADBC connection (bypassing database/sql sqldriver).
// This path is used as a fallback when sqldriver cannot decode complex Arrow types.
func (e *EphemeralConnection) openADBCConnection(ctx context.Context) (adbc.Database, adbc.Connection, error) {
	driverManager := &drivermgr.Driver{}

	db, err := driverManager.NewDatabase(map[string]string{
		"driver": "duckdb",
		"path":   e.config.ToDBConnectionURI(),
	})
	if err != nil {
		return nil, nil, err
	}

	conn, err := db.Open(ctx)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}

	if err := e.setupLakehouseADBC(ctx, conn); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, nil, err
	}

	return db, conn, nil
}

// Applies the same lakehouse attach setup used in the standard sql-based path.
func (e *EphemeralConnection) setupLakehouseADBC(ctx context.Context, conn adbc.Connection) error {
	cfg, ok := e.config.(Config)
	if !ok || !cfg.HasLakehouse() {
		return nil
	}

	attacher := NewLakehouseAttacher()
	if err := ValidateLakehouseConfig(cfg.Lakehouse); err != nil {
		return fmt.Errorf("invalid lakehouse config: %w", err)
	}

	statements, err := attacher.GenerateAttachStatements(cfg.Lakehouse, cfg.GetLakehouseAlias())
	if err != nil {
		return fmt.Errorf("failed to generate lakehouse statements: %w", err)
	}

	for _, stmt := range statements {
		if err := e.execLakehouseStatementADBC(ctx, conn, stmt); err != nil {
			return err
		}
	}

	return nil
}

// Executes a single attach/init statement through core ADBC.
func (e *EphemeralConnection) execLakehouseStatementADBC(ctx context.Context, conn adbc.Connection, stmt string) error {
	statement, err := conn.NewStatement()
	if err != nil {
		return fmt.Errorf("failed to execute lakehouse statement: %w", err)
	}
	defer statement.Close()

	if err := statement.SetSqlQuery(stmt); err != nil {
		return fmt.Errorf("failed to execute lakehouse statement: %w", err)
	}

	if _, err := statement.ExecuteUpdate(ctx); err != nil {
		return fmt.Errorf("failed to execute lakehouse statement: %w", err)
	}

	return nil
}

// Executes a query via core ADBC and converts Arrow records into QueryResult.
func (e *EphemeralConnection) SelectWithSchemaViaADBC(ctx context.Context, queryString string) (*query.QueryResult, error) {
	db, conn, err := e.openADBCConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(queryString); err != nil {
		return nil, err
	}

	recordReader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}
	defer recordReader.Release()

	return convertRecordReaderToQueryResult(recordReader)
}

// Materializes the full RecordReader into Bruin's query result shape.
func convertRecordReaderToQueryResult(recordReader array.RecordReader) (*query.QueryResult, error) {
	schema := recordReader.Schema()
	fields := schema.Fields()

	result := &query.QueryResult{
		Columns:     make([]string, len(fields)),
		ColumnTypes: make([]string, len(fields)),
		Rows:        make([][]interface{}, 0),
	}

	for i, field := range fields {
		result.Columns[i] = field.Name
		result.ColumnTypes[i] = normalizeTypeName(field.Type.String())
	}

	for recordReader.Next() {
		record := recordReader.Record()
		columns := record.Columns()

		for rowIdx := 0; rowIdx < int(record.NumRows()); rowIdx++ {
			row := make([]interface{}, len(columns))
			for colIdx, col := range columns {
				value, err := recordValueFromArrowArray(col, rowIdx)
				if err != nil {
					return nil, err
				}
				row[colIdx] = value
			}
			result.Rows = append(result.Rows, row)
		}
	}

	if err := recordReader.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// Converts a single Arrow cell to a Go value that is safe for output/logging.
func recordValueFromArrowArray(col arrow.Array, rowIdx int) (interface{}, error) {
	if col.IsNull(rowIdx) {
		return nil, nil
	}

	if colUnion, ok := col.(array.Union); ok {
		col = colUnion.Field(colUnion.ChildID(rowIdx))
	}

	switch typed := col.(type) {
	case *array.Boolean:
		return typed.Value(rowIdx), nil
	case *array.Int8:
		return typed.Value(rowIdx), nil
	case *array.Uint8:
		return typed.Value(rowIdx), nil
	case *array.Int16:
		return typed.Value(rowIdx), nil
	case *array.Uint16:
		return typed.Value(rowIdx), nil
	case *array.Int32:
		return typed.Value(rowIdx), nil
	case *array.Uint32:
		return typed.Value(rowIdx), nil
	case *array.Int64:
		return typed.Value(rowIdx), nil
	case *array.Uint64:
		return typed.Value(rowIdx), nil
	case *array.Float32:
		return typed.Value(rowIdx), nil
	case *array.Float64:
		return typed.Value(rowIdx), nil
	case *array.String:
		return copyString(typed.Value(rowIdx)), nil
	case *array.LargeString:
		return copyString(typed.Value(rowIdx)), nil
	case *array.Binary:
		value := typed.Value(rowIdx)
		copied := make([]byte, len(value))
		copy(copied, value)
		return copied, nil
	case *array.LargeBinary:
		value := typed.Value(rowIdx)
		copied := make([]byte, len(value))
		copy(copied, value)
		return copied, nil
	case *array.Date32:
		return typed.Value(rowIdx).ToTime().Format(time.RFC3339), nil
	case *array.Date64:
		return typed.Value(rowIdx).ToTime().Format(time.RFC3339), nil
	case *array.Time32:
		timeType := typed.DataType().(*arrow.Time32Type)
		return typed.Value(rowIdx).ToTime(timeType.Unit).Format(time.RFC3339), nil
	case *array.Time64:
		timeType := typed.DataType().(*arrow.Time64Type)
		return typed.Value(rowIdx).ToTime(timeType.Unit).Format(time.RFC3339), nil
	case *array.Timestamp:
		timestampType := typed.DataType().(*arrow.TimestampType)
		return typed.Value(rowIdx).ToTime(timestampType.Unit).Format(time.RFC3339), nil
	case *array.Decimal128:
		decimalType := typed.DataType().(*arrow.Decimal128Type)
		return convertDecimal128WithScale(typed.Value(rowIdx), int64(decimalType.Scale)), nil
	}

	// For complex types, Arrow arrays expose marshaled values (JSON-compatible).
	if marshaler, ok := col.(interface{ GetOneForMarshal(int) interface{} }); ok {
		return normalizeMarshaledArrowValue(marshaler.GetOneForMarshal(rowIdx)), nil
	}

	if valueStringer, ok := col.(interface{ ValueStr(int) string }); ok {
		return copyString(valueStringer.ValueStr(rowIdx)), nil
	}

	return nil, fmt.Errorf("unsupported arrow type in adbc fallback: %s", col.DataType().String())
}

// Recursively converts marshaled Arrow values into plain Go values.
func normalizeMarshaledArrowValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case nil:
		return nil
	case json.RawMessage:
		var decoded interface{}
		if err := json.Unmarshal(typed, &decoded); err != nil {
			return copyString(string(typed))
		}
		return normalizeMarshaledArrowValue(decoded)
	case map[string]interface{}:
		normalized := make(map[string]interface{}, len(typed))
		for key, nested := range typed {
			normalized[key] = normalizeMarshaledArrowValue(nested)
		}
		return normalized
	case []interface{}:
		normalized := make([]interface{}, len(typed))
		for i, nested := range typed {
			normalized[i] = normalizeMarshaledArrowValue(nested)
		}
		return normalized
	default:
		return typed
	}
}

// Keeps decimal handling consistent with the existing sql-based conversion path.
func convertDecimal128WithScale(value decimal128.Num, scale int64) float64 {
	floatValue := value.ToFloat64(int32(scale))
	return roundToScale(floatValue, scale)
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
