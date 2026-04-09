//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// EphemeralConnection uses the ADBC low-level API to query DuckDB directly,
// bypassing the database/sql adapter which doesn't support complex Arrow types
// (LIST, STRUCT, MAP).
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

// openADBC creates an ADBC database and connection, including lakehouse setup.
// The caller must close both the connection and database when done.
//
//nolint:ireturn
func (e *EphemeralConnection) openADBC(ctx context.Context) (adbc.Database, adbc.Connection, error) {
	path := e.config.ToDBConnectionURI()
	opts := map[string]string{
		"driver": "duckdb",
		"path":   path,
	}

	if cfg, ok := e.config.(Config); ok && cfg.ReadOnly {
		opts["access_mode"] = "read_only"
	}

	var drv drivermgr.Driver
	adb, err := drv.NewDatabase(opts)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create ADBC database: %w", err)
	}

	conn, err := adb.Open(ctx)
	if err != nil {
		adb.Close()
		return nil, nil, fmt.Errorf("failed to open ADBC connection: %w", err)
	}

	if err := e.setupLakehouseADBC(ctx, conn); err != nil {
		conn.Close()
		adb.Close()
		return nil, nil, err
	}

	return adb, conn, nil
}

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

	for _, sqlStr := range statements {
		if err := execADBCStatement(ctx, conn, sqlStr); err != nil {
			return fmt.Errorf("failed to execute lakehouse statement: %w", err)
		}
	}
	return nil
}

func execADBCStatement(ctx context.Context, conn adbc.Connection, sqlStr string) error {
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sqlStr); err != nil {
		return err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return err
	}
	if reader != nil {
		reader.Release()
	}
	return nil
}

//nolint:ireturn
func (e *EphemeralConnection) QueryContext(ctx context.Context, queryStr string, args ...any) (Rows, error) {
	adb, conn, err := e.openADBC(ctx)
	if err != nil {
		return nil, err
	}
	defer adb.Close()
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(inlineQueryArgs(queryStr, args)); err != nil {
		return nil, err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return &bufferedRows{
			columns:     []string{},
			columnTypes: []*ColumnType{},
			index:       -1,
		}, nil
	}
	defer reader.Release()

	return bufferArrowReader(reader)
}

func (e *EphemeralConnection) ExecContext(ctx context.Context, sqlStr string, arguments ...any) (sql.Result, error) {
	adb, conn, err := e.openADBC(ctx)
	if err != nil {
		return nil, err
	}
	defer adb.Close()
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(inlineQueryArgs(sqlStr, arguments)); err != nil {
		return nil, err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}
	if reader != nil {
		reader.Release()
	}

	return driver.RowsAffected(0), nil
}

//nolint:ireturn
func (e *EphemeralConnection) QueryRowContext(ctx context.Context, queryStr string, args ...any) Row {
	adb, conn, err := e.openADBC(ctx)
	if err != nil {
		return &errorRow{err: err}
	}
	defer adb.Close()
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return &errorRow{err: err}
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(inlineQueryArgs(queryStr, args)); err != nil {
		return &errorRow{err: err}
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return &errorRow{err: err}
	}
	if reader == nil {
		return &errorRow{err: sql.ErrNoRows}
	}
	defer reader.Release()

	buffered, err := bufferArrowReader(reader)
	if err != nil {
		return &errorRow{err: err}
	}

	return &bufferedRow{rows: buffered}
}

// bufferArrowReader reads all records from an Arrow RecordReader into memory.
func bufferArrowReader(reader array.RecordReader) (*bufferedRows, error) {
	schema := reader.Schema()
	numFields := schema.NumFields()
	cols := make([]string, numFields)
	colTypes := make([]*ColumnType, numFields)

	for i, field := range schema.Fields() {
		cols[i] = field.Name
		colTypes[i] = arrowFieldToColumnType(field)
	}

	var data [][]any
	for reader.Next() {
		record := reader.RecordBatch()
		numRows := int(record.NumRows())
		numCols := int(record.NumCols())
		for i := range numRows {
			row := make([]any, numCols)
			for j := range numCols {
				col := record.Column(j)
				if col.IsNull(i) {
					row[j] = nil
				} else {
					row[j] = extractArrowValue(col, i)
				}
			}
			data = append(data, row)
		}
	}

	if err := reader.Err(); err != nil {
		return nil, err
	}

	return &bufferedRows{
		columns:     cols,
		columnTypes: colTypes,
		data:        data,
		index:       -1,
	}, nil
}

// extractArrowValue extracts a native Go value from an Arrow array at position i.
// Scalar types are returned as their natural Go equivalents.
// Complex types (LIST, STRUCT, MAP) are returned as their string representation,
// which is the key advantage over the database/sql adapter that cannot handle these types.
func extractArrowValue(col arrow.Array, i int) any {
	switch arr := col.(type) {
	case *array.Boolean:
		return arr.Value(i)
	case *array.Int8:
		return int64(arr.Value(i))
	case *array.Int16:
		return int64(arr.Value(i))
	case *array.Int32:
		return int64(arr.Value(i))
	case *array.Int64:
		return arr.Value(i)
	case *array.Uint8:
		return int64(arr.Value(i))
	case *array.Uint16:
		return int64(arr.Value(i))
	case *array.Uint32:
		return int64(arr.Value(i))
	case *array.Uint64:
		return int64(arr.Value(i)) //nolint:gosec // overflow is acceptable; returning raw uint64 causes silent zeroing in convertAssign
	case *array.Float32:
		return float64(arr.Value(i))
	case *array.Float64:
		return arr.Value(i)
	case *array.String:
		return copyString(arr.Value(i))
	case *array.LargeString:
		return copyString(arr.Value(i))
	case *array.Binary:
		v := arr.Value(i)
		cp := make([]byte, len(v))
		copy(cp, v)
		return cp
	case *array.LargeBinary:
		v := arr.Value(i)
		cp := make([]byte, len(v))
		copy(cp, v)
		return cp
	case *array.Date32:
		return arr.Value(i).ToTime()
	case *array.Date64:
		return arr.Value(i).ToTime()
	case *array.Time32:
		return arr.Value(i).ToTime(arr.DataType().(*arrow.Time32Type).Unit)
	case *array.Time64:
		return arr.Value(i).ToTime(arr.DataType().(*arrow.Time64Type).Unit)
	case *array.Timestamp:
		return arr.Value(i).ToTime(arr.DataType().(*arrow.TimestampType).Unit)
	case *array.Decimal128:
		return arr.Value(i)
	default:
		// Complex types (LIST, STRUCT, MAP, UNION, etc.) and any unhandled types
		// are returned as their string representation.
		return copyString(col.ValueStr(i))
	}
}

// arrowFieldToColumnType converts an Arrow schema field to a ColumnType.
func arrowFieldToColumnType(field arrow.Field) *ColumnType {
	ct := &ColumnType{
		name:         field.Name,
		databaseType: normalizeTypeName(field.Type.String()),
	}
	if dt, ok := field.Type.(*arrow.Decimal128Type); ok {
		ct.precision = int64(dt.Precision)
		ct.scale = int64(dt.Scale)
		ct.hasDecimalInfo = true
	}
	return ct
}

// inlineQueryArgs substitutes positional ? parameters with their escaped values.
// This is used for internal queries (e.g. information_schema lookups) where
// parameters are controlled by Bruin, not user input.
func inlineQueryArgs(queryStr string, args []any) string {
	if len(args) == 0 {
		return queryStr
	}
	parts := strings.SplitN(queryStr, "?", len(args)+1)
	if len(parts) < len(args)+1 {
		return queryStr
	}
	var b strings.Builder
	for i, arg := range args {
		b.WriteString(parts[i])
		switch v := arg.(type) {
		case string:
			b.WriteString("'" + strings.ReplaceAll(v, "'", "''") + "'")
		default:
			fmt.Fprintf(&b, "%v", v)
		}
	}
	b.WriteString(parts[len(args)])
	return b.String()
}

// bufferedRows holds rows data in memory and implements the Rows interface.
type bufferedRows struct {
	columns     []string
	columnTypes []*ColumnType
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

func (r *bufferedRows) ColumnTypes() ([]*ColumnType, error) {
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
	case *sql.NullString:
		if src == nil {
			*d = sql.NullString{}
		} else if s, ok := src.(string); ok {
			*d = sql.NullString{String: s, Valid: true}
		} else {
			*d = sql.NullString{String: fmt.Sprintf("%v", src), Valid: true}
		}
		return nil
	case *sql.NullInt64:
		if src == nil {
			*d = sql.NullInt64{}
		} else if i, ok := src.(int64); ok {
			*d = sql.NullInt64{Int64: i, Valid: true}
		}
		return nil
	case *sql.NullFloat64:
		if src == nil {
			*d = sql.NullFloat64{}
		} else if f, ok := src.(float64); ok {
			*d = sql.NullFloat64{Float64: f, Valid: true}
		}
		return nil
	case *sql.NullBool:
		if src == nil {
			*d = sql.NullBool{}
		} else if b, ok := src.(bool); ok {
			*d = sql.NullBool{Bool: b, Valid: true}
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
