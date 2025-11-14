//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// DriverInstaller is an interface for installing the DuckDB ADBC driver.
type DriverInstaller interface {
	InstallDuckDBDriver(ctx context.Context) error
}

var (
	driverInstallOnce sync.Once
	errDriverInstall  error
	driverInstaller   DriverInstaller // Set by python package to avoid circular dependency
)

// SetDriverInstaller sets the driver installer to use for installing the ADBC driver.
// This is called by the python package to avoid circular dependencies.
func SetDriverInstaller(installer DriverInstaller) {
	driverInstaller = installer
}

// ensureDriverInstalled ensures the DuckDB ADBC driver is installed.
// It lazily installs the driver on first connection attempt.
func ensureDriverInstalled(ctx context.Context) error {
	driverInstallOnce.Do(func() {
		// First, try to load the driver to see if it's already installed
		var drv drivermgr.Driver
		db, err := drv.NewDatabase(map[string]string{
			"driver": "duckdb",
			"path":   ":memory:",
		})
		if err == nil {
			// Driver is already installed
			_ = db.Close()
			return
		}

		// Driver not found, need to install it
		if driverInstaller == nil {
			errDriverInstall = errors.New("driver installer not set - please ensure the python package is initialized")
			return
		}

		if err := driverInstaller.InstallDuckDBDriver(ctx); err != nil {
			errDriverInstall = fmt.Errorf("failed to install duckdb driver: %w", err)
			return
		}
	})

	return errDriverInstall
}

type ADBCConnection struct {
	config DuckDBConfig
}

func NewEphemeralConnection(c DuckDBConfig) (*ADBCConnection, error) {
	return &ADBCConnection{config: c}, nil
}

//nolint:ireturn // Returning ADBC interface types by design
func (e *ADBCConnection) getConnection(ctx context.Context) (adbc.Connection, adbc.Database, error) {
	// Ensure driver is installed
	if err := ensureDriverInstalled(ctx); err != nil {
		return nil, nil, err
	}

	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver": "duckdb",
		"path":   e.config.ToDBConnectionURI(),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create ADBC database: %w", err)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("failed to open ADBC connection: %w", err)
	}

	return conn, db, nil
}

// QueryContext implements the connection interface.
//
//nolint:ireturn // Returning interface type is by design for abstraction
func (e *ADBCConnection) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	conn, db, err := e.getConnection(ctx)
	if err != nil {
		return nil, err
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}

	// Note: ADBC parameterized queries work differently than database/sql
	// They require RecordBatch binding, not variadic args
	// For now, we don't support parameterized queries
	if len(args) > 0 {
		stmt.Close()
		_ = conn.Close()
		_ = db.Close()
		return nil, errors.New("parameterized queries not yet supported with ADBC")
	}

	err = stmt.SetSqlQuery(query)
	if err != nil {
		stmt.Close()
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("failed to set SQL query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return newADBCRows(reader, stmt, conn, db), nil
}

// ExecContext implements the connection interface.
func (e *ADBCConnection) ExecContext(ctx context.Context, sqlQuery string, arguments ...any) (sql.Result, error) {
	conn, db, err := e.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
		_ = db.Close()
	}()

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}
	defer stmt.Close()

	if len(arguments) > 0 {
		return nil, errors.New("parameterized queries not yet supported with ADBC")
	}

	err = stmt.SetSqlQuery(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to set SQL query: %w", err)
	}

	rowsAffected, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute update: %w", err)
	}

	return &basicResult{rowsAffected: rowsAffected}, nil
}

// QueryRowContext implements the connection interface.
//
//nolint:ireturn // Returning interface type is by design for abstraction
func (e *ADBCConnection) QueryRowContext(ctx context.Context, query string, args ...any) Row {
	conn, db, err := e.getConnection(ctx)
	if err != nil {
		return &adbcRow{err: err}
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		_ = conn.Close()
		_ = db.Close()
		return &adbcRow{err: err}
	}

	if len(args) > 0 {
		stmt.Close()
		_ = conn.Close()
		_ = db.Close()
		return &adbcRow{err: errors.New("parameterized queries not yet supported with ADBC")}
	}

	err = stmt.SetSqlQuery(query)
	if err != nil {
		stmt.Close()
		_ = conn.Close()
		_ = db.Close()
		return &adbcRow{err: err}
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		_ = conn.Close()
		_ = db.Close()
		return &adbcRow{err: err}
	}

	// Read the first record
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			reader.Release()
			stmt.Close()
			_ = conn.Close()
			_ = db.Close()
			return &adbcRow{err: err}
		}
		reader.Release()
		stmt.Close()
		_ = conn.Close()
		_ = db.Close()
		return &adbcRow{err: sql.ErrNoRows}
	}

	//nolint:staticcheck // Using deprecated method for Arrow compatibility
	record := reader.Record()
	if record.NumRows() == 0 {
		reader.Release()
		stmt.Close()
		_ = conn.Close()
		_ = db.Close()
		return &adbcRow{err: sql.ErrNoRows}
	}

	// Materialize the first row
	values := make([]interface{}, record.NumCols())
	for i := range int(record.NumCols()) {
		col := record.Column(i)
		values[i] = getArrowValue(col, 0)
	}

	reader.Release()
	stmt.Close()
	_ = conn.Close()
	_ = db.Close()

	return &adbcRow{values: values}
}

type basicResult struct {
	rowsAffected int64
}

func (r *basicResult) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId not supported")
}

func (r *basicResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// adbcRows implements the Rows interface.
type adbcRows struct {
	reader        array.RecordReader
	stmt          adbc.Statement
	conn          adbc.Connection
	db            adbc.Database
	currentRecord arrow.Record   //nolint:staticcheck // Using deprecated type for Arrow compatibility
	allRecords    []arrow.Record //nolint:staticcheck // Using deprecated type for Arrow compatibility
	currentRow    int64
	closed        bool
	err           error
}

func newADBCRows(reader array.RecordReader, stmt adbc.Statement, conn adbc.Connection, db adbc.Database) *adbcRows {
	return &adbcRows{
		reader:     reader,
		stmt:       stmt,
		conn:       conn,
		db:         db,
		currentRow: -1,
	}
}

func (r *adbcRows) Next() bool {
	if r.closed {
		return false
	}

	r.currentRow++

	if r.currentRecord == nil || r.currentRow >= r.currentRecord.NumRows() {
		// Don't release the previous record yet - keep it in allRecords
		// This prevents premature memory cleanup

		if !r.reader.Next() {
			if err := r.reader.Err(); err != nil {
				r.err = err
			}
			return false
		}

		//nolint:staticcheck // Using deprecated method for Arrow compatibility
		r.currentRecord = r.reader.Record()
		r.currentRecord.Retain()
		r.allRecords = append(r.allRecords, r.currentRecord)
		r.currentRow = 0
	}

	return true
}

func (r *adbcRows) Scan(dest ...interface{}) error {
	if r.currentRecord == nil {
		return errors.New("no current record")
	}

	if len(dest) != int(r.currentRecord.NumCols()) {
		return fmt.Errorf("expected %d destination arguments, got %d", r.currentRecord.NumCols(), len(dest))
	}

	for i := range int(r.currentRecord.NumCols()) {
		col := r.currentRecord.Column(i)
		value := getArrowValue(col, int(r.currentRow))

		switch d := dest[i].(type) {
		case *interface{}:
			*d = value
		case *string:
			if value == nil {
				*d = ""
			} else if s, ok := value.(string); ok {
				*d = s
			} else {
				*d = fmt.Sprintf("%v", value)
			}
		case *int, *int8, *int16, *int32, *int64:
			if value == nil {
				return fmt.Errorf("cannot scan NULL into int type at column %d", i)
			}
			// Type assertion and conversion
			switch v := value.(type) {
			case int64:
				switch d := dest[i].(type) {
				case *int:
					*d = int(v)
				case *int8:
					//nolint:gosec // Intentional narrowing conversion, caller responsible for range
					*d = int8(v)
				case *int16:
					//nolint:gosec // Intentional narrowing conversion, caller responsible for range
					*d = int16(v)
				case *int32:
					//nolint:gosec // Intentional narrowing conversion, caller responsible for range
					*d = int32(v)
				case *int64:
					*d = v
				}
			default:
				return fmt.Errorf("cannot convert %T to int at column %d", value, i)
			}
		case *float32, *float64:
			if value == nil {
				return fmt.Errorf("cannot scan NULL into float type at column %d", i)
			}
			switch v := value.(type) {
			case float64:
				switch d := dest[i].(type) {
				case *float32:
					*d = float32(v)
				case *float64:
					*d = v
				}
			default:
				return fmt.Errorf("cannot convert %T to float at column %d", value, i)
			}
		case *bool:
			if value == nil {
				*d = false
			} else if b, ok := value.(bool); ok {
				*d = b
			} else {
				return fmt.Errorf("cannot convert %T to bool at column %d", value, i)
			}
		case sql.Scanner:
			// Support sql.Scanner interface for custom types
			if err := d.Scan(value); err != nil {
				return fmt.Errorf("error scanning column %d: %w", i, err)
			}
		default:
			return fmt.Errorf("unsupported destination type for column %d: %T", i, dest[i])
		}
	}

	return nil
}

func (r *adbcRows) Columns() ([]string, error) {
	schema := r.reader.Schema()
	columns := make([]string, schema.NumFields())
	for i := range schema.NumFields() {
		columns[i] = schema.Field(i).Name
	}
	return columns, nil
}

func (r *adbcRows) ColumnTypes() ([]ColumnType, error) {
	schema := r.reader.Schema()
	types := make([]ColumnType, schema.NumFields())
	for i := range schema.NumFields() {
		field := schema.Field(i)
		// Map Arrow type names to SQL type names for compatibility
		sqlType := arrowTypeToSQLType(field.Type.String())
		types[i] = &adbcColumnType{name: field.Name, dbType: sqlType}
	}
	return types, nil
}

// arrowTypeToSQLType maps Arrow type names to SQL type names.
func arrowTypeToSQLType(arrowType string) string {
	switch arrowType {
	case "int8", "int16", "int32", "int64":
		return "INTEGER"
	case "uint8", "uint16", "uint32", "uint64":
		return "INTEGER"
	case "float32", "float64":
		return "DOUBLE"
	case "utf8", "large_utf8":
		return "VARCHAR"
	case "binary", "large_binary":
		return "BLOB"
	case "bool":
		return "BOOLEAN"
	case "date32", "date64":
		return "DATE"
	case "timestamp":
		return "TIMESTAMP"
	default:
		// For decimal types, uppercase and fix spacing: "decimal(5, 2)" -> "DECIMAL(5,2)"
		if len(arrowType) >= 7 && arrowType[:7] == "decimal" {
			// Replace "decimal" with "DECIMAL" and remove spaces after commas
			result := "DECIMAL" + arrowType[7:]
			// Remove spaces after commas
			result = strings.ReplaceAll(result, ", ", ",")
			return result
		}
		// For timestamp with unit, just return "TIMESTAMP": "timestamp[us]" -> "TIMESTAMP"
		if len(arrowType) >= 9 && arrowType[:9] == "timestamp" {
			return "TIMESTAMP"
		}
		// For other types, keep the original
		return arrowType
	}
}

func (r *adbcRows) Err() error {
	return r.err
}

func (r *adbcRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release all retained records
	for _, rec := range r.allRecords {
		rec.Release()
	}
	r.allRecords = nil

	r.reader.Release()
	r.stmt.Close()
	_ = r.conn.Close()
	_ = r.db.Close()
	return nil
}

// adbcRow implements the Row interface.
type adbcRow struct {
	values []interface{}
	err    error
}

func (r *adbcRow) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}

	if len(dest) != len(r.values) {
		return fmt.Errorf("expected %d destination arguments, got %d", len(r.values), len(dest))
	}

	for i, value := range r.values {
		switch d := dest[i].(type) {
		case *interface{}:
			*d = value
		case *string:
			if value == nil {
				*d = ""
			} else if s, ok := value.(string); ok {
				*d = s
			} else {
				*d = fmt.Sprintf("%v", value)
			}
		case *int, *int8, *int16, *int32, *int64:
			if value == nil {
				return fmt.Errorf("cannot scan NULL into int type at column %d", i)
			}
			switch v := value.(type) {
			case int64:
				switch d := dest[i].(type) {
				case *int:
					*d = int(v)
				case *int8:
					//nolint:gosec // Intentional narrowing conversion, caller responsible for range
					*d = int8(v)
				case *int16:
					//nolint:gosec // Intentional narrowing conversion, caller responsible for range
					*d = int16(v)
				case *int32:
					//nolint:gosec // Intentional narrowing conversion, caller responsible for range
					*d = int32(v)
				case *int64:
					*d = v
				}
			default:
				return fmt.Errorf("cannot convert %T to int at column %d", value, i)
			}
		case *float32, *float64:
			if value == nil {
				return fmt.Errorf("cannot scan NULL into float type at column %d", i)
			}
			switch v := value.(type) {
			case float64:
				switch d := dest[i].(type) {
				case *float32:
					*d = float32(v)
				case *float64:
					*d = v
				}
			default:
				return fmt.Errorf("cannot convert %T to float at column %d", value, i)
			}
		case *bool:
			if value == nil {
				*d = false
			} else if b, ok := value.(bool); ok {
				*d = b
			} else {
				return fmt.Errorf("cannot convert %T to bool at column %d", value, i)
			}
		case sql.Scanner:
			if err := d.Scan(value); err != nil {
				return fmt.Errorf("error scanning column %d: %w", i, err)
			}
		default:
			return fmt.Errorf("unsupported destination type for column %d: %T", i, dest[i])
		}
	}

	return nil
}

// adbcColumnType implements the ColumnType interface.
type adbcColumnType struct {
	name   string
	dbType string
}

func (c *adbcColumnType) DatabaseTypeName() string {
	return c.dbType
}

// getArrowValue extracts a value from an Arrow array at the given index.
func getArrowValue(arr arrow.Array, idx int) interface{} {
	if arr.IsNull(idx) {
		return nil
	}

	switch arr := arr.(type) {
	case *array.Int8:
		return int64(arr.Value(idx))
	case *array.Int16:
		return int64(arr.Value(idx))
	case *array.Int32:
		return int64(arr.Value(idx))
	case *array.Int64:
		return arr.Value(idx)
	case *array.Uint8:
		return uint64(arr.Value(idx))
	case *array.Uint16:
		return uint64(arr.Value(idx))
	case *array.Uint32:
		return uint64(arr.Value(idx))
	case *array.Uint64:
		return arr.Value(idx)
	case *array.Float32:
		return float64(arr.Value(idx))
	case *array.Float64:
		return arr.Value(idx)
	case *array.String:
		// Explicitly copy the string to ensure it's not a view into Arrow memory
		s := arr.Value(idx)
		// Force a copy by converting to []byte and back
		return string(append([]byte(nil), []byte(s)...))
	case *array.LargeString:
		// Explicitly copy the string to ensure it's not a view into Arrow memory
		s := arr.Value(idx)
		// Force a copy by converting to []byte and back
		return string(append([]byte(nil), []byte(s)...))
	case *array.Binary:
		return string(arr.Value(idx))
	case *array.LargeBinary:
		return string(arr.Value(idx))
	case *array.FixedSizeBinary:
		return string(arr.Value(idx))
	case *array.Boolean:
		return arr.Value(idx)
	case *array.Date32:
		return arr.Value(idx).ToTime()
	case *array.Date64:
		return arr.Value(idx).ToTime()
	case *array.Timestamp:
		return arr.Value(idx).ToTime(arr.DataType().(*arrow.TimestampType).Unit)
	case *array.Time32:
		return arr.Value(idx)
	case *array.Time64:
		return arr.Value(idx)
	case *array.Decimal128:
		val := arr.Value(idx)
		dt := arr.DataType().(*arrow.Decimal128Type)
		f := val.ToFloat64(dt.Scale)
		// Format with exact precision to avoid floating-point representation issues
		// For scale 2, format as "%.2f"
		formatted := fmt.Sprintf("%.*f", dt.Scale, f)
		// Parse back to float64 to ensure consistent type
		result, _ := strconv.ParseFloat(formatted, 64)
		return result
	case *array.Decimal256:
		val := arr.Value(idx)
		dt := arr.DataType().(*arrow.Decimal256Type)
		f := val.ToFloat64(dt.Scale)
		// Format with exact precision to avoid floating-point representation issues
		formatted := fmt.Sprintf("%.*f", dt.Scale, f)
		// Parse back to float64 to ensure consistent type
		result, _ := strconv.ParseFloat(formatted, 64)
		return result
	default:
		// For any unhandled type, use the string representation
		return arr.ValueStr(idx)
	}
}
