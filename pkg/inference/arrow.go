package inference

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
)

var decimalType = regexp.MustCompile(`^(?:decimal|numeric|number)\((\d+),\s*(\d+)\)$`)

var oracleNumberPrecision = regexp.MustCompile(`^number\((\d+)\)$`)

//nolint:ireturn
func readRecord(ctx context.Context, conn any, sql string, maxRows int, columns []pipeline.Column) (arrow.RecordBatch, error) {
	q := &query.Query{Query: sql}
	if reader, ok := conn.(interface {
		SelectArrow(ctx context.Context, query *query.Query, maxRows int) (arrow.RecordBatch, error)
	}); ok {
		return reader.SelectArrow(ctx, q, maxRows)
	}
	reader, ok := conn.(interface {
		SelectWithSchema(ctx context.Context, query *query.Query) (*query.QueryResult, error)
	})
	if !ok {
		return nil, errors.New("connection does not support inference input queries")
	}
	input, err := reader.SelectWithSchema(ctx, q)
	if err != nil {
		return nil, err
	}
	if input != nil && len(input.Rows) > maxRows {
		return nil, fmt.Errorf("inference input exceeds max_rows (%d)", maxRows)
	}
	return recordFromQuery(input, columns)
}

// recordFromQuery is the fallback for connections without native Arrow reads.
// Unknown or lossy conversions fail rather than silently becoming strings.
//
//nolint:ireturn
func recordFromQuery(input *query.QueryResult, columns []pipeline.Column) (arrow.RecordBatch, error) {
	if input == nil {
		return nil, errors.New("inference input query returned no result")
	}
	fields := make([]arrow.Field, len(input.Columns))
	for i, name := range input.Columns {
		typeName := ""
		if i < len(input.ColumnTypes) {
			typeName = input.ColumnTypes[i]
		}
		// Explicit column declarations can supply precision/scale absent from drivers.
		for _, column := range columns {
			if column.Name == name && column.Type != "" {
				typeName = column.Type
				if strings.EqualFold(typeName, "decimal") || strings.EqualFold(typeName, "numeric") {
					if column.Precision != nil && column.Scale != nil {
						typeName = fmt.Sprintf("decimal(%d,%d)", *column.Precision, *column.Scale)
					}
				}
			}
		}
		dt, err := queryArrowType(typeName)
		if err != nil {
			return nil, fmt.Errorf("inference input column %q: %w", name, err)
		}
		fields[i] = arrow.Field{Name: name, Type: dt, Nullable: true}
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(fields, nil))
	defer builder.Release()
	for rowIndex, row := range input.Rows {
		if len(row) != len(fields) {
			return nil, fmt.Errorf("inference row %d has an invalid column count", rowIndex+1)
		}
		for i, value := range row {
			if err := appendArrowValue(builder.Field(i), value); err != nil {
				// Values can contain private source data; report only the location and type.
				return nil, fmt.Errorf("cannot losslessly encode inference input row %d column %q as %s", rowIndex+1, fields[i].Name, fields[i].Type)
			}
		}
	}
	return builder.NewRecordBatch(), nil
}

//nolint:ireturn
func queryArrowType(name string) (arrow.DataType, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if parts := decimalType.FindStringSubmatch(name); parts != nil {
		precision, _ := strconv.Atoi(parts[1])
		scale, _ := strconv.Atoi(parts[2])
		if precision > 0 && precision <= 76 && scale <= precision {
			if precision <= 38 {
				return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, nil //nolint:gosec
			}
			return &arrow.Decimal256Type{Precision: int32(precision), Scale: int32(scale)}, nil //nolint:gosec
		}
	}
	// Oracle NUMBER with a single precision argument, e.g. NUMBER(10).
	if parts := oracleNumberPrecision.FindStringSubmatch(name); parts != nil {
		precision, _ := strconv.Atoi(parts[1])
		if precision > 0 && precision <= 76 {
			if precision <= 18 {
				return arrow.PrimitiveTypes.Int64, nil
			}
			if precision <= 38 {
				return &arrow.Decimal128Type{Precision: int32(precision)}, nil //nolint:gosec
			}
			return &arrow.Decimal256Type{Precision: int32(precision)}, nil //nolint:gosec
		}
	}
	// Oracle DatabaseTypeName values, including optional length qualifiers
	// such as VARCHAR2(100) or NVARCHAR2(50 CHAR).
	for _, prefix := range []string{"varchar2", "nvarchar2", "nchar", "clob", "nclob"} {
		if name == prefix || strings.HasPrefix(name, prefix+"(") {
			return arrow.BinaryTypes.String, nil
		}
	}
	// Bare Oracle NUMBER is the default integer column type (e.g. age NUMBER).
	// Fractional numbers require an explicit columns.type decimal(p,s) declaration.
	if name == "number" {
		return arrow.PrimitiveTypes.Int64, nil
	}
	switch name {
	case "bool", "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	case "tinyint", "int8":
		// PostgreSQL int8 means bigint, unlike Arrow's int8. Widen both losslessly.
		return arrow.PrimitiveTypes.Int64, nil
	case "smallint", "int2", "int16":
		return arrow.PrimitiveTypes.Int16, nil
	case "integer", "int", "int4", "int32":
		return arrow.PrimitiveTypes.Int32, nil
	case "bigint", "int64", "long":
		return arrow.PrimitiveTypes.Int64, nil
	case "uint8", "uint16", "uint32", "uint64", "utinyint", "usmallint", "uinteger", "ubigint":
		return arrow.PrimitiveTypes.Uint64, nil
	case "float", "real", "float4", "float32":
		return arrow.PrimitiveTypes.Float32, nil
	case "double", "double precision", "float8", "float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "string", "text", "varchar", "char", "character varying", "nvarchar", "utf8",
		"varchar2", "nvarchar2", "nchar", "clob", "nclob":
		return arrow.BinaryTypes.String, nil
	case "binary", "varbinary", "blob", "bytea", "bytes":
		return arrow.BinaryTypes.Binary, nil
	case "date", "date32":
		return arrow.FixedWidthTypes.Date32, nil
	case "time", "time without time zone":
		return &arrow.Time64Type{Unit: arrow.Nanosecond}, nil
	case "timestamp", "datetime", "timestamp_ntz", "timestamp without time zone":
		return &arrow.TimestampType{Unit: arrow.Nanosecond}, nil
	case "timestamptz", "timestamp_tz", "timestamp_ltz", "timestamp with time zone":
		return &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, nil
	}
	return nil, fmt.Errorf("unsupported or incomplete type %q; declare an exact supported columns.type or use a native Arrow connection", name)
}

func appendArrowValue(builder array.Builder, value any) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}
	switch b := builder.(type) {
	case *array.StringBuilder:
		switch v := value.(type) {
		case string:
			b.Append(v)
		case []byte:
			b.Append(string(v))
		default:
			return errors.New("not a string")
		}
		return nil
	case *array.BinaryBuilder:
		v, ok := value.([]byte)
		if !ok {
			return errors.New("not binary")
		}
		b.Append(v)
		return nil
	}
	text := fmt.Sprint(value)
	if bytes, ok := value.([]byte); ok {
		text = string(bytes)
	}
	if dt, ok := builder.Type().(arrow.DecimalType); ok {
		switch v := value.(type) {
		case float32, float64:
			return errors.New("decimal precision already lost to floating point")
		case decimal128.Num:
			text = v.ToString(dt.GetScale())
		case decimal256.Num:
			text = v.ToString(dt.GetScale())
		case *big.Rat:
			text = v.RatString()
		}
		exact, ok := new(big.Rat).SetString(text)
		if !ok {
			return errors.New("invalid decimal")
		}
		text = exact.FloatString(int(dt.GetScale()))
		roundTrip, ok := new(big.Rat).SetString(text)
		if !ok || roundTrip.Cmp(exact) != 0 {
			return errors.New("decimal scale would round value")
		}
	}
	if t, ok := value.(time.Time); ok {
		switch dt := builder.Type().(type) {
		case *arrow.TimestampType:
			if dt.TimeZone == "" {
				text = t.Format("2006-01-02T15:04:05.999999999")
			} else {
				text = t.Format(time.RFC3339Nano)
			}
		case *arrow.Date32Type:
			text = t.Format("2006-01-02")
		case *arrow.Time64Type:
			text = t.Format("15:04:05.999999999")
		}
	}
	if text == "(null)" {
		return errors.New("not a valid typed value")
	}
	return builder.AppendValueFromString(text)
}

// writeArrow writes IPC file format, the same mmap:// contract as Python assets.
func writeArrow(dst io.WriteSeeker, input arrow.RecordBatch, outputColumn string, results []string) error {
	if int64(len(results)) != input.NumRows() {
		return errors.New("inference result count does not match input")
	}
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(results, nil)
	output := builder.NewStringArray()
	defer output.Release()
	fields := append(input.Schema().Fields(), arrow.Field{Name: outputColumn, Type: arrow.BinaryTypes.String, Nullable: true})
	columns := append(append([]arrow.Array(nil), input.Columns()...), output)
	schema := arrow.NewSchema(fields, nil)
	record := array.NewRecordBatch(schema, columns, input.NumRows())
	defer record.Release()
	writer, err := ipc.NewFileWriter(dst, ipc.WithSchema(schema))
	if err != nil {
		return err
	}
	if err := writer.Write(record); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}
