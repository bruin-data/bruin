package diff

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// CommonDataType represents the normalized data type categories.
type CommonDataType string

const (
	CommonTypeNumeric  CommonDataType = "numeric"
	CommonTypeString   CommonDataType = "string"
	CommonTypeBoolean  CommonDataType = "boolean"
	CommonTypeDateTime CommonDataType = "datetime"
	CommonTypeBinary   CommonDataType = "binary"
	CommonTypeJSON     CommonDataType = "json"
	CommonTypeUnknown  CommonDataType = "unknown"
)

// TypeMapping defines the interface for mapping database-specific types to common types.
type TypeMapping interface {
	MapType(databaseType string) CommonDataType
	IsNumeric(databaseType string) bool
	IsString(databaseType string) bool
	IsBoolean(databaseType string) bool
	IsDateTime(databaseType string) bool
}

// DatabaseTypeMapper provides a base implementation for type mapping.
type DatabaseTypeMapper struct {
	numericTypes  map[string]bool
	stringTypes   map[string]bool
	booleanTypes  map[string]bool
	datetimeTypes map[string]bool
	binaryTypes   map[string]bool
	jsonTypes     map[string]bool
}

func NewDatabaseTypeMapper() *DatabaseTypeMapper {
	return &DatabaseTypeMapper{
		numericTypes:  make(map[string]bool),
		stringTypes:   make(map[string]bool),
		booleanTypes:  make(map[string]bool),
		datetimeTypes: make(map[string]bool),
		binaryTypes:   make(map[string]bool),
		jsonTypes:     make(map[string]bool),
	}
}

func (m *DatabaseTypeMapper) AddNumericTypes(types ...string) {
	for _, t := range types {
		m.numericTypes[strings.ToLower(t)] = true
	}
}

func (m *DatabaseTypeMapper) AddStringTypes(types ...string) {
	for _, t := range types {
		m.stringTypes[strings.ToLower(t)] = true
	}
}

func (m *DatabaseTypeMapper) AddBooleanTypes(types ...string) {
	for _, t := range types {
		m.booleanTypes[strings.ToLower(t)] = true
	}
}

func (m *DatabaseTypeMapper) AddDateTimeTypes(types ...string) {
	for _, t := range types {
		m.datetimeTypes[strings.ToLower(t)] = true
	}
}

func (m *DatabaseTypeMapper) AddBinaryTypes(types ...string) {
	for _, t := range types {
		m.binaryTypes[strings.ToLower(t)] = true
	}
}

func (m *DatabaseTypeMapper) AddJSONTypes(types ...string) {
	for _, t := range types {
		m.jsonTypes[strings.ToLower(t)] = true
	}
}

func (m *DatabaseTypeMapper) MapType(databaseType string) CommonDataType {
	lowerType := strings.ToLower(databaseType)

	// First try exact match
	if m.numericTypes[lowerType] {
		return CommonTypeNumeric
	}
	if m.stringTypes[lowerType] {
		return CommonTypeString
	}
	if m.booleanTypes[lowerType] {
		return CommonTypeBoolean
	}
	if m.datetimeTypes[lowerType] {
		return CommonTypeDateTime
	}
	if m.binaryTypes[lowerType] {
		return CommonTypeBinary
	}
	if m.jsonTypes[lowerType] {
		return CommonTypeJSON
	}

	// If no exact match, try prefix matching for parametrized types
	// Extract base type by removing everything after '(' or whitespace
	baseType := lowerType
	if parenIndex := strings.Index(baseType, "("); parenIndex != -1 {
		baseType = baseType[:parenIndex]
	}
	if spaceIndex := strings.Index(baseType, " "); spaceIndex != -1 {
		baseType = baseType[:spaceIndex]
	}

	// Try prefix match
	if m.numericTypes[baseType] {
		return CommonTypeNumeric
	}
	if m.stringTypes[baseType] {
		return CommonTypeString
	}
	if m.booleanTypes[baseType] {
		return CommonTypeBoolean
	}
	if m.datetimeTypes[baseType] {
		return CommonTypeDateTime
	}
	if m.binaryTypes[baseType] {
		return CommonTypeBinary
	}
	if m.jsonTypes[baseType] {
		return CommonTypeJSON
	}

	return CommonTypeUnknown
}

func (m *DatabaseTypeMapper) IsNumeric(databaseType string) bool {
	return m.numericTypes[strings.ToLower(databaseType)]
}

func (m *DatabaseTypeMapper) IsString(databaseType string) bool {
	return m.stringTypes[strings.ToLower(databaseType)]
}

func (m *DatabaseTypeMapper) IsBoolean(databaseType string) bool {
	return m.booleanTypes[strings.ToLower(databaseType)]
}

func (m *DatabaseTypeMapper) IsDateTime(databaseType string) bool {
	return m.datetimeTypes[strings.ToLower(databaseType)]
}

// NewDuckDBTypeMapper provides DuckDB-specific type mapping.
func NewDuckDBTypeMapper() *DatabaseTypeMapper {
	mapper := NewDatabaseTypeMapper()

	// Numeric types in DuckDB (base types only, prefix matching handles parametrized versions)
	mapper.AddNumericTypes(
		"integer", "int", "int4", "signed",
		"bigint", "int8", "long",
		"smallint", "int2", "short",
		"tinyint", "int1",
		"ubigint", "uint8",
		"uinteger", "uint4", "uint", "usmallint", "uint2",
		"utinyint", "uint1",
		"double", "float8", "numeric", "decimal",
		"real", "float4", "float",
		"hugeint", "int16",
		"uhugeint", "uint16",
	)

	// String types in DuckDB (base types only)
	mapper.AddStringTypes(
		"varchar", "char", "bpchar", "text", "string",
	)

	// Boolean types in DuckDB
	mapper.AddBooleanTypes(
		"boolean", "bool", "logical",
	)

	// DateTime types in DuckDB
	mapper.AddDateTimeTypes(
		"date", "time", "timetz", "timestamp", "timestamptz", "datetime",
		"timestamp with time zone", "timestamp without time zone",
		"time with time zone", "time without time zone",
		"interval",
	)

	// Binary types in DuckDB
	mapper.AddBinaryTypes(
		"blob", "bytea", "binary", "varbinary",
	)

	// JSON types in DuckDB
	mapper.AddJSONTypes(
		"json",
	)

	return mapper
}

// NewBigQueryTypeMapper provides BigQuery-specific type mapping.
func NewBigQueryTypeMapper() *DatabaseTypeMapper {
	mapper := NewDatabaseTypeMapper()

	// Numeric types in BigQuery (base types only, case-insensitive with prefix matching)
	mapper.AddNumericTypes(
		"int64", "INT64", "integer", "INTEGER", "int", "INT",
		"smallint", "SMALLINT", "bigint", "BIGINT", "tinyint", "TINYINT",
		"float64", "FLOAT64", "float", "FLOAT", "numeric", "NUMERIC",
		"decimal", "DECIMAL", "bignumeric", "BIGNUMERIC", "bigdecimal", "BIGDECIMAL",
	)

	// String types in BigQuery (base types only)
	mapper.AddStringTypes(
		"string", "STRING", "bytes", "BYTES",
	)

	// Boolean types in BigQuery
	mapper.AddBooleanTypes(
		"bool", "BOOL", "boolean", "BOOLEAN",
	)

	// DateTime types in BigQuery
	mapper.AddDateTimeTypes(
		"date", "DATE", "datetime", "DATETIME", "time", "TIME",
		"timestamp", "TIMESTAMP",
	)

	// JSON types in BigQuery
	mapper.AddJSONTypes(
		"json", "JSON",
	)

	// Note: BigQuery GEOGRAPHY, ARRAY, and STRUCT types are not commonly comparable
	// across databases, so they'll be mapped to CommonTypeUnknown by default

	return mapper
}

// NewPostgresTypeMapper provides PostgreSQL-specific type mapping.
func NewPostgresTypeMapper() *DatabaseTypeMapper {
	mapper := NewDatabaseTypeMapper()

	// Numeric types in PostgreSQL
	mapper.AddNumericTypes(
		"smallint", "int2",
		"integer", "int", "int4",
		"bigint", "int8",
		"decimal", "numeric",
		"real", "float4",
		"double precision", "float8",
		"smallserial", "serial2",
		"serial", "serial4",
		"bigserial", "serial8",
		"money",
	)

	// String types in PostgreSQL
	mapper.AddStringTypes(
		"character varying", "varchar",
		"character", "char", "bpchar",
		"text",
		"name",
		"cidr", "inet", "macaddr", "macaddr8",
		"uuid",
	)

	// Boolean types in PostgreSQL
	mapper.AddBooleanTypes(
		"boolean", "bool",
	)

	// DateTime types in PostgreSQL
	mapper.AddDateTimeTypes(
		"timestamp", "timestamptz",
		"timestamp without time zone",
		"timestamp with time zone",
		"date",
		"time", "timetz",
		"time without time zone",
		"time with time zone",
		"interval",
	)

	// Binary types in PostgreSQL
	mapper.AddBinaryTypes(
		"bytea",
		"bit", "bit varying", "varbit",
	)

	// JSON types in PostgreSQL
	mapper.AddJSONTypes(
		"json", "jsonb",
	)

	return mapper
}

// NewSnowflakeTypeMapper provides Snowflake-specific type mapping.
func NewSnowflakeTypeMapper() *DatabaseTypeMapper {
	mapper := NewDatabaseTypeMapper()

	// Numeric types in Snowflake
	mapper.AddNumericTypes(
		"number", "decimal", "numeric",
		"int", "integer", "bigint", "smallint", "tinyint",
		"byteint",
		"float", "float4", "float8",
		"double", "double precision",
		"real",
	)

	// String types in Snowflake
	mapper.AddStringTypes(
		"varchar", "char", "character",
		"string", "text",
	)

	// Boolean types in Snowflake
	mapper.AddBooleanTypes(
		"boolean", "bool",
	)

	// DateTime types in Snowflake
	mapper.AddDateTimeTypes(
		"date",
		"datetime",
		"time",
		"timestamp", "timestamp_ltz", "timestamp_ntz", "timestamp_tz",
	)

	// Binary types in Snowflake
	mapper.AddBinaryTypes(
		"binary", "varbinary",
	)

	// JSON types in Snowflake
	mapper.AddJSONTypes(
		"variant", "object", "array",
	)

	return mapper
}

type Table struct {
	Name    string
	Columns []*Column
}

type Column struct {
	Name           string
	Type           string         // Original database-specific type (e.g., "INTEGER", "VARCHAR(255)")
	NormalizedType CommonDataType // Normalized common type (e.g., "numeric", "string")
	Nullable       bool
	PrimaryKey     bool
	Unique         bool

	Stats ColumnStatistics
}

type Type struct {
	Name      string
	Size      int
	Precision int
	Scale     int
}

type TableSummaryResult struct {
	RowCount int64
	Table    *Table
}

// ColumnStatistics is an interface for different types of column statistics.
type ColumnStatistics interface {
	Type() string
}

// NumericalStatistics holds statistics for numerical columns.
type NumericalStatistics struct {
	Min       *float64 // pointer to handle NULL values
	Max       *float64
	Avg       *float64
	Sum       *float64
	Count     int64
	NullCount int64
	StdDev    *float64
}

func (ns *NumericalStatistics) Type() string {
	return "numerical"
}

// StringStatistics holds statistics for string/text columns.
type StringStatistics struct {
	DistinctCount int64
	MaxLength     int
	MinLength     int
	AvgLength     float64
	Count         int64
	NullCount     int64
	EmptyCount    int64
	MostCommon    map[string]int64 // value -> frequency
	TopNDistinct  []string         // top N most common values
}

func (ss *StringStatistics) Type() string {
	return "string"
}

type BooleanStatistics struct {
	TrueCount  int64
	FalseCount int64
	NullCount  int64
	Count      int64
}

func (bs *BooleanStatistics) Type() string {
	return "boolean"
}

type DateTimeStatistics struct {
	EarliestDate *time.Time // Earliest datetime value or nil
	LatestDate   *time.Time // Latest datetime value or nil
	Count        int64
	NullCount    int64
	UniqueCount  int64
}

func (dts *DateTimeStatistics) Type() string {
	return "datetime"
}

type JSONStatistics struct {
	Count     int64
	NullCount int64
}

func (js *JSONStatistics) Type() string {
	return "json"
}

type UnknownStatistics struct{}

func (us *UnknownStatistics) Type() string {
	return "unknown"
}

// ParseDateTime converts various datetime representations to time.Time.
// This is a shared utility function used across all database implementations.
func ParseDateTime(value interface{}) (*time.Time, error) {
	switch val := value.(type) {
	case time.Time:
		return &val, nil
	case string:
		if val == "" {
			return nil, errors.New("empty string")
		}
		// Try common datetime formats
		formats := []string{
			time.RFC3339,                    // 2006-01-02T15:04:05Z07:00
			time.RFC3339Nano,                // 2006-01-02T15:04:05.999999999Z07:00
			"2006-01-02T15:04:05",           // RFC3339 without timezone
			"2006-01-02 15:04:05 -0700 MST", // BigQuery format with timezone
			"2006-01-02 15:04:05",           // Standard datetime
			"2006-01-02 15:04:05.999999",    // With microseconds
			"2006-01-02",                    // Date only
			"15:04:05",                      // Time only
		}

		for _, format := range formats {
			if parsedTime, err := time.Parse(format, val); err == nil {
				return &parsedTime, nil
			}
		}
		return nil, fmt.Errorf("unable to parse datetime string: %s", val)
	default:
		// Try converting to string and then parsing
		if str := fmt.Sprintf("%v", val); str != "" && str != "<nil>" {
			return ParseDateTime(str)
		}
		return nil, fmt.Errorf("unsupported datetime type: %T", val)
	}
}
