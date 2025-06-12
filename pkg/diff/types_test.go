package diff

import (
	"testing"
)

func TestDatabaseTypeMapper_MapType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mapper         *DatabaseTypeMapper
		inputType      string
		expectedResult CommonDataType
	}{
		// Basic exact matches
		{
			name:           "exact numeric match",
			mapper:         createTestMapper(),
			inputType:      "integer",
			expectedResult: CommonTypeNumeric,
		},
		{
			name:           "exact string match",
			mapper:         createTestMapper(),
			inputType:      "varchar",
			expectedResult: CommonTypeString,
		},
		{
			name:           "exact boolean match",
			mapper:         createTestMapper(),
			inputType:      "boolean",
			expectedResult: CommonTypeBoolean,
		},
		{
			name:           "exact datetime match",
			mapper:         createTestMapper(),
			inputType:      "timestamp",
			expectedResult: CommonTypeDateTime,
		},

		// Case insensitive exact matches
		{
			name:           "case insensitive numeric",
			mapper:         createTestMapper(),
			inputType:      "INTEGER",
			expectedResult: CommonTypeNumeric,
		},
		{
			name:           "case insensitive string",
			mapper:         createTestMapper(),
			inputType:      "VARCHAR",
			expectedResult: CommonTypeString,
		},

		// Parametrized types with parentheses
		{
			name:           "numeric with precision/scale",
			mapper:         createTestMapper(),
			inputType:      "numeric(10,2)",
			expectedResult: CommonTypeNumeric,
		},
		{
			name:           "varchar with length",
			mapper:         createTestMapper(),
			inputType:      "varchar(255)",
			expectedResult: CommonTypeString,
		},
		{
			name:           "decimal with parameters",
			mapper:         createTestMapper(),
			inputType:      "decimal(18,4)",
			expectedResult: CommonTypeNumeric,
		},
		{
			name:           "timestamp with precision",
			mapper:         createTestMapper(),
			inputType:      "timestamp(6)",
			expectedResult: CommonTypeDateTime,
		},

		// Case insensitive parametrized types
		{
			name:           "uppercase numeric with parameters",
			mapper:         createTestMapper(),
			inputType:      "NUMERIC(38,9)",
			expectedResult: CommonTypeNumeric,
		},
		{
			name:           "uppercase varchar with length",
			mapper:         createTestMapper(),
			inputType:      "VARCHAR(100)",
			expectedResult: CommonTypeString,
		},

		// Types with spaces
		{
			name:           "timestamp with time zone",
			mapper:         createTestMapper(),
			inputType:      "timestamp with time zone",
			expectedResult: CommonTypeDateTime,
		},
		{
			name:           "time with precision and zone",
			mapper:         createTestMapper(),
			inputType:      "time(6) with time zone",
			expectedResult: CommonTypeDateTime,
		},

		// Unknown types
		{
			name:           "unknown type",
			mapper:         createTestMapper(),
			inputType:      "geography",
			expectedResult: CommonTypeUnknown,
		},
		{
			name:           "unknown parametrized type",
			mapper:         createTestMapper(),
			inputType:      "geometry(point)",
			expectedResult: CommonTypeUnknown,
		},

		// Edge cases
		{
			name:           "empty string",
			mapper:         createTestMapper(),
			inputType:      "",
			expectedResult: CommonTypeUnknown,
		},
		{
			name:           "only parentheses",
			mapper:         createTestMapper(),
			inputType:      "(10,2)",
			expectedResult: CommonTypeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.mapper.MapType(tt.inputType)
			if result != tt.expectedResult {
				t.Errorf("MapType(%q) = %v, want %v", tt.inputType, result, tt.expectedResult)
			}
		})
	}
}

func TestDuckDBTypeMapper(t *testing.T) {
	t.Parallel()
	mapper := NewDuckDBTypeMapper()

	tests := []struct {
		inputType      string
		expectedResult CommonDataType
	}{
		// DuckDB specific numeric types
		{"integer", CommonTypeNumeric},
		{"bigint", CommonTypeNumeric},
		{"double", CommonTypeNumeric},
		{"decimal(10,2)", CommonTypeNumeric},
		{"DECIMAL(18,4)", CommonTypeNumeric},

		// DuckDB string types
		{"varchar", CommonTypeString},
		{"text", CommonTypeString},
		{"varchar(255)", CommonTypeString},
		{"VARCHAR(100)", CommonTypeString},

		// DuckDB boolean types
		{"boolean", CommonTypeBoolean},
		{"bool", CommonTypeBoolean},
		{"logical", CommonTypeBoolean},

		// DuckDB datetime types
		{"date", CommonTypeDateTime},
		{"timestamp", CommonTypeDateTime},
		{"timestamptz", CommonTypeDateTime},
		{"timestamp with time zone", CommonTypeDateTime},

		// DuckDB binary types
		{"blob", CommonTypeBinary},
		{"bytea", CommonTypeBinary},

		// DuckDB JSON types
		{"json", CommonTypeJSON},
	}

	for _, tt := range tests {
		t.Run("DuckDB_"+tt.inputType, func(t *testing.T) {
			t.Parallel()
			result := mapper.MapType(tt.inputType)
			if result != tt.expectedResult {
				t.Errorf("DuckDB mapper: MapType(%q) = %v, want %v", tt.inputType, result, tt.expectedResult)
			}
		})
	}
}

func TestBigQueryTypeMapper(t *testing.T) {
	t.Parallel()
	mapper := NewBigQueryTypeMapper()

	tests := []struct {
		inputType      string
		expectedResult CommonDataType
	}{
		// BigQuery numeric types
		{"INT64", CommonTypeNumeric},
		{"int64", CommonTypeNumeric},
		{"FLOAT64", CommonTypeNumeric},
		{"NUMERIC", CommonTypeNumeric},
		{"numeric(38,9)", CommonTypeNumeric},
		{"NUMERIC(10,2)", CommonTypeNumeric},
		{"BIGNUMERIC", CommonTypeNumeric},
		{"bignumeric(76,38)", CommonTypeNumeric},

		// BigQuery string types
		{"STRING", CommonTypeString},
		{"string", CommonTypeString},
		{"BYTES", CommonTypeString},
		{"bytes", CommonTypeString},

		// BigQuery boolean types
		{"BOOL", CommonTypeBoolean},
		{"bool", CommonTypeBoolean},
		{"BOOLEAN", CommonTypeBoolean},
		{"boolean", CommonTypeBoolean},

		// BigQuery datetime types
		{"DATE", CommonTypeDateTime},
		{"date", CommonTypeDateTime},
		{"DATETIME", CommonTypeDateTime},
		{"TIME", CommonTypeDateTime},
		{"TIMESTAMP", CommonTypeDateTime},

		// BigQuery JSON types
		{"JSON", CommonTypeJSON},
		{"json", CommonTypeJSON},

		// BigQuery specific unknown types
		{"GEOGRAPHY", CommonTypeUnknown},
		{"ARRAY<STRING>", CommonTypeUnknown},
		{"STRUCT<name STRING, age INT64>", CommonTypeUnknown},
	}

	for _, tt := range tests {
		t.Run("BigQuery_"+tt.inputType, func(t *testing.T) {
			t.Parallel()
			result := mapper.MapType(tt.inputType)
			if result != tt.expectedResult {
				t.Errorf("BigQuery mapper: MapType(%q) = %v, want %v", tt.inputType, result, tt.expectedResult)
			}
		})
	}
}

func TestDatabaseTypeMapper_IsNumeric(t *testing.T) {
	t.Parallel()
	mapper := createTestMapper()

	tests := []struct {
		inputType string
		expected  bool
	}{
		{"integer", true},
		{"INTEGER", true}, // IsNumeric handles case conversion
		{"varchar", false},
		{"boolean", false},
		{"nonexistent", false},
	}

	for _, tt := range tests {
		t.Run(tt.inputType, func(t *testing.T) {
			t.Parallel()
			result := mapper.IsNumeric(tt.inputType)
			if result != tt.expected {
				t.Errorf("IsNumeric(%q) = %v, want %v", tt.inputType, result, tt.expected)
			}
		})
	}
}

func TestDatabaseTypeMapper_IsString(t *testing.T) {
	t.Parallel()

	mapper := createTestMapper()

	tests := []struct {
		inputType string
		expected  bool
	}{
		{"varchar", true},
		{"text", true},
		{"integer", false},
		{"nonexistent", false},
	}

	for _, tt := range tests {
		t.Run(tt.inputType, func(t *testing.T) {
			t.Parallel()
			result := mapper.IsString(tt.inputType)
			if result != tt.expected {
				t.Errorf("IsString(%q) = %v, want %v", tt.inputType, result, tt.expected)
			}
		})
	}
}

func TestDatabaseTypeMapper_IsBoolean(t *testing.T) {
	t.Parallel()
	mapper := createTestMapper()

	tests := []struct {
		inputType string
		expected  bool
	}{
		{"boolean", true},
		{"bool", true},
		{"integer", false},
		{"nonexistent", false},
	}

	for _, tt := range tests {
		t.Run(tt.inputType, func(t *testing.T) {
			t.Parallel()
			result := mapper.IsBoolean(tt.inputType)
			if result != tt.expected {
				t.Errorf("IsBoolean(%q) = %v, want %v", tt.inputType, result, tt.expected)
			}
		})
	}
}

func TestDatabaseTypeMapper_IsDateTime(t *testing.T) {
	t.Parallel()
	mapper := createTestMapper()

	tests := []struct {
		inputType string
		expected  bool
	}{
		{"timestamp", true},
		{"date", true},
		{"datetime", true},
		{"integer", false},
		{"nonexistent", false},
	}

	for _, tt := range tests {
		t.Run(tt.inputType, func(t *testing.T) {
			t.Parallel()
			result := mapper.IsDateTime(tt.inputType)
			if result != tt.expected {
				t.Errorf("IsDateTime(%q) = %v, want %v", tt.inputType, result, tt.expected)
			}
		})
	}
}

func TestParametrizedTypeExtraction(t *testing.T) {
	t.Parallel()
	mapper := createTestMapper()

	tests := []struct {
		name           string
		inputType      string
		expectedResult CommonDataType
		description    string
	}{
		{
			name:           "complex numeric with precision and scale",
			inputType:      "numeric(38,9)",
			expectedResult: CommonTypeNumeric,
			description:    "Should extract 'numeric' from 'numeric(38,9)'",
		},
		{
			name:           "varchar with max length",
			inputType:      "varchar(max)",
			expectedResult: CommonTypeString,
			description:    "Should extract 'varchar' from 'varchar(max)'",
		},
		{
			name:           "timestamp with precision",
			inputType:      "timestamp(6)",
			expectedResult: CommonTypeDateTime,
			description:    "Should extract 'timestamp' from 'timestamp(6)'",
		},
		{
			name:           "type with space before parentheses",
			inputType:      "decimal (10,2)",
			expectedResult: CommonTypeNumeric,
			description:    "Should handle space before parentheses",
		},
		{
			name:           "nested parentheses",
			inputType:      "array(varchar(255))",
			expectedResult: CommonTypeUnknown,
			description:    "Should extract 'array' (unknown type)",
		},
		{
			name:           "multiple spaces in type definition",
			inputType:      "timestamp   with   time   zone",
			expectedResult: CommonTypeDateTime,
			description:    "Should extract 'timestamp' from complex type with spaces",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := mapper.MapType(tt.inputType)
			if result != tt.expectedResult {
				t.Errorf("%s: MapType(%q) = %v, want %v", tt.description, tt.inputType, result, tt.expectedResult)
			}
		})
	}
}

func TestCrossDatabaseTypeCompatibility(t *testing.T) {
	t.Parallel()
	duckDBMapper := NewDuckDBTypeMapper()
	bigQueryMapper := NewBigQueryTypeMapper()

	// Test that equivalent types map to the same common type
	equivalentTypes := []struct {
		duckDBType   string
		bigQueryType string
		commonType   CommonDataType
		description  string
	}{
		{
			duckDBType:   "integer",
			bigQueryType: "INT64",
			commonType:   CommonTypeNumeric,
			description:  "Integer types should map to numeric",
		},
		{
			duckDBType:   "varchar(255)",
			bigQueryType: "STRING",
			commonType:   CommonTypeString,
			description:  "String types should map to string",
		},
		{
			duckDBType:   "boolean",
			bigQueryType: "BOOL",
			commonType:   CommonTypeBoolean,
			description:  "Boolean types should map to boolean",
		},
		{
			duckDBType:   "timestamp",
			bigQueryType: "TIMESTAMP",
			commonType:   CommonTypeDateTime,
			description:  "Timestamp types should map to datetime",
		},
		{
			duckDBType:   "decimal(10,2)",
			bigQueryType: "NUMERIC(10,2)",
			commonType:   CommonTypeNumeric,
			description:  "Decimal/numeric types should map to numeric",
		},
	}

	for _, tt := range equivalentTypes {
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			duckDBResult := duckDBMapper.MapType(tt.duckDBType)
			bigQueryResult := bigQueryMapper.MapType(tt.bigQueryType)

			if duckDBResult != tt.commonType {
				t.Errorf("DuckDB: MapType(%q) = %v, want %v", tt.duckDBType, duckDBResult, tt.commonType)
			}

			if bigQueryResult != tt.commonType {
				t.Errorf("BigQuery: MapType(%q) = %v, want %v", tt.bigQueryType, bigQueryResult, tt.commonType)
			}

			if duckDBResult != bigQueryResult {
				t.Errorf("Type compatibility failed: DuckDB %q (%v) != BigQuery %q (%v)",
					tt.duckDBType, duckDBResult, tt.bigQueryType, bigQueryResult)
			}
		})
	}
}

func createTestMapper() *DatabaseTypeMapper {
	mapper := NewDatabaseTypeMapper()

	mapper.AddNumericTypes("integer", "numeric", "decimal", "bigint", "smallint")
	mapper.AddStringTypes("varchar", "text", "char")
	mapper.AddBooleanTypes("boolean", "bool")
	mapper.AddDateTimeTypes("timestamp", "date", "datetime", "time")
	mapper.AddBinaryTypes("blob", "binary")
	mapper.AddJSONTypes("json")

	return mapper
}
