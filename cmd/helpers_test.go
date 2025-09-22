package cmd

import (
	"strings"
	"testing"
)

func TestConvertDuckDBDecimal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		parts    []string
		expected string
		success  bool
	}{
		{
			name:     "valid DuckDB decimal - 2.99",
			parts:    []string{"3", "2", "299"},
			expected: "2.99",
			success:  true,
		},
		{
			name:     "valid DuckDB decimal - 123.45",
			parts:    []string{"5", "2", "12345"},
			expected: "123.45",
			success:  true,
		},
		{
			name:     "valid DuckDB decimal - 0.1",
			parts:    []string{"1", "1", "1"},
			expected: "0.1",
			success:  true,
		},
		{
			name:     "valid DuckDB decimal - 100.0",
			parts:    []string{"3", "0", "100"},
			expected: "100",
			success:  true,
		},
		{
			name:     "valid DuckDB decimal - 0.001",
			parts:    []string{"3", "3", "1"},
			expected: "0.001",
			success:  true,
		},
		{
			name:     "valid DuckDB decimal - 999.999",
			parts:    []string{"6", "3", "999999"},
			expected: "999.999",
			success:  true,
		},
		{
			name:     "invalid - wrong number of parts",
			parts:    []string{"3", "2"},
			expected: "",
			success:  false,
		},
		{
			name:     "invalid - non-numeric width",
			parts:    []string{"abc", "2", "299"},
			expected: "",
			success:  false,
		},
		{
			name:     "invalid - non-numeric scale",
			parts:    []string{"3", "xyz", "299"},
			expected: "",
			success:  false,
		},
		{
			name:     "invalid - non-numeric value",
			parts:    []string{"3", "2", "def"},
			expected: "",
			success:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, success := convertDuckDBDecimal(tt.parts)
			if success != tt.success {
				t.Errorf("convertDuckDBDecimal() success = %v, want %v", success, tt.success)
			}
			if result != tt.expected {
				t.Errorf("convertDuckDBDecimal() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestConvertPostgreSQLDecimal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		parts    []string
		expected string
		success  bool
	}{
		{
			name:     "valid PostgreSQL decimal - 3.142",
			parts:    []string{"3142", "-3", "false", "finite", "true"},
			expected: "3.142",
			success:  true,
		},
		{
			name:     "valid PostgreSQL decimal - 123.45",
			parts:    []string{"12345", "-2", "false", "finite", "true"},
			expected: "123.45",
			success:  true,
		},
		{
			name:     "valid PostgreSQL decimal - 0.1",
			parts:    []string{"1", "-1", "false", "finite", "true"},
			expected: "0.1",
			success:  true,
		},
		{
			name:     "valid PostgreSQL decimal - 100.0",
			parts:    []string{"100", "0", "false", "finite", "true"},
			expected: "100",
			success:  true,
		},
		{
			name:     "valid PostgreSQL decimal - 0.001",
			parts:    []string{"1", "-3", "false", "finite", "true"},
			expected: "0.001",
			success:  true,
		},
		{
			name:     "valid PostgreSQL decimal - 999.999",
			parts:    []string{"999999", "-3", "false", "finite", "true"},
			expected: "999.999",
			success:  true,
		},
		{
			name:     "invalid - wrong number of parts",
			parts:    []string{"3142", "-3", "false"},
			expected: "",
			success:  false,
		},
		{
			name:     "invalid - non-numeric value",
			parts:    []string{"abc", "-3", "false", "finite", "true"},
			expected: "",
			success:  false,
		},
		{
			name:     "invalid - non-numeric scale",
			parts:    []string{"3142", "xyz", "false", "finite", "true"},
			expected: "",
			success:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, success := convertPostgreSQLDecimal(tt.parts)
			if success != tt.success {
				t.Errorf("convertPostgreSQLDecimal() success = %v, want %v", success, tt.success)
			}
			if result != tt.expected {
				t.Errorf("convertPostgreSQLDecimal() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestConvertValueToString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		// Nil and basic types
		{
			name:     "nil value",
			input:    nil,
			expected: "",
		},
		{
			name:     "string value",
			input:    "hello",
			expected: "hello",
		},
		{
			name:     "int64 value",
			input:    int64(42),
			expected: "42",
		},
		{
			name:     "float64 value",
			input:    float64(3.14),
			expected: "3.14",
		},
		{
			name:     "bool value",
			input:    true,
			expected: "true",
		},

		// DuckDB decimal strings
		{
			name:     "DuckDB decimal string - 2.99",
			input:    "{3 2 299}",
			expected: "2.99",
		},
		{
			name:     "DuckDB decimal string - 123.45",
			input:    "{5 2 12345}",
			expected: "123.45",
		},
		{
			name:     "DuckDB decimal string - 0.1",
			input:    "{1 1 1}",
			expected: "0.1",
		},
		{
			name:     "DuckDB decimal string - 100.0",
			input:    "{3 0 100}",
			expected: "100",
		},

		// PostgreSQL decimal strings
		{
			name:     "PostgreSQL decimal string - 3.142",
			input:    "{3142 -3 false finite true}",
			expected: "3.142",
		},
		{
			name:     "PostgreSQL decimal string - 123.45",
			input:    "{12345 -2 false finite true}",
			expected: "123.45",
		},
		{
			name:     "PostgreSQL decimal string - 0.1",
			input:    "{1 -1 false finite true}",
			expected: "0.1",
		},
		{
			name:     "PostgreSQL decimal string - 100.0",
			input:    "{100 0 false finite true}",
			expected: "100",
		},

		// Edge cases - strings with {} that are NOT decimal encodings
		{
			name:     "non-decimal string with braces - 3 parts but non-numeric",
			input:    "{29 a b}",
			expected: "{29 a b}",
		},
		{
			name:     "non-decimal string with braces - 5 parts but non-numeric",
			input:    "{abc def ghi jkl mno}",
			expected: "{abc def ghi jkl mno}",
		},
		{
			name:     "non-decimal string with braces - 2 parts",
			input:    "{hello world}",
			expected: "{hello world}",
		},
		{
			name:     "non-decimal string with braces - 4 parts",
			input:    "{a b c d}",
			expected: "{a b c d}",
		},
		{
			name:     "non-decimal string with braces - 6 parts",
			input:    "{1 2 3 4 5 6}",
			expected: "{1 2 3 4 5 6}",
		},
		{
			name:     "non-decimal string with braces - mixed numeric and text",
			input:    "{123 abc 456}",
			expected: "{123 abc 456}",
		},
		{
			name:     "non-decimal string with braces - partial numeric",
			input:    "{123 -3 false finite}",
			expected: "{123 -3 false finite}",
		},

		// Other numeric types
		{
			name:     "int32 value",
			input:    int32(42),
			expected: "42",
		},
		{
			name:     "int value",
			input:    42,
			expected: "42",
		},
		{
			name:     "float32 value",
			input:    float32(3.14),
			expected: "3.14",
		},
		{
			name:     "bool false",
			input:    false,
			expected: "false",
		},

		// Complex types that should fall back to fmt.Sprintf
		{
			name:     "slice",
			input:    []int{1, 2, 3},
			expected: "[1 2 3]",
		},
		{
			name:     "map",
			input:    map[string]int{"a": 1, "b": 2},
			expected: "map[a:1 b:2]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Use appropriate connection type based on test case
			var connType string
			switch {
			case strings.Contains(tt.name, "DuckDB"):
				connType = "*duck.Client"
			case strings.Contains(tt.name, "PostgreSQL"):
				connType = "*postgres.Client"
			default:
				connType = "*duck.Client" // default
			}
			result := convertValueToStringWithConnection(tt.input, connType)
			if result != tt.expected {
				t.Errorf("convertValueToStringWithConnection() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestConvertValueToStringEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    interface{}
		expected string
		connType string
	}{
		{
			name:     "negative DuckDB decimal",
			input:    "{3 2 -299}",
			expected: "-2.99",
			connType: "*duck.Client",
		},
		{
			name:     "zero DuckDB decimal",
			input:    "{3 2 0}",
			expected: "0",
			connType: "*duck.Client",
		},
		{
			name:     "large DuckDB decimal",
			input:    "{10 2 1234567890}",
			expected: "12345678.90",
			connType: "*duck.Client",
		},
		{
			name:     "non-decimal braces with 3 parts",
			input:    "{2 a b}",
			expected: "{2 a b}",
			connType: "*duck.Client",
		},
		{
			name:     "non-decimal braces with 5 parts",
			input:    "{2 a b c d}",
			expected: "{2 a b c d}",
			connType: "*duck.Client",
		},
		{
			name:     "negative PostgreSQL decimal",
			input:    "{-3142 -3 false finite true}",
			expected: "-3.142",
			connType: "*postgres.Client",
		},
		{
			name:     "zero PostgreSQL decimal",
			input:    "{0 -3 false finite true}",
			expected: "0",
			connType: "*postgres.Client",
		},
		{
			name:     "large PostgreSQL decimal",
			input:    "{1234567890 -2 false finite true}",
			expected: "12345678.90",
			connType: "*postgres.Client",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
			connType: "*duck.Client",
		},
		{
			name:     "string with only braces",
			input:    "{}",
			expected: "{}",
			connType: "*duck.Client",
		},
		{
			name:     "string with single space in braces",
			input:    "{ }",
			expected: "{ }",
			connType: "*duck.Client",
		},
		{
			name:     "string with multiple spaces in braces",
			input:    "{   }",
			expected: "{   }",
			connType: "*duck.Client",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := convertValueToStringWithConnection(tt.input, tt.connType)
			if result != tt.expected {
				t.Errorf("convertValueToStringWithConnection() = %v, want %v", result, tt.expected)
			}
		})
	}
}
