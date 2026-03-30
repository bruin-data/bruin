package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeQuickSightName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple name",
			input:    "my_dataset",
			expected: "my_dataset",
		},
		{
			name:     "spaces to underscores",
			input:    "My Sales Data",
			expected: "my_sales_data",
		},
		{
			name:     "dashes to underscores",
			input:    "my-sales-data",
			expected: "my_sales_data",
		},
		{
			name:     "special characters removed",
			input:    "data@source#1",
			expected: "datasource1",
		},
		{
			name:     "consecutive underscores collapsed",
			input:    "my___data___set",
			expected: "my_data_set",
		},
		{
			name:     "leading trailing underscores trimmed",
			input:    "_my_data_",
			expected: "my_data",
		},
		{
			name:     "empty string returns unnamed",
			input:    "",
			expected: "unnamed",
		},
		{
			name:     "only special chars returns unnamed",
			input:    "@#$%",
			expected: "unnamed",
		},
		{
			name:     "dots to underscores",
			input:    "schema.table.name",
			expected: "schema_table_name",
		},
		{
			name:     "slashes to underscores",
			input:    "path/to/data",
			expected: "path_to_data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := sanitizeQuickSightName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildTableReference(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		schema   string
		table    string
		expected string
	}{
		{
			name:     "with schema",
			schema:   "public",
			table:    "orders",
			expected: "public.orders",
		},
		{
			name:     "without schema",
			schema:   "",
			table:    "orders",
			expected: "orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := buildTableReference(tt.schema, tt.table)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMapQuickSightColumnType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"STRING", "STRING"},
		{"INTEGER", "INTEGER"},
		{"DECIMAL", "FLOAT"},
		{"DATETIME", "TIMESTAMP"},
		{"UNKNOWN", "UNKNOWN"},
		{"string", "STRING"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, mapQuickSightColumnType(tt.input))
		})
	}
}
