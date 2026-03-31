package cmd

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/quicksight"
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

func TestCreateQuickSightDatasetAsset_RelationalTable(t *testing.T) {
	t.Parallel()

	detail := &quicksight.DataSetDetail{
		ID:         "ds-001",
		Name:       "Sales Data",
		ImportMode: "SPICE",
		Columns: []quicksight.DataSetColumn{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "STRING"},
		},
		PhysicalTableMaps: map[string]quicksight.PhysicalTable{
			"table1": {
				SchemaName: "public",
				TableName:  "orders",
			},
		},
	}

	asset := createQuickSightDatasetAsset(detail, "/tmp/assets")

	assert.Equal(t, "quicksight.datasets.dataset_sales_data", asset.Name)
	assert.Equal(t, "SPICE", asset.Parameters["import_mode"])
	assert.Empty(t, asset.Parameters["custom_sql"])
	assert.Len(t, asset.Columns, 2)
	assert.Len(t, asset.Upstreams, 1)
	assert.Equal(t, "public.orders", asset.Upstreams[0].Value)
}

func TestCreateQuickSightDatasetAsset_CustomSql(t *testing.T) {
	t.Parallel()

	detail := &quicksight.DataSetDetail{
		ID:         "ds-002",
		Name:       "Issues Query",
		ImportMode: "SPICE",
		Columns: []quicksight.DataSetColumn{
			{Name: "id", Type: "INTEGER"},
			{Name: "title", Type: "STRING"},
		},
		PhysicalTableMaps: map[string]quicksight.PhysicalTable{
			"custom1": {
				SqlQuery: "select * from issues where true",
				SqlName:  "issues_query",
			},
		},
	}

	asset := createQuickSightDatasetAsset(detail, "/tmp/assets")

	assert.Equal(t, "quicksight.datasets.dataset_issues_query", asset.Name)
	assert.Equal(t, "SPICE", asset.Parameters["import_mode"])
	assert.Equal(t, "select * from issues where true", asset.Parameters["custom_sql"])
	assert.Len(t, asset.Columns, 2)
	assert.Empty(t, asset.Upstreams) // CustomSql has no table-based upstreams
}
