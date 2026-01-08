package mcp

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDBToolDefinitions(t *testing.T) {
	tools := GetDBToolDefinitions()

	assert.Len(t, tools, 4, "should have 4 database tools")

	// Verify tool names
	toolNames := make([]string, len(tools))
	for i, tool := range tools {
		name, ok := tool["name"].(string)
		require.True(t, ok, "tool should have a name")
		toolNames[i] = name
	}

	assert.Contains(t, toolNames, "bruin_list_connections")
	assert.Contains(t, toolNames, "bruin_get_table_schema")
	assert.Contains(t, toolNames, "bruin_get_table_summary")
	assert.Contains(t, toolNames, "bruin_sample_column_values")
}

func TestIsDBTool(t *testing.T) {
	tests := []struct {
		name     string
		toolName string
		expected bool
	}{
		{
			name:     "bruin_list_connections is a DB tool",
			toolName: "bruin_list_connections",
			expected: true,
		},
		{
			name:     "bruin_get_table_schema is a DB tool",
			toolName: "bruin_get_table_schema",
			expected: true,
		},
		{
			name:     "bruin_get_table_summary is a DB tool",
			toolName: "bruin_get_table_summary",
			expected: true,
		},
		{
			name:     "bruin_sample_column_values is a DB tool",
			toolName: "bruin_sample_column_values",
			expected: true,
		},
		{
			name:     "bruin_get_column_stats is not a DB tool (removed)",
			toolName: "bruin_get_column_stats",
			expected: false,
		},
		{
			name:     "bruin_get_overview is not a DB tool",
			toolName: "bruin_get_overview",
			expected: false,
		},
		{
			name:     "bruin_get_docs_tree is not a DB tool",
			toolName: "bruin_get_docs_tree",
			expected: false,
		},
		{
			name:     "unknown tool is not a DB tool",
			toolName: "unknown_tool",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsDBTool(tt.toolName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHandleDBToolCall_MissingParameters(t *testing.T) {
	tests := []struct {
		name     string
		toolName string
		args     map[string]interface{}
		errField string
	}{
		{
			name:     "get_table_schema missing connection",
			toolName: "bruin_get_table_schema",
			args:     map[string]interface{}{"table": "users"},
			errField: "connection and table parameters are required",
		},
		{
			name:     "get_table_schema missing table",
			toolName: "bruin_get_table_schema",
			args:     map[string]interface{}{"connection": "mydb"},
			errField: "connection and table parameters are required",
		},
		{
			name:     "get_table_summary missing connection",
			toolName: "bruin_get_table_summary",
			args:     map[string]interface{}{"table": "users"},
			errField: "connection and table parameters are required",
		},
		{
			name:     "get_table_summary missing table",
			toolName: "bruin_get_table_summary",
			args:     map[string]interface{}{"connection": "mydb"},
			errField: "connection and table parameters are required",
		},
		{
			name:     "sample_column_values missing connection",
			toolName: "bruin_sample_column_values",
			args:     map[string]interface{}{"table": "users", "column": "status"},
			errField: "connection, table, and column parameters are required",
		},
		{
			name:     "sample_column_values missing table",
			toolName: "bruin_sample_column_values",
			args:     map[string]interface{}{"connection": "mydb", "column": "status"},
			errField: "connection, table, and column parameters are required",
		},
		{
			name:     "sample_column_values missing column",
			toolName: "bruin_sample_column_values",
			args:     map[string]interface{}{"connection": "mydb", "table": "users"},
			errField: "connection, table, and column parameters are required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := HandleDBToolCall(tt.toolName, tt.args, false)
			assert.NoError(t, err)
			assert.Contains(t, result, tt.errField)
		})
	}
}

func TestHandleDBToolCall_UnknownTool(t *testing.T) {
	result, err := HandleDBToolCall("unknown_tool", nil, false)
	assert.Error(t, err)
	assert.Empty(t, result)
	assert.Contains(t, err.Error(), "unknown database tool")
}

func TestFormatDBError(t *testing.T) {
	result := formatDBError("test_operation", assert.AnError)

	var parsed map[string]string
	err := json.Unmarshal([]byte(result), &parsed)
	require.NoError(t, err)

	assert.Equal(t, "test_operation", parsed["operation"])
	assert.NotEmpty(t, parsed["error"])
}

func TestTableSchemaJSON(t *testing.T) {
	schema := &TableSchema{
		TableName:  "users",
		Connection: "mydb",
		Columns: []ColumnInfo{
			{Name: "id", Type: "integer", Nullable: false},
			{Name: "email", Type: "varchar", Nullable: true},
		},
	}

	jsonBytes, err := json.Marshal(schema)
	require.NoError(t, err)

	var parsed TableSchema
	err = json.Unmarshal(jsonBytes, &parsed)
	require.NoError(t, err)

	assert.Equal(t, "users", parsed.TableName)
	assert.Equal(t, "mydb", parsed.Connection)
	assert.Len(t, parsed.Columns, 2)
	assert.Equal(t, "id", parsed.Columns[0].Name)
	assert.Equal(t, "integer", parsed.Columns[0].Type)
}

func TestSampleValuesJSON(t *testing.T) {
	sample := &SampleValues{
		ColumnName: "status",
		TableName:  "orders",
		Values:     []interface{}{"active", "pending", "completed"},
	}

	jsonBytes, err := json.Marshal(sample)
	require.NoError(t, err)

	var parsed SampleValues
	err = json.Unmarshal(jsonBytes, &parsed)
	require.NoError(t, err)

	assert.Equal(t, "status", parsed.ColumnName)
	assert.Equal(t, "orders", parsed.TableName)
	assert.Len(t, parsed.Values, 3)
}

func TestTableSummaryJSON(t *testing.T) {
	summary := &TableSummary{
		TableName:  "users",
		Connection: "mydb",
		RowCount:   1000,
		Columns: []ColumnSummary{
			{
				Name:           "id",
				Type:           "integer",
				NormalizedType: "numeric",
				Nullable:       false,
				PrimaryKey:     true,
				Unique:         true,
				Stats: map[string]interface{}{
					"type":       "numerical",
					"min":        1.0,
					"max":        1000.0,
					"null_count": int64(0),
					"count":      int64(1000),
				},
			},
			{
				Name:           "email",
				Type:           "varchar",
				NormalizedType: "string",
				Nullable:       true,
				PrimaryKey:     false,
				Unique:         false,
				Stats: map[string]interface{}{
					"type":           "string",
					"distinct_count": int64(950),
					"null_count":     int64(50),
					"count":          int64(1000),
				},
			},
		},
	}

	jsonBytes, err := json.Marshal(summary)
	require.NoError(t, err)

	var parsed TableSummary
	err = json.Unmarshal(jsonBytes, &parsed)
	require.NoError(t, err)

	assert.Equal(t, "users", parsed.TableName)
	assert.Equal(t, "mydb", parsed.Connection)
	assert.Equal(t, int64(1000), parsed.RowCount)
	assert.Len(t, parsed.Columns, 2)
	assert.Equal(t, "id", parsed.Columns[0].Name)
	assert.Equal(t, "integer", parsed.Columns[0].Type)
	assert.True(t, parsed.Columns[0].PrimaryKey)
	assert.Equal(t, "email", parsed.Columns[1].Name)
	assert.True(t, parsed.Columns[1].Nullable)
}

func TestDBToolDefinitionsHaveRequiredFields(t *testing.T) {
	tools := GetDBToolDefinitions()

	for _, tool := range tools {
		name, ok := tool["name"].(string)
		require.True(t, ok, "tool should have name")
		assert.NotEmpty(t, name)

		desc, ok := tool["description"].(string)
		require.True(t, ok, "tool should have description")
		assert.NotEmpty(t, desc)

		inputSchema, ok := tool["inputSchema"].(map[string]interface{})
		require.True(t, ok, "tool should have inputSchema")
		assert.NotNil(t, inputSchema)

		schemaType, ok := inputSchema["type"].(string)
		require.True(t, ok, "inputSchema should have type")
		assert.Equal(t, "object", schemaType)
	}
}

func TestDBToolDefinitionsRequiredParams(t *testing.T) {
	tools := GetDBToolDefinitions()
	toolMap := make(map[string]map[string]interface{})
	for _, tool := range tools {
		name := tool["name"].(string)
		toolMap[name] = tool
	}

	// bruin_list_connections has no required params
	listConnTool := toolMap["bruin_list_connections"]
	inputSchema := listConnTool["inputSchema"].(map[string]interface{})
	_, hasRequired := inputSchema["required"]
	assert.False(t, hasRequired, "bruin_list_connections should not have required params")

	// bruin_get_table_schema requires connection and table
	tableSchemaTool := toolMap["bruin_get_table_schema"]
	inputSchema = tableSchemaTool["inputSchema"].(map[string]interface{})
	required := inputSchema["required"].([]string)
	assert.Contains(t, required, "connection")
	assert.Contains(t, required, "table")

	// bruin_get_table_summary requires connection and table
	tableSummaryTool := toolMap["bruin_get_table_summary"]
	inputSchema = tableSummaryTool["inputSchema"].(map[string]interface{})
	required = inputSchema["required"].([]string)
	assert.Contains(t, required, "connection")
	assert.Contains(t, required, "table")

	// bruin_sample_column_values requires connection, table, and column
	sampleValuesTool := toolMap["bruin_sample_column_values"]
	inputSchema = sampleValuesTool["inputSchema"].(map[string]interface{})
	required = inputSchema["required"].([]string)
	assert.Contains(t, required, "connection")
	assert.Contains(t, required, "table")
	assert.Contains(t, required, "column")
}
