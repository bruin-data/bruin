package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/spf13/afero"
)

// DBToolsConfig holds configuration for database tools.
type DBToolsConfig struct {
	RepoRoot    string
	Environment string
}

// Global config that can be set via environment variables or MCP initialization.
var dbToolsConfig *DBToolsConfig

// initDBToolsConfig initializes the database tools configuration from environment.
func initDBToolsConfig() {
	if dbToolsConfig != nil {
		return
	}

	dbToolsConfig = &DBToolsConfig{
		RepoRoot:    os.Getenv("BRUIN_REPO_ROOT"),
		Environment: os.Getenv("BRUIN_ENVIRONMENT"),
	}

	// If repo root not set, try to find it from current directory
	if dbToolsConfig.RepoRoot == "" {
		cwd, err := os.Getwd()
		if err == nil {
			repo, err := git.FindRepoFromPath(cwd)
			if err == nil {
				dbToolsConfig.RepoRoot = repo.Path
			}
		}
	}
}

// getConnectionManager creates a connection manager from the config.
func getConnectionManager(ctx context.Context) (config.ConnectionAndDetailsGetter, error) {
	initDBToolsConfig()

	if dbToolsConfig.RepoRoot == "" {
		return nil, fmt.Errorf("no Bruin repository found. Set BRUIN_REPO_ROOT environment variable or run from a Bruin project directory")
	}

	fs := afero.NewOsFs()
	configPath := filepath.Join(dbToolsConfig.RepoRoot, ".bruin.yml")

	cm, err := config.LoadOrCreate(fs, configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config from %s: %w", configPath, err)
	}

	if dbToolsConfig.Environment != "" {
		if err := cm.SelectEnvironment(dbToolsConfig.Environment); err != nil {
			return nil, fmt.Errorf("failed to select environment '%s': %w", dbToolsConfig.Environment, err)
		}
	}

	manager, errs := connection.NewManagerFromConfigWithContext(ctx, cm)
	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to create connection manager: %w", errs[0])
	}

	return manager, nil
}

// TableSchema represents the schema of a database table.
type TableSchema struct {
	TableName   string         `json:"table_name"`
	Columns     []ColumnInfo   `json:"columns"`
	Connection  string         `json:"connection"`
	Error       string         `json:"error,omitempty"`
}

// ColumnInfo represents information about a column.
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable,omitempty"`
}


// SampleValues represents sample distinct values for a column.
type SampleValues struct {
	ColumnName string        `json:"column_name"`
	TableName  string        `json:"table_name"`
	Values     []interface{} `json:"values"`
	Error      string        `json:"error,omitempty"`
}

// TableSummary represents comprehensive statistics for an entire table.
type TableSummary struct {
	TableName  string                  `json:"table_name"`
	Connection string                  `json:"connection"`
	RowCount   int64                   `json:"row_count"`
	Columns    []ColumnSummary         `json:"columns"`
	Error      string                  `json:"error,omitempty"`
}

// ColumnSummary represents statistics for a single column within a table summary.
type ColumnSummary struct {
	Name           string      `json:"name"`
	Type           string      `json:"type"`
	NormalizedType string      `json:"normalized_type"`
	Nullable       bool        `json:"nullable"`
	PrimaryKey     bool        `json:"primary_key"`
	Unique         bool        `json:"unique"`
	Stats          interface{} `json:"stats,omitempty"`
}

// Selector interface for querying databases.
type Selector interface {
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error)
}

// GetTableSummaryWithStats retrieves comprehensive statistics for all columns in a table.
// This uses the TableSummarizer interface which is implemented by most database clients.
// Exported for use by the enhance command to pre-fetch stats before calling Claude.
func GetTableSummaryWithStats(ctx context.Context, connectionName, tableName string) *TableSummary {
	manager, err := getConnectionManager(ctx)
	if err != nil {
		return &TableSummary{TableName: tableName, Error: err.Error()}
	}
	return GetTableSummaryWithManager(ctx, manager, connectionName, tableName)
}

// GetTableSummaryWithManager retrieves comprehensive statistics using a provided connection manager.
// This is useful when the caller already has a connection manager (e.g., the enhance command).
func GetTableSummaryWithManager(ctx context.Context, manager config.ConnectionAndDetailsGetter, connectionName, tableName string) *TableSummary {
	if manager == nil {
		return &TableSummary{TableName: tableName, Error: "connection manager is nil"}
	}

	conn := manager.GetConnection(connectionName)
	if conn == nil {
		return &TableSummary{TableName: tableName, Error: fmt.Sprintf("connection '%s' not found", connectionName)}
	}

	summarizer, ok := conn.(diff.TableSummarizer)
	if !ok {
		return &TableSummary{TableName: tableName, Error: fmt.Sprintf("connection '%s' does not support table summary", connectionName)}
	}

	result, err := summarizer.GetTableSummary(ctx, tableName, false)
	if err != nil {
		return &TableSummary{TableName: tableName, Connection: connectionName, Error: fmt.Sprintf("failed to get table summary: %v", err)}
	}

	summary := &TableSummary{
		TableName:  tableName,
		Connection: connectionName,
		RowCount:   result.RowCount,
		Columns:    make([]ColumnSummary, 0, len(result.Table.Columns)),
	}

	for _, col := range result.Table.Columns {
		colSummary := ColumnSummary{
			Name:           col.Name,
			Type:           col.Type,
			NormalizedType: string(col.NormalizedType),
			Nullable:       col.Nullable,
			PrimaryKey:     col.PrimaryKey,
			Unique:         col.Unique,
		}

		// Convert statistics to a map for JSON serialization
		if col.Stats != nil {
			colSummary.Stats = convertStatsToMap(col.Stats)
		}

		summary.Columns = append(summary.Columns, colSummary)
	}

	return summary
}

// convertStatsToMap converts diff.ColumnStatistics to a map for JSON serialization.
func convertStatsToMap(stats diff.ColumnStatistics) map[string]interface{} {
	result := make(map[string]interface{})
	result["type"] = stats.Type()

	switch s := stats.(type) {
	case *diff.NumericalStatistics:
		if s.Min != nil {
			result["min"] = *s.Min
		}
		if s.Max != nil {
			result["max"] = *s.Max
		}
		if s.Avg != nil {
			result["avg"] = *s.Avg
		}
		result["count"] = s.Count
		result["null_count"] = s.NullCount
		if s.StdDev != nil {
			result["stddev"] = *s.StdDev
		}
	case *diff.StringStatistics:
		result["distinct_count"] = s.DistinctCount
		result["min_length"] = s.MinLength
		result["max_length"] = s.MaxLength
		result["avg_length"] = s.AvgLength
		result["count"] = s.Count
		result["null_count"] = s.NullCount
		result["empty_count"] = s.EmptyCount
	case *diff.BooleanStatistics:
		result["true_count"] = s.TrueCount
		result["false_count"] = s.FalseCount
		result["count"] = s.Count
		result["null_count"] = s.NullCount
	case *diff.DateTimeStatistics:
		if !s.EarliestDate.IsZero() {
			result["earliest_date"] = s.EarliestDate.String()
		}
		if !s.LatestDate.IsZero() {
			result["latest_date"] = s.LatestDate.String()
		}
		result["unique_count"] = s.UniqueCount
		result["count"] = s.Count
		result["null_count"] = s.NullCount
	case *diff.JSONStatistics:
		result["count"] = s.Count
		result["null_count"] = s.NullCount
	}

	return result
}

// getTableSchema retrieves the schema for a given table.
func getTableSchema(ctx context.Context, connectionName, tableName string) *TableSchema {
	manager, err := getConnectionManager(ctx)
	if err != nil {
		return &TableSchema{TableName: tableName, Error: err.Error()}
	}

	conn := manager.GetConnection(connectionName)
	if conn == nil {
		return &TableSchema{TableName: tableName, Error: fmt.Sprintf("connection '%s' not found", connectionName)}
	}

	selector, ok := conn.(Selector)
	if !ok {
		return &TableSchema{TableName: tableName, Error: fmt.Sprintf("connection '%s' does not support schema queries", connectionName)}
	}

	// Query to get schema (SELECT * FROM table WHERE 1=0)
	q := &query.Query{Query: fmt.Sprintf("SELECT * FROM %s WHERE 1=0", tableName)}
	result, err := selector.SelectWithSchema(ctx, q)
	if err != nil {
		return &TableSchema{TableName: tableName, Connection: connectionName, Error: fmt.Sprintf("failed to query schema: %v", err)}
	}

	schema := &TableSchema{
		TableName:  tableName,
		Connection: connectionName,
		Columns:    make([]ColumnInfo, 0, len(result.Columns)),
	}

	for i, colName := range result.Columns {
		colType := ""
		if i < len(result.ColumnTypes) {
			colType = result.ColumnTypes[i]
		}
		schema.Columns = append(schema.Columns, ColumnInfo{
			Name: colName,
			Type: colType,
		})
	}

	return schema
}

// getSampleColumnValues retrieves sample distinct values for a column.
// Limit is capped at 1000 for performance on large tables.
func getSampleColumnValues(ctx context.Context, connectionName, tableName, columnName string, limit int) *SampleValues {
	if limit <= 0 {
		limit = 20
	}
	if limit > 1000 {
		limit = 1000
	}

	manager, err := getConnectionManager(ctx)
	if err != nil {
		return &SampleValues{TableName: tableName, ColumnName: columnName, Error: err.Error()}
	}

	conn := manager.GetConnection(connectionName)
	if conn == nil {
		return &SampleValues{TableName: tableName, ColumnName: columnName, Error: fmt.Sprintf("connection '%s' not found", connectionName)}
	}

	selector, ok := conn.(Selector)
	if !ok {
		return &SampleValues{TableName: tableName, ColumnName: columnName, Error: fmt.Sprintf("connection '%s' does not support queries", connectionName)}
	}

	// Query for distinct values
	sampleQuery := fmt.Sprintf(`
		SELECT DISTINCT %s
		FROM %s
		WHERE %s IS NOT NULL
		LIMIT %d
	`, columnName, tableName, columnName, limit)

	q := &query.Query{Query: sampleQuery}
	result, err := selector.Select(ctx, q)
	if err != nil {
		return &SampleValues{TableName: tableName, ColumnName: columnName, Error: fmt.Sprintf("failed to query sample values: %v", err)}
	}

	sample := &SampleValues{
		TableName:  tableName,
		ColumnName: columnName,
		Values:     make([]interface{}, 0, len(result)),
	}

	for _, row := range result {
		if len(row) > 0 {
			sample.Values = append(sample.Values, row[0])
		}
	}

	return sample
}

// listConnections returns a list of available connection names.
func listConnections(ctx context.Context) ([]string, error) {
	managerInterface, err := getConnectionManager(ctx)
	if err != nil {
		return nil, err
	}

	// Type assert to get the concrete Manager type
	manager, ok := managerInterface.(*connection.Manager)
	if !ok {
		return nil, fmt.Errorf("unexpected connection manager type")
	}

	connections := make([]string, 0)
	for name := range manager.AllConnectionDetails {
		connections = append(connections, name)
	}

	return connections, nil
}

// HandleDBToolCall handles database-related tool calls.
func HandleDBToolCall(toolName string, args map[string]interface{}, debug bool) (string, error) {
	ctx := context.Background()

	switch toolName {
	case "bruin_list_connections":
		connections, err := listConnections(ctx)
		if err != nil {
			return formatDBError("list_connections", err), nil
		}
		result := map[string]interface{}{
			"connections": connections,
		}
		jsonBytes, _ := json.MarshalIndent(result, "", "  ")
		return string(jsonBytes), nil

	case "bruin_get_table_schema":
		connectionName, _ := args["connection"].(string)
		tableName, _ := args["table"].(string)
		if connectionName == "" || tableName == "" {
			return formatDBError("get_table_schema", fmt.Errorf("connection and table parameters are required")), nil
		}
		schema := getTableSchema(ctx, connectionName, tableName)
		jsonBytes, _ := json.MarshalIndent(schema, "", "  ")
		return string(jsonBytes), nil

	case "bruin_get_table_summary":
		connectionName, _ := args["connection"].(string)
		tableName, _ := args["table"].(string)
		if connectionName == "" || tableName == "" {
			return formatDBError("get_table_summary", fmt.Errorf("connection and table parameters are required")), nil
		}
		summary := GetTableSummaryWithStats(ctx, connectionName, tableName)
		jsonBytes, _ := json.MarshalIndent(summary, "", "  ")
		return string(jsonBytes), nil

	case "bruin_sample_column_values":
		connectionName, _ := args["connection"].(string)
		tableName, _ := args["table"].(string)
		columnName, _ := args["column"].(string)
		limit := 20
		if l, ok := args["limit"].(float64); ok {
			limit = int(l)
		}
		if connectionName == "" || tableName == "" || columnName == "" {
			return formatDBError("sample_column_values", fmt.Errorf("connection, table, and column parameters are required")), nil
		}
		sample := getSampleColumnValues(ctx, connectionName, tableName, columnName, limit)
		jsonBytes, _ := json.MarshalIndent(sample, "", "  ")
		return string(jsonBytes), nil

	default:
		return "", fmt.Errorf("unknown database tool: %s", toolName)
	}
}

func formatDBError(operation string, err error) string {
	result := map[string]string{
		"error":     err.Error(),
		"operation": operation,
	}
	jsonBytes, _ := json.MarshalIndent(result, "", "  ")
	return string(jsonBytes)
}

// GetDBToolDefinitions returns the MCP tool definitions for database tools.
func GetDBToolDefinitions() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"name":        "bruin_list_connections",
			"description": "List all available database connections configured in the Bruin project. Use this first to discover what connections are available before querying tables.",
			"inputSchema": map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			},
		},
		{
			"name":        "bruin_get_table_schema",
			"description": "Get the schema (column names and types) for a database table. Use this to understand the structure of a table before suggesting data quality checks.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"connection": map[string]interface{}{
						"type":        "string",
						"description": "Name of the database connection (from bruin_list_connections)",
					},
					"table": map[string]interface{}{
						"type":        "string",
						"description": "Fully qualified table name (e.g., 'schema.table' or 'project.dataset.table')",
					},
				},
				"required": []string{"connection", "table"},
			},
		},
		{
			"name":        "bruin_get_table_summary",
			"description": "Get comprehensive statistics for ALL columns in a table in a single query. Returns row count, column types, and statistics (null_count, distinct_count, min/max, etc.) for each column. Use this to determine appropriate data quality checks for the entire table at once.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"connection": map[string]interface{}{
						"type":        "string",
						"description": "Name of the database connection (from bruin_list_connections)",
					},
					"table": map[string]interface{}{
						"type":        "string",
						"description": "Fully qualified table name (e.g., 'schema.table' or 'project.dataset.table')",
					},
				},
				"required": []string{"connection", "table"},
			},
		},
		{
			"name":        "bruin_sample_column_values",
			"description": "Get sample distinct values for a column (max 1000 values). Use this to suggest accepted_values checks for enum-like columns (e.g., status, type, category columns).",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"connection": map[string]interface{}{
						"type":        "string",
						"description": "Name of the database connection",
					},
					"table": map[string]interface{}{
						"type":        "string",
						"description": "Fully qualified table name",
					},
					"column": map[string]interface{}{
						"type":        "string",
						"description": "Column name to sample",
					},
					"limit": map[string]interface{}{
						"type":        "integer",
						"description": "Maximum number of distinct values to return (default: 20)",
					},
				},
				"required": []string{"connection", "table", "column"},
			},
		},
	}
}

// IsDBTool checks if a tool name is a database tool.
func IsDBTool(toolName string) bool {
	dbTools := map[string]bool{
		"bruin_list_connections":     true,
		"bruin_get_table_schema":     true,
		"bruin_get_table_summary":    true,
		"bruin_sample_column_values": true,
	}
	return dbTools[toolName]
}
