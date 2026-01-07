package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
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

// ColumnStats represents statistics for a column.
type ColumnStats struct {
	ColumnName    string      `json:"column_name"`
	TableName     string      `json:"table_name"`
	TotalRows     int64       `json:"total_rows"`
	NullCount     int64       `json:"null_count"`
	DistinctCount int64       `json:"distinct_count"`
	MinValue      interface{} `json:"min_value,omitempty"`
	MaxValue      interface{} `json:"max_value,omitempty"`
	Error         string      `json:"error,omitempty"`
}

// SampleValues represents sample distinct values for a column.
type SampleValues struct {
	ColumnName string        `json:"column_name"`
	TableName  string        `json:"table_name"`
	Values     []interface{} `json:"values"`
	Error      string        `json:"error,omitempty"`
}

// Selector interface for querying databases.
type Selector interface {
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error)
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

// getColumnStats retrieves statistics for a column.
func getColumnStats(ctx context.Context, connectionName, tableName, columnName string) *ColumnStats {
	manager, err := getConnectionManager(ctx)
	if err != nil {
		return &ColumnStats{TableName: tableName, ColumnName: columnName, Error: err.Error()}
	}

	conn := manager.GetConnection(connectionName)
	if conn == nil {
		return &ColumnStats{TableName: tableName, ColumnName: columnName, Error: fmt.Sprintf("connection '%s' not found", connectionName)}
	}

	selector, ok := conn.(Selector)
	if !ok {
		return &ColumnStats{TableName: tableName, ColumnName: columnName, Error: fmt.Sprintf("connection '%s' does not support queries", connectionName)}
	}

	// Build stats query
	statsQuery := fmt.Sprintf(`
		SELECT
			COUNT(*) as total_rows,
			COUNT(*) - COUNT(%s) as null_count,
			COUNT(DISTINCT %s) as distinct_count,
			MIN(%s) as min_value,
			MAX(%s) as max_value
		FROM %s
	`, columnName, columnName, columnName, columnName, tableName)

	q := &query.Query{Query: statsQuery}
	result, err := selector.Select(ctx, q)
	if err != nil {
		return &ColumnStats{TableName: tableName, ColumnName: columnName, Error: fmt.Sprintf("failed to query stats: %v", err)}
	}

	stats := &ColumnStats{
		TableName:  tableName,
		ColumnName: columnName,
	}

	if len(result) > 0 && len(result[0]) >= 5 {
		row := result[0]
		if v, ok := row[0].(int64); ok {
			stats.TotalRows = v
		}
		if v, ok := row[1].(int64); ok {
			stats.NullCount = v
		}
		if v, ok := row[2].(int64); ok {
			stats.DistinctCount = v
		}
		stats.MinValue = row[3]
		stats.MaxValue = row[4]
	}

	return stats
}

// getSampleColumnValues retrieves sample distinct values for a column.
func getSampleColumnValues(ctx context.Context, connectionName, tableName, columnName string, limit int) *SampleValues {
	if limit <= 0 {
		limit = 20
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

	case "bruin_get_column_stats":
		connectionName, _ := args["connection"].(string)
		tableName, _ := args["table"].(string)
		columnName, _ := args["column"].(string)
		if connectionName == "" || tableName == "" || columnName == "" {
			return formatDBError("get_column_stats", fmt.Errorf("connection, table, and column parameters are required")), nil
		}
		stats := getColumnStats(ctx, connectionName, tableName, columnName)
		jsonBytes, _ := json.MarshalIndent(stats, "", "  ")
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
			"name":        "bruin_get_column_stats",
			"description": "Get statistics for a specific column including total rows, null count, distinct count, min/max values. Use this to determine appropriate data quality checks (e.g., not_null if null_count is 0, unique if distinct_count equals total_rows).",
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
						"description": "Column name to analyze",
					},
				},
				"required": []string{"connection", "table", "column"},
			},
		},
		{
			"name":        "bruin_sample_column_values",
			"description": "Get sample distinct values for a column. Use this to suggest accepted_values checks for enum-like columns (e.g., status, type, category columns).",
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
		"bruin_list_connections":    true,
		"bruin_get_table_schema":    true,
		"bruin_get_column_stats":    true,
		"bruin_sample_column_values": true,
	}
	return dbTools[toolName]
}
