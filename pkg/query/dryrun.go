package query

import "context"

// DryRunColumn represents a single column in the output schema of a dry-run result.
type DryRunColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// DryRunResult holds the output of a dry-run operation.
// Different databases populate different fields depending on their capabilities.
type DryRunResult struct {
	// ConnectionType identifies the database backend (e.g., "bigquery", "snowflake", "postgres").
	ConnectionType string `json:"connectionType"`

	// Valid indicates whether the query passed validation without errors.
	Valid bool `json:"valid"`

	// BigQuery-specific fields
	TotalBytesProcessed int64          `json:"totalBytesProcessed,omitempty"`
	EstimatedCostUSD    float64        `json:"estimatedCostUSD,omitempty"`
	StatementType       string         `json:"statementType,omitempty"`
	ReferencedTables    []string       `json:"referencedTables,omitempty"`
	Schema              []DryRunColumn `json:"schema,omitempty"`

	// EXPLAIN-based databases populate this with the query plan output.
	ExplainRows *QueryResult `json:"explainRows,omitempty"`
}

// QueryExecutionSummary holds metadata for a successfully executed statement.
// Databases populate this when a statement does not return a result set.
type QueryExecutionSummary struct {
	ConnectionType        string         `json:"connectionType,omitempty"`
	JobID                 string         `json:"jobId,omitempty"`
	StatementType         string         `json:"statementType,omitempty"`
	TotalBytesProcessed   int64          `json:"totalBytesProcessed,omitempty"`
	TotalBytesBilled      int64          `json:"totalBytesBilled,omitempty"`
	SlotMillis            int64          `json:"slotMillis,omitempty"`
	DMLAffectedRows       *int64         `json:"dmlAffectedRows,omitempty"`
	DMLStats              *DMLStatistics `json:"dmlStats,omitempty"`
	DDLOperationPerformed string         `json:"ddlOperationPerformed,omitempty"`
	DDLTargetTable        string         `json:"ddlTargetTable,omitempty"`
	DDLTargetRoutine      string         `json:"ddlTargetRoutine,omitempty"`
}

type DMLStatistics struct {
	InsertedRowCount int64 `json:"insertedRowCount,omitempty"`
	DeletedRowCount  int64 `json:"deletedRowCount,omitempty"`
	UpdatedRowCount  int64 `json:"updatedRowCount,omitempty"`
}

// QueryDryRunner is an optional interface that database connections can implement
// to support dry-run / explain functionality for the "bruin query --dry-run" command.
type QueryDryRunner interface {
	DryRunQuery(ctx context.Context, q *Query) (*DryRunResult, error)
}
