package diff

import (
	"context"
)

// TableSummarizer defines an interface for connections that can provide a summary of a table.
type TableSummarizer interface {
	GetTableSummary(ctx context.Context, tableName string, schemaOnly bool) (*TableSummaryResult, error)
}

// SchemalessSummarizer is implemented by TableSummarizers whose schema is
// inferred from the underlying data (e.g. MongoDB) rather than declared in a
// catalog. For such sources, data-diff omits ALTER TABLE statement generation,
// which has no meaning without a fixed, DDL-managed schema.
type SchemalessSummarizer interface {
	// IsSchemaless reports whether the source lacks a fixed, catalog-defined schema.
	IsSchemaless() bool
}

type CostEstimator interface {
	EstimateTableDiffCost(ctx context.Context, tableName string, schemaOnly bool) (*TableDiffCostEstimate, error)
}

type QueryCostEstimate struct {
	QueryType      string `json:"queryType"`
	Query          string `json:"query"`
	BytesProcessed int64  `json:"bytesProcessed"`
	BytesBilled    int64  `json:"bytesBilled"`
}

type TableDiffCostEstimate struct {
	TableName           string               `json:"tableName"`
	Queries             []*QueryCostEstimate `json:"queries"`
	TotalBytesProcessed int64                `json:"totalBytesProcessed"`
	TotalBytesBilled    int64                `json:"totalBytesBilled"`
	Supported           bool                 `json:"supported"`
	UnsupportedReason   string               `json:"unsupportedReason,omitempty"`
}

type DiffCostEstimate struct {
	SourceTable         *TableDiffCostEstimate `json:"sourceTable"`
	TargetTable         *TableDiffCostEstimate `json:"targetTable"`
	TotalBytesProcessed int64                  `json:"totalBytesProcessed"`
	TotalBytesBilled    int64                  `json:"totalBytesBilled"`
}
