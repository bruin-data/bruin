package diff

import (
	"context"
)

// TableSummarizer defines an interface for connections that can provide a summary of a table.
type TableSummarizer interface {
	GetTableSummary(ctx context.Context, tableName string) (*TableSummaryResult, error)
}
