package mongoatlas

import (
	"context"

	"github.com/bruin-data/bruin/pkg/diff"
	bruinmongo "github.com/bruin-data/bruin/pkg/mongo"
)

// GetTableSummary implements diff.TableSummarizer for MongoDB Atlas collections.
// It reuses the shared MongoDB summarizer; the table identifier is a collection
// name, optionally qualified as "database.collection".
func (db *DB) GetTableSummary(ctx context.Context, tableName string, schemaOnly bool) (*diff.TableSummaryResult, error) {
	if err := db.ensureClient(ctx); err != nil {
		return nil, err
	}
	return bruinmongo.BuildTableSummary(ctx, db.client, db.config.Database, tableName, schemaOnly, diff.SampleSizeFromContext(ctx))
}
