package mongoatlas

import (
	"context"

	"github.com/bruin-data/bruin/pkg/ansisql"
	bruinmongo "github.com/bruin-data/bruin/pkg/mongo"
	"github.com/bruin-data/bruin/pkg/query"
)

func (db *DB) Select(ctx context.Context, q *query.Query) ([][]interface{}, error) {
	res, err := db.SelectWithSchema(ctx, q)
	if err != nil {
		return nil, err
	}
	return res.Rows, nil
}

func (db *DB) SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error) {
	if err := db.ensureClient(ctx); err != nil {
		return nil, err
	}
	return bruinmongo.RunQuery(ctx, db.client, db.config.Database, q.Query)
}

func (db *DB) Limit(q string, limit int64) string {
	return bruinmongo.LimitQuery(q, limit)
}

// GetDatabaseSummary enumerates the databases and collections on the Atlas
// cluster so `bruin import database` can turn them into assets.
func (db *DB) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	if err := db.ensureClient(ctx); err != nil {
		return nil, err
	}
	return bruinmongo.DatabaseSummary(ctx, db.client, db.config.Database)
}
