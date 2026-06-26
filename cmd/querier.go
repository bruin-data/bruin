package cmd

import (
	"context"

	"github.com/bruin-data/bruin/pkg/query"
)

// schemaQuerier is a connection that can run a query and return its typed
// columns and rows. Several commands (fetch, import, patch, test) need exactly
// this capability, so they share one definition instead of each repeating the
// anonymous interface.
type schemaQuerier interface {
	SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error)
}
