package postgres

import (
	"context"

	"github.com/bruin-data/bruin/pkg/query"
)

type PgClient interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
}
