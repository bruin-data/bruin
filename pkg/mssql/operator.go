package mssql

import (
	"context"

	"github.com/bruin-data/bruin/pkg/query"
)

type MsClient interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
}
