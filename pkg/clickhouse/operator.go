package clickhouse

import (
	"context"
	"github.com/pkg/errors"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

type materializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
}

type queryExtractor interface {
	ExtractQueriesFromString(content string) ([]*query.Query, error)
}

type ClickHouseClient interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
}

type connectionFetcher interface {
	GetClickHouseConnection(name string) (ClickHouseClient, error)
	GetConnection(name string) (interface{}, error)
}

type BasicOperator struct {
	connection   connectionFetcher
	extractor    queryExtractor
	materializer materializer
}

func (b BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	// TODO implement me
	panic("implement me")
}

func (o BasicOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	queries, err := o.extractor.ExtractQueriesFromString(t.ExecutableFile.Content)
	if err != nil {
		return errors.Wrap(err, "cannot extract queries from the task file")
	}

	if len(queries) == 0 {
		return nil
	}

	if len(queries) > 1 && t.Materialization.Type != pipeline.MaterializationTypeNone {
		return errors.New("cannot enable materialization for tasks with multiple queries")
	}

	q := queries[0]
	materialized, err := o.materializer.Render(t, q.String())
	if err != nil {
		return err
	}

	q.Query = materialized
	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	conn, err := o.connection.GetClickHouseConnection(connName)
	if err != nil {
		return err
	}

	return conn.RunQueryWithoutResult(ctx, q)
}

func NewBasicOperator(conn connectionFetcher, extractor queryExtractor, materializer materializer) *BasicOperator {
	return &BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: materializer,
	}
}
