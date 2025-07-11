package duck

import (
	"context"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type materializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
	LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error
}

type DuckDBClient interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error)
	CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error
}

type connectionFetcher interface {
	GetDuckDBConnection(name string) (DuckDBClient, error)
	GetConnection(name string) (interface{}, error)
}

type BasicOperator struct {
	connection   connectionFetcher
	extractor    query.QueryExtractor
	materializer materializer
}

func NewBasicOperator(conn connectionFetcher, extractor query.QueryExtractor, materializer materializer) *BasicOperator {
	return &BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: materializer,
	}
}

func (o BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o BasicOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	extractor := o.extractor.CloneForAsset(ctx, p, t)
	queries, err := extractor.ExtractQueriesFromString(t.ExecutableFile.Content)
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
	writer := ctx.Value(executor.KeyPrinter)
	err = o.materializer.LogIfFullRefreshAndDDL(writer, t)
	if err != nil {
		return err
	}
	materialized, err := o.materializer.Render(t, q.String())
	if err != nil {
		return err
	}

	q.Query = materialized
	if t.Materialization.Strategy == pipeline.MaterializationStrategyTimeInterval {
		renderedQueries, err := extractor.ExtractQueriesFromString(materialized)
		if err != nil {
			return errors.Wrap(err, "cannot re-extract/render materialized query for time_interval strategy")
		}

		if len(renderedQueries) == 0 {
			return errors.New("rendered queries unexpectedly empty")
		}

		q.Query = renderedQueries[0].Query
	}
	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	conn, err := o.connection.GetDuckDBConnection(connName)
	if err != nil {
		return err
	}

	if t.Materialization.Type != pipeline.MaterializationTypeNone {
		err = conn.CreateSchemaIfNotExist(ctx, t)
		if err != nil {
			return err
		}
	}

	return conn.RunQueryWithoutResult(ctx, q)
}

func NewColumnCheckOperator(manager connectionFetcher) *ansisql.ColumnCheckOperator {
	return ansisql.NewColumnCheckOperator(map[string]ansisql.CheckRunner{
		"not_null":        ansisql.NewNotNullCheck(manager),
		"unique":          ansisql.NewUniqueCheck(manager),
		"positive":        ansisql.NewPositiveCheck(manager),
		"non_negative":    ansisql.NewNonNegativeCheck(manager),
		"negative":        ansisql.NewNegativeCheck(manager),
		"accepted_values": &AcceptedValuesCheck{conn: manager},
		"pattern":         &PatternCheck{conn: manager},
	})
}
