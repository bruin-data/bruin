package fabric_warehouse

import (
	"context"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type materializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
}

type BasicOperator struct {
	connection   config.ConnectionGetter
	extractor    query.QueryExtractor
	materializer materializer
}

func NewBasicOperator(conn config.ConnectionGetter, extractor query.QueryExtractor, materializer materializer) *BasicOperator {
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
	extractor, err := o.extractor.CloneForAsset(ctx, p, t)
	if err != nil {
		return errors.Wrapf(err, "failed to clone extractor for asset %s", t.Name)
	}

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
	materialized, err := o.materializer.Render(t, q.String())
	if err != nil {
		return err
	}
	q.Query = materialized

	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	conn, ok := o.connection.GetConnection(connName).(*DB)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not a Fabric Warehouse connection", connName)
	}

	writer := ctx.Value(executor.KeyPrinter)
	ansisql.LogQueryIfVerbose(ctx, writer, q.Query)

	return conn.RunQueryWithoutResult(ctx, q)
}

func (o BasicOperator) GetConnection() (config.ConnectionGetter, bool) {
	return o.connection, true
}
