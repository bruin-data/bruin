package trino

import (
	"context"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type BasicOperator struct {
	connection config.ConnectionGetter
	extractor  query.QueryExtractor
}

func NewBasicOperator(conn config.ConnectionGetter, extractor query.QueryExtractor) *BasicOperator {
	return &BasicOperator{
		connection: conn,
		extractor:  extractor,
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

	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	conn, ok := o.connection.GetConnection(connName).(*Client)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not a trino connection", connName)
	}

	// Execute each query
	for _, q := range queries {
		err = conn.RunQueryWithoutResult(ctx, q)
		if err != nil {
			return err
		}
	}

	return nil
}
