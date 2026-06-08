package sail

import (
	"context"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/devenv"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/pkg/errors"
)

type materializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
	LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error
}

type devEnv interface {
	Modify(ctx context.Context, p *pipeline.Pipeline, a *pipeline.Asset, q *query.Query) (*query.Query, error)
	RegisterAssetForSchemaCache(ctx context.Context, p *pipeline.Pipeline, a *pipeline.Asset, q *query.Query) error
}

type BasicOperator struct {
	connection   config.ConnectionGetter
	extractor    query.QueryExtractor
	materializer materializer
	devEnv       devEnv
}

func NewBasicOperator(conn config.ConnectionGetter, extractor query.QueryExtractor, fullRefresh bool, hoister pipeline.DeclareHoister, parser *sqlparser.SQLParser) *BasicOperator {
	return &BasicOperator{
		connection: conn,
		extractor:  extractor,
		materializer: pipeline.HookWrapperMaterializer{
			Mat:     NewMaterializer(fullRefresh),
			Hoister: hoister,
		},
		devEnv: &devenv.DevEnvQueryModifier{
			// Sail speaks Spark SQL; the SQL parser has no Spark dialect, so we
			// reuse Trino (closest available) for dev-env rewrites.
			Dialect: "trino",
			Conn:    conn,
			Parser:  parser,
		},
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
		if t.Materialization.Strategy != pipeline.MaterializationStrategyDDL {
			return nil
		}
		queries = []*query.Query{{Query: ""}}
	}

	if len(queries) > 1 && t.Materialization.Type != pipeline.MaterializationTypeNone {
		return errors.New("cannot enable materialization for tasks with multiple queries")
	}

	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	rawConn := o.connection.GetConnection(connName)
	if rawConn == nil {
		return config.NewConnectionNotFoundError(ctx, "", connName)
	}

	conn, ok := rawConn.(*Client)
	if !ok {
		return errors.Errorf("connection '%s' is not a Sail connection", connName)
	}

	q := queries[0]
	materialized, err := o.materializer.Render(t, q.String())
	if err != nil {
		return err
	}
	writer := ctx.Value(executor.KeyPrinter)
	err = o.materializer.LogIfFullRefreshAndDDL(writer, t)
	if err != nil {
		return err
	}

	materializedQueries, err := extractor.ExtractQueriesFromString(materialized)
	if err != nil {
		return errors.Wrap(err, "cannot extract queries from materialized string")
	}

	var lastQuery *query.Query
	for _, queryObj := range materializedQueries {
		queryToRun := queryObj
		if o.devEnv != nil {
			queryToRun, err = o.devEnv.Modify(ctx, p, t, queryObj)
			if err != nil {
				return err
			}
		}

		ansisql.LogQueryIfVerbose(ctx, writer, queryToRun.Query)

		err = conn.RunQueryWithoutResult(ctx, queryToRun)
		if err != nil {
			return err
		}
		lastQuery = queryToRun
	}

	if o.devEnv == nil {
		return nil
	}

	if lastQuery == nil {
		return nil
	}

	err = o.devEnv.RegisterAssetForSchemaCache(ctx, p, t, lastQuery)
	if err != nil {
		return errors.Wrap(err, "cannot register asset for schema cache")
	}

	return nil
}
