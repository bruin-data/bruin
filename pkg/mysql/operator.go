package mysql

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

type MySQLClient interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error)
	Ping(ctx context.Context) error
	GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error)
	CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error
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

func NewBasicOperator(conn config.ConnectionGetter, extractor query.QueryExtractor, materializer materializer, parser *sqlparser.SQLParser) *BasicOperator {
	return &BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: materializer,
		devEnv: &devenv.DevEnvQueryModifier{
			Dialect: "mysql",
			Conn:    conn,
			Parser:  parser,
		},
	}
}

func (o BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o BasicOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) error {
	extractor, err := o.extractor.CloneForAsset(ctx, p, asset)
	if err != nil {
		return errors.Wrapf(err, "failed to clone extractor for asset %s", asset.Name)
	}

	queries, err := extractor.ExtractQueriesFromString(asset.ExecutableFile.Content)
	if err != nil {
		return errors.Wrap(err, "cannot extract queries from the task file")
	}

	if len(queries) == 0 {
		return nil
	}

	if len(queries) > 1 && asset.Materialization.Type != pipeline.MaterializationTypeNone {
		return errors.New("MySQL operator can only handle a single query when materialization is enabled")
	}

	q := queries[0]
	materialized, err := o.materializer.Render(asset, q.String())
	if err != nil {
		return err
	}

	writer := ctx.Value(executor.KeyPrinter)
	if err := o.materializer.LogIfFullRefreshAndDDL(writer, asset); err != nil {
		return err
	}

	q.Query = materialized

	if asset.Materialization.Strategy == pipeline.MaterializationStrategyTimeInterval {
		renderedQueries, err := extractor.ExtractQueriesFromString(materialized)
		if err != nil {
			return errors.Wrap(err, "cannot re-extract rendered query for time_interval strategy")
		}
		if len(renderedQueries) == 0 {
			return errors.New("rendered queries unexpectedly empty")
		}
		q.Query = renderedQueries[0].Query
	}

	connName, err := p.GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}

	conn, ok := o.connection.GetConnection(connName).(MySQLClient)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not a MySQL connection", connName)
	}

	if asset.Materialization.Type != pipeline.MaterializationTypeNone {
		if err := conn.CreateSchemaIfNotExist(ctx, asset); err != nil {
			return errors.Wrap(err, "failed to ensure schema exists")
		}
	}

	if o.devEnv == nil {
		ansisql.LogQueryIfVerbose(ctx, writer, q.Query)
		return conn.RunQueryWithoutResult(ctx, q)
	}

	q, err = o.devEnv.Modify(ctx, p, asset, q)
	if err != nil {
		return err
	}

	ansisql.LogQueryIfVerbose(ctx, writer, q.Query)

	if err := conn.RunQueryWithoutResult(ctx, q); err != nil {
		return err
	}

	if err := o.devEnv.RegisterAssetForSchemaCache(ctx, p, asset, q); err != nil {
		return errors.Wrap(err, "cannot register asset for schema cache")
	}

	return nil
}

func NewColumnCheckOperator(manager config.ConnectionGetter) *ansisql.ColumnCheckOperator {
	return ansisql.NewColumnCheckOperator(map[string]ansisql.CheckRunner{
		"not_null":        ansisql.NewNotNullCheck(manager),
		"unique":          ansisql.NewUniqueCheck(manager),
		"positive":        ansisql.NewPositiveCheck(manager),
		"non_negative":    ansisql.NewNonNegativeCheck(manager),
		"negative":        ansisql.NewNegativeCheck(manager),
		"min":             ansisql.NewMinCheck(manager),
		"max":             ansisql.NewMaxCheck(manager),
		"accepted_values": &AcceptedValuesCheck{conn: manager},
		"pattern":         &PatternCheck{conn: manager},
	})
}
