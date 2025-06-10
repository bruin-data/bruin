package postgres

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/devenv"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/helpers"
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

type PgClient interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error)
	Ping(ctx context.Context) error
	GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error)
	CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error
}

type connectionFetcher interface {
	GetPgConnection(name string) (PgClient, error)
	GetConnection(name string) (interface{}, error)
}

type devEnv interface {
	Modify(ctx context.Context, p *pipeline.Pipeline, a *pipeline.Asset, q *query.Query) (*query.Query, error)
	RegisterAssetForSchemaCache(ctx context.Context, p *pipeline.Pipeline, a *pipeline.Asset, q *query.Query) error
}

type BasicOperator struct {
	connection   connectionFetcher
	extractor    query.QueryExtractor
	materializer materializer
	devEnv       devEnv
}

func NewBasicOperator(conn connectionFetcher, extractor query.QueryExtractor, materializer materializer, parser *sqlparser.SQLParser) *BasicOperator {
	return &BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: materializer,
		devEnv: &devenv.DevEnvQueryModifier{
			Dialect: "postgres",
			Conn:    conn,
			Parser:  parser,
		},
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

	if len(queries) > 1 {
		// if the code hits here this means that we used an extractor that splits the content into multiple queries.
		// this is not supported and we should not allow it, Postgres-like platforms accepts multiple SQL statements in a single query, that's what we use instead.
		return errors.New("Postgres-like operators can only handle one query at a time, this seems like a bug, please report the issue to the Bruin team")
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

	conn, err := o.connection.GetPgConnection(connName)
	if err != nil {
		return err
	}

	err = conn.CreateSchemaIfNotExist(ctx, t)
	if err != nil {
		return err
	}

	if o.devEnv == nil {
		return conn.RunQueryWithoutResult(ctx, q)
	}

	q, err = o.devEnv.Modify(ctx, p, t, q)
	if err != nil {
		return err
	}

	err = conn.RunQueryWithoutResult(ctx, q)
	if err != nil {
		return err
	}

	err = o.devEnv.RegisterAssetForSchemaCache(ctx, p, t, q)
	if err != nil {
		return errors.Wrap(err, "cannot register asset for schema cache")
	}

	return nil
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

type QuerySensor struct {
	connection connectionFetcher
	extractor  query.QueryExtractor
	sensorMode string
}

func NewQuerySensor(conn connectionFetcher, extractor query.QueryExtractor, sensorMode string) *QuerySensor {
	return &QuerySensor{
		connection: conn,
		extractor:  extractor,
		sensorMode: sensorMode,
	}
}

func (o *QuerySensor) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o *QuerySensor) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	if o.sensorMode == "skip" {
		return nil
	}
	qq, ok := t.Parameters["query"]
	if !ok {
		return errors.New("query sensor requires a parameter named 'query'")
	}
	extractor := o.extractor.CloneForAsset(ctx, p, t)

	qry, err := extractor.ExtractQueriesFromString(qq)
	if err != nil {
		return errors.Wrap(err, "failed to render query sensor query")
	}
	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	conn, err := o.connection.GetPgConnection(connName)
	if err != nil {
		return err
	}

	trimmedQuery := helpers.TrimToLength(qry[0].Query, 50)
	printer, printerExists := ctx.Value(executor.KeyPrinter).(io.Writer)
	if printerExists {
		fmt.Fprintln(printer, "Poking:", trimmedQuery)
	}

	timeout := time.After(24 * time.Hour)
	for {
		select {
		case <-timeout:
			return errors.New("Sensor timed out after 24 hours")
		default:
			res, err := conn.Select(ctx, qry[0])
			if err != nil {
				return err
			}
			intRes, err := helpers.CastResultToInteger(res)
			if err != nil {
				return errors.Wrap(err, "failed to parse query sensor result")
			}

			if intRes > 0 {
				return nil
			}
			if o.sensorMode == "once" || o.sensorMode == "" {
				return errors.New("Sensor didn't return the expected result")
			}

			pokeInterval := helpers.GetPokeInterval(ctx, t)
			time.Sleep(time.Duration(pokeInterval) * time.Second)
			if printerExists {
				fmt.Fprintln(printer, "Info: Sensor didn't return the expected result, waiting for", pokeInterval, "seconds")
			}
		}
	}
}
