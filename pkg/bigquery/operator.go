package bigquery

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
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
	IsFullRefresh() bool
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

func NewBasicOperator(conn config.ConnectionGetter, extractor query.QueryExtractor, materializer materializer, parser *sqlparser.SQLParser) *BasicOperator {
	return &BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: materializer,
		devEnv: &devenv.DevEnvQueryModifier{
			Dialect: "bigquery",
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

	conn, ok := o.connection.GetConnection(connName).(DB)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not a bigquery connection", connName)
	}

	if t.Materialization.Type != pipeline.MaterializationTypeNone {
		if err := conn.CreateDataSetIfNotExist(t, ctx); err != nil {
			return err
		}
	}

	if o.materializer.IsFullRefresh() {
		err = conn.DropTableOnMismatch(ctx, t.Name, t)
		if err != nil {
			return errors.Wrapf(err, "failed to check for mismatches for table '%s'", t.Name)
		}
	}

	// Print SQL query in verbose mode
	if verbose := ctx.Value(executor.KeyVerbose); verbose != nil && verbose.(bool) {
		if w, ok := writer.(io.Writer); ok {
			queryPreview := strings.TrimSpace(q.Query)
			if len(queryPreview) > 5000 {
				queryPreview = queryPreview[:5000] + "\n... (truncated)"
			}
			fmt.Fprintf(w, "Executing SQL query:\n%s\n\n", queryPreview)
		}
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

type checkRunner interface {
	Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error
}

type ColumnCheckOperator struct {
	checkRunners map[string]checkRunner
}

func NewColumnCheckOperator(manager config.ConnectionGetter) (*ColumnCheckOperator, error) {
	return &ColumnCheckOperator{
		checkRunners: map[string]checkRunner{
			"not_null":        ansisql.NewNotNullCheck(manager),
			"unique":          ansisql.NewUniqueCheck(manager),
			"positive":        ansisql.NewPositiveCheck(manager),
			"non_negative":    ansisql.NewNonNegativeCheck(manager),
			"negative":        ansisql.NewNegativeCheck(manager),
			"min":             ansisql.NewMinCheck(manager),
			"max":             ansisql.NewMaxCheck(manager),
			"accepted_values": &AcceptedValuesCheck{conn: manager},
			"pattern":         &PatternCheck{conn: manager},
		},
	}, nil
}

func (o ColumnCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	test, ok := ti.(*scheduler.ColumnCheckInstance)
	if !ok {
		return errors.New("cannot run a non-column check instance")
	}

	executor, ok := o.checkRunners[test.Check.Name]
	if !ok {
		return errors.New("there is no executor configured for the check type, check cannot be run: " + test.Check.Name)
	}

	return executor.Check(ctx, test)
}

type MetadataPushOperator struct {
	connection config.ConnectionGetter
}

func NewMetadataPushOperator(conn config.ConnectionGetter) *MetadataPushOperator {
	return &MetadataPushOperator{
		connection: conn,
	}
}

func (o *MetadataPushOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	conn, err := ti.GetPipeline().GetConnectionNameForAsset(ti.GetAsset())
	if err != nil {
		return err
	}

	client, ok := o.connection.GetConnection(conn).(DB)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not a bigquery connection", conn)
	}

	writer := ctx.Value(executor.KeyPrinter).(io.Writer)
	if writer == nil {
		return errors.New("no writer found in context, please create an issue for this: https://github.com/bruin-data/bruin/issues")
	}

	err = client.UpdateTableMetadataIfNotExist(ctx, ti.GetAsset())
	if err != nil {
		var noMetadata NoMetadataUpdatedError
		if errors.As(err, &noMetadata) {
			_, _ = writer.Write([]byte("No metadata found to be pushed to BigQuery, skipping...\n"))
			return nil
		}

		return err
	}

	return nil
}

type QuerySensor struct {
	connection config.ConnectionGetter
	extractor  query.QueryExtractor
	sensorMode string
}

func NewQuerySensor(conn config.ConnectionGetter, extractor query.QueryExtractor, sensorMode string) *QuerySensor {
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
	extractor, err := o.extractor.CloneForAsset(ctx, p, t)
	if err != nil {
		return errors.Wrapf(err, "failed to clone extractor for asset %s", t.Name)
	}

	qry, err := extractor.ExtractQueriesFromString(qq)
	if err != nil {
		return errors.Wrap(err, "failed to render query sensor query")
	}
	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	conn, ok := o.connection.GetConnection(connName).(DB)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not a bigquery connection", connName)
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

type TableSensor struct {
	connection config.ConnectionGetter
	sensorMode string
	extractor  query.QueryExtractor
}

func NewTableSensor(conn config.ConnectionGetter, sensorMode string, extractor query.QueryExtractor) *TableSensor {
	return &TableSensor{
		connection: conn,
		sensorMode: sensorMode,
		extractor:  extractor,
	}
}

func (ts *TableSensor) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return ts.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (ts *TableSensor) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	if ts.sensorMode == "skip" {
		return nil
	}
	tableName, ok := t.Parameters["table"]
	if !ok {
		return errors.New("table sensor requires a parameter named 'table'")
	}

	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	conn, ok := ts.connection.GetConnection(connName).(DB)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not a bigquery connection", connName)
	}

	qq, err := conn.BuildTableExistsQuery(tableName)
	if err != nil {
		return err
	}
	extractedQueries, err := ts.extractor.ExtractQueriesFromString(qq)
	if err != nil {
		return err
	}
	extractedQuery := extractedQueries[0]
	printer, printerExists := ctx.Value(executor.KeyPrinter).(io.Writer)
	if printerExists {
		fmt.Fprintln(printer, "Poking:", tableName)
	}

	timeout := time.After(24 * time.Hour)
	for {
		select {
		case <-timeout:
			return errors.New("Sensor timed out after 24 hours")
		default:
			res, err := conn.Select(ctx, extractedQuery)
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
			if ts.sensorMode == "once" || ts.sensorMode == "" {
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
