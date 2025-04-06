package bigquery

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type materializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
	IsFullRefresh() bool
}

type queryExtractor interface {
	ExtractQueriesFromString(filepath string) ([]*query.Query, error)
}

type connectionFetcher interface {
	GetBqConnection(name string) (DB, error)
	GetConnection(name string) (interface{}, error)
}

type BasicOperator struct {
	connection   connectionFetcher
	extractor    queryExtractor
	materializer materializer
}

func NewBasicOperator(conn connectionFetcher, extractor queryExtractor, materializer materializer) *BasicOperator {
	return &BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: materializer,
	}
}

func (o BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	o.extractor = helpers.SetNewRenderer(ctx, ti.GetAsset())
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
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
	if t.Materialization.Strategy == pipeline.MaterializationStrategyTimeInterval {
		renderedQueries, err := o.extractor.ExtractQueriesFromString(materialized)
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

	conn, err := o.connection.GetBqConnection(connName)
	if err != nil {
		return err
	}
	if err := conn.CreateDataSetIfNotExist(t, ctx); err != nil {
		return err
	}

	if o.materializer.IsFullRefresh() {
		err = conn.DropTableOnMismatch(ctx, t.Name, t)
		if err != nil {
			return errors.Wrapf(err, "failed to check for mismatches for table '%s'", t.Name)
		}
	}

	return conn.RunQueryWithoutResult(ctx, q)
}

type checkRunner interface {
	Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error
}

type ColumnCheckOperator struct {
	checkRunners map[string]checkRunner
}

func NewColumnCheckOperator(manager connectionFetcher) (*ColumnCheckOperator, error) {
	return &ColumnCheckOperator{
		checkRunners: map[string]checkRunner{
			"not_null":        ansisql.NewNotNullCheck(manager),
			"unique":          ansisql.NewUniqueCheck(manager),
			"positive":        ansisql.NewPositiveCheck(manager),
			"non_negative":    ansisql.NewNonNegativeCheck(manager),
			"negative":        ansisql.NewNegativeCheck(manager),
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
	connection connectionFetcher
}

func NewMetadataPushOperator(conn connectionFetcher) *MetadataPushOperator {
	return &MetadataPushOperator{
		connection: conn,
	}
}

func (o *MetadataPushOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	conn, err := ti.GetPipeline().GetConnectionNameForAsset(ti.GetAsset())
	if err != nil {
		return err
	}

	client, err := o.connection.GetBqConnection(conn)
	if err != nil {
		return err
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

type renderer interface {
	Render(query string) (string, error)
}

type QuerySensor struct {
	connection connectionFetcher
	renderer   renderer
	sensorMode string
}

func NewQuerySensor(conn connectionFetcher, renderer renderer, sensorMode string) *QuerySensor {
	return &QuerySensor{
		connection: conn,
		renderer:   renderer,
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

	qq, err := o.renderer.Render(qq)
	if err != nil {
		return errors.Wrap(err, "failed to render query sensor query")
	}
	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	conn, err := o.connection.GetBqConnection(connName)
	if err != nil {
		return err
	}

	trimmedQuery := helpers.TrimToLength(qq, 50)
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
			res, err := conn.Select(ctx, &query.Query{Query: qq})
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
	connection connectionFetcher
	sensorMode string
}

func NewTableSensor(conn connectionFetcher, sensorMode string) *TableSensor {
	return &TableSensor{
		connection: conn,
		sensorMode: sensorMode,
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

	conn, err := ts.connection.GetBqConnection(connName)
	if err != nil {
		return err
	}

	qq, err := conn.BuildTableExistsQuery(tableName)
	if err != nil {
		return err
	}

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
			res, err := conn.Select(ctx, &query.Query{Query: qq})
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
