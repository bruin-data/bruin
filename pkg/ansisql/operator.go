package ansisql

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

// TableExistsChecker defines the interface for checking if a table exists
type TableExistsChecker interface {
	Select(ctx context.Context, q *query.Query) ([][]interface{}, error)
	BuildTableExistsQuery(tableName string) (string, error)
}

// PipelineProvider defines the interface for getting connection names from assets
type PipelineProvider interface {
	GetConnectionNameForAsset(asset *pipeline.Asset) (string, error)
}

// TimeProvider defines the interface for time-related operations (for testability)
type TimeProvider interface {
	Now() time.Time
	Sleep(duration time.Duration)
	After(duration time.Duration) <-chan time.Time
}

// DefaultTimeProvider implements TimeProvider with real time operations
type DefaultTimeProvider struct{}

func (d *DefaultTimeProvider) Now() time.Time {
	return time.Now()
}

func (d *DefaultTimeProvider) Sleep(duration time.Duration) {
	time.Sleep(duration)
}

func (d *DefaultTimeProvider) After(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}

// Printer defines the interface for output operations (for testability)
type Printer interface {
	Println(a ...interface{}) (n int, err error)
}

// ContextPrinter implements Printer by extracting writer from context
type ContextPrinter struct {
	ctx context.Context
}

func NewContextPrinter(ctx context.Context) *ContextPrinter {
	return &ContextPrinter{ctx: ctx}
}

func (c *ContextPrinter) Println(a ...interface{}) (n int, err error) {
	if writer, ok := c.ctx.Value(executor.KeyPrinter).(io.Writer); ok {
		return fmt.Fprintln(writer, a...)
	}
	return 0, nil
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

	conn := o.connection.GetConnection(connName)
	if conn == nil {
		return errors.Errorf("'%s' does not exist", connName)
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
			if querier, ok := conn.(interface {
				Select(ctx context.Context, q *query.Query) ([][]interface{}, error)
			}); ok {
				res, err := querier.Select(ctx, qry[0])
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
	connection   config.ConnectionGetter
	sensorMode   string
	extractor    query.QueryExtractor
	timeProvider TimeProvider
	printer      Printer
}

// NewTableSensor creates a new TableSensor with default dependencies
func NewTableSensor(conn config.ConnectionGetter, sensorMode string, extractor query.QueryExtractor) *TableSensor {
	return &TableSensor{
		connection:   conn,
		sensorMode:   sensorMode,
		extractor:    extractor,
		timeProvider: &DefaultTimeProvider{},
	}
}

// NewTableSensorWithDependencies creates a new TableSensor with custom dependencies for testing
func NewTableSensorWithDependencies(
	conn config.ConnectionGetter,
	sensorMode string,
	extractor query.QueryExtractor,
	timeProvider TimeProvider,
	printer Printer,
) *TableSensor {
	return &TableSensor{
		connection:   conn,
		sensorMode:   sensorMode,
		extractor:    extractor,
		timeProvider: timeProvider,
		printer:      printer,
	}
}

func (ts *TableSensor) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return ts.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (ts *TableSensor) RunTask(ctx context.Context, p PipelineProvider, t *pipeline.Asset) error {
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

	conn := ts.connection.GetConnection(connName)
	if conn == nil {
		return errors.Errorf("'%s' does not exist", connName)
	}

	// Build the table exists query
	qq, err := conn.BuildTableExistsQuery(tableName)
	if err != nil {
		return errors.Wrap(err, "failed to build table exists query")
	}

	// Extract the query
	extractedQueries, err := ts.extractor.ExtractQueriesFromString(qq)
	if err != nil {
		return errors.Wrap(err, "failed to extract table exists query")
	}

	if len(extractedQueries) == 0 {
		return errors.New("no queries extracted from table exists query")
	}

	extractedQuery := extractedQueries[0]

	// Set up printer
	printer := ts.printer
	if printer == nil {
		printer = NewContextPrinter(ctx)
	}

	printer.Println("Poking:", tableName)

	// Run the sensor loop
	return ts.runSensorLoop(ctx, tableChecker, extractedQuery, t, printer)
}

// runSensorLoop contains the main sensor logic, separated for testability
func (ts *TableSensor) runSensorLoop(
	ctx context.Context,
	tableChecker TableExistsChecker,
	extractedQuery *query.Query,
	t *pipeline.Asset,
	printer Printer,
) error {
	timeout := ts.timeProvider.After(24 * time.Hour)

	for {
		select {
		case <-timeout:
			return errors.New("Sensor timed out after 24 hours")
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Check if table exists
			res, err := tableChecker.Select(ctx, extractedQuery)
			if err != nil {
				return errors.Wrap(err, "failed to check table existence")
			}

			intRes, err := helpers.CastResultToInteger(res)
			if err != nil {
				return errors.Wrap(err, "failed to parse table sensor result")
			}

			// If table exists, we're done
			if intRes > 0 {
				return nil
			}

			// If sensor mode is "once", fail immediately
			if ts.sensorMode == "once" || ts.sensorMode == "" {
				return errors.New("Sensor didn't return the expected result")
			}

			// Otherwise, wait and retry
			pokeInterval := helpers.GetPokeInterval(ctx, t)
			ts.timeProvider.Sleep(time.Duration(pokeInterval) * time.Second)
			printer.Println("Info: Sensor didn't return the expected result, waiting for", pokeInterval, "seconds")
		}
	}
}
