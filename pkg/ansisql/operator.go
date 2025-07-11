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
	extractor := o.extractor.CloneForAsset(ctx, p, t)

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
