package athena

import (
	"context"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type materializer interface {
	Render(task *pipeline.Asset, query, location string) ([]string, error)
}

type Client interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	GetResultsLocation() string
	Ping(ctx context.Context) error
	SelectWithSchema(ctx context.Context, queryObject *query.Query) (*query.QueryResult, error)
}

type queryExtractor interface {
	ExtractQueriesFromString(content string) ([]*query.Query, error)
	ExtractQueriesFromSlice(content []string) ([]*query.Query, error)
}

type connectionFetcher interface {
	GetAthenaConnectionWithoutDefault(name string) (Client, error)
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
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o BasicOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	conn, err := o.connection.GetAthenaConnectionWithoutDefault(connName)
	if err != nil {
		return err
	}

	materializedQueries, err := o.materializer.Render(t, t.ExecutableFile.Content, conn.GetResultsLocation())
	if err != nil {
		return errors.Wrap(err, "cannot extract queries from the task file")
	}
	queries, err := o.extractor.ExtractQueriesFromSlice(materializedQueries)
	if err != nil {
		return errors.Wrap(err, "cannot extract queries from the task file")
	}

	if len(queries) == 0 {
		return nil
	}

	for _, q := range queries {
		err = conn.RunQueryWithoutResult(ctx, q)
		if err != nil {
			return err
		}
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

type renderer interface {
	Render(query string) (string, error)
}

type QuerySensor struct {
	connection     connectionFetcher
	renderer       renderer
	secondsToSleep int64
}

func NewQuerySensor(conn connectionFetcher, renderer renderer, secondsToSleep int64) *QuerySensor {
	return &QuerySensor{
		connection:     conn,
		renderer:       renderer,
		secondsToSleep: secondsToSleep,
	}
}

func (o *QuerySensor) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o *QuerySensor) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
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

	conn, err := o.connection.GetAthenaConnectionWithoutDefault(connName)
	if err != nil {
		return err
	}

	for {
		res, err := conn.Select(ctx, &query.Query{Query: qq})
		if err != nil {
			return err
		}

		intRes, err := helpers.CastResultToInteger(res)
		if err != nil {
			return errors.Wrap(err, "failed to parse query sensor result")
		}

		if intRes > 0 {
			break
		}

		time.Sleep(time.Duration(o.secondsToSleep) * time.Second)
	}

	return nil
}
