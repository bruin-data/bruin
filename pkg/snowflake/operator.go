package snowflake

import (
	"context"
	"io"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
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
	LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error
}

type SfClient interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	Ping(ctx context.Context) error
	SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error)
	CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error
	PushColumnDescriptions(ctx context.Context, asset *pipeline.Asset) error
	RecreateTableOnMaterializationTypeMismatch(ctx context.Context, asset *pipeline.Asset) error
	SelectOnlyLastResult(ctx context.Context, query *query.Query) ([][]interface{}, error)
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

	conn, ok := o.connection.GetConnection(connName).(SfClient)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not a snowflake connection", connName)
	}

	if t.Materialization.Type != pipeline.MaterializationTypeNone {
		err = conn.CreateSchemaIfNotExist(ctx, t)
		if err != nil {
			return err
		}
	}

	if o.materializer.IsFullRefresh() {
		err = conn.RecreateTableOnMaterializationTypeMismatch(ctx, t)
		if err != nil {
			return err
		}
	}

	ansisql.LogQueryIfVerbose(ctx, writer, q.Query)

	return conn.RunQueryWithoutResult(ctx, q)
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

type QuerySensor struct {
	connection     config.ConnectionGetter
	extractor      query.QueryExtractor
	secondsToSleep int64
}

func NewQuerySensor(conn config.ConnectionGetter, extractor query.QueryExtractor, secondsToSleep int64) *QuerySensor {
	return &QuerySensor{
		connection:     conn,
		extractor:      extractor,
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

	conn, ok := o.connection.GetConnection(connName).(SfClient)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not a snowflake connection", connName)
	}

	for {
		res, err := conn.SelectOnlyLastResult(ctx, qry[0])
		if err != nil {
			return err
		}

		intRes, err := helpers.CastResultToInteger(res, true)
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

type MetadataOperator struct {
	connection config.ConnectionGetter
}

func NewMetadataPushOperator(conn config.ConnectionGetter) *MetadataOperator {
	return &MetadataOperator{
		connection: conn,
	}
}

func (o *MetadataOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	connName, err := ti.GetPipeline().GetConnectionNameForAsset(ti.GetAsset())
	if err != nil {
		return err
	}

	client, ok := o.connection.GetConnection(connName).(SfClient)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not a snowflake connection", connName)
	}

	writer := ctx.Value(executor.KeyPrinter).(io.Writer)
	if writer == nil {
		return errors.New("no writer found in context, please create an issue for this: https://github.com/bruin-data/bruin/issues")
	}

	// Skip metadata push for views
	if ti.GetAsset().Materialization.Type == pipeline.MaterializationTypeView {
		_, _ = writer.Write([]byte("\"Skipping metadata update: Column comments are not supported for Views.\n"))
		return nil
	}

	err = client.PushColumnDescriptions(ctx, ti.GetAsset())
	if err != nil {
		_, _ = writer.Write([]byte("Failed to push metadata to Snowflake, skipping...\n"))
		return err
	}

	return nil
}
