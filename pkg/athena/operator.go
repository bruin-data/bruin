package athena

import (
	"context"
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
	Render(task *pipeline.Asset, query, location string) ([]string, error)
	LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error
}

type initialTableMaterializer interface {
	RenderInitialTable(asset *pipeline.Asset, query, location string) ([]string, error)
}

type initialTableInsertionIndexer interface {
	InitialTableInsertionIndex(asset *pipeline.Asset) int
}

type tableExistsQueryBuilder interface {
	BuildTableExistsQuery(tableName string) (string, error)
}

type Client interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	GetResultsLocation() string
	Ping(ctx context.Context) error
	SelectWithSchema(ctx context.Context, queryObject *query.Query) (*query.QueryResult, error)
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
			Dialect: "athena",
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
	writer := ctx.Value(executor.KeyPrinter)
	err = o.materializer.LogIfFullRefreshAndDDL(writer, t)
	if err != nil {
		return err
	}
	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	rawConn := o.connection.GetConnection(connName)
	if rawConn == nil {
		return config.NewConnectionNotFoundError(ctx, "", connName)
	}

	conn, ok := rawConn.(Client)
	if !ok {
		return errors.Errorf("connection '%s' is not an athena connection", connName)
	}

	q := queries[0]
	materializedQueries, err := o.materializer.Render(t, q.String(), conn.GetResultsLocation())
	if err != nil {
		return err
	}

	if t.Materialization.Strategy == pipeline.MaterializationStrategyTimeInterval {
		materializedQueries, err = extractor.ReextractQueriesFromSlice(materializedQueries)
		if err != nil {
			return err
		}
	}

	if shouldInitializeIncrementalTable(t) && !isFullRefreshMaterializer(o.materializer) {
		initializer, canInitialize := o.materializer.(initialTableMaterializer)
		tableChecker, canCheck := conn.(tableExistsQueryBuilder)
		if canInitialize && canCheck {
			existsQuery, err := tableChecker.BuildTableExistsQuery(t.Name)
			if err != nil {
				return errors.Wrap(err, "cannot build incremental target existence query")
			}
			result, err := conn.Select(ctx, &query.Query{Query: existsQuery})
			if err != nil {
				return errors.Wrap(err, "cannot check whether incremental target exists")
			}
			count, err := helpers.CastResultToInteger(result, true)
			if err != nil {
				return errors.Wrap(err, "cannot parse incremental target existence result")
			}
			if count == 0 {
				initialQueries, err := initializer.RenderInitialTable(t, q.String(), conn.GetResultsLocation())
				if err != nil {
					return err
				}
				insertionIndex := 0
				if indexer, ok := o.materializer.(initialTableInsertionIndexer); ok {
					insertionIndex = indexer.InitialTableInsertionIndex(t)
				}
				if insertionIndex < 0 || insertionIndex > len(materializedQueries) {
					insertionIndex = 0
				}
				withInitialTable := make([]string, 0, len(initialQueries)+len(materializedQueries))
				withInitialTable = append(withInitialTable, materializedQueries[:insertionIndex]...)
				withInitialTable = append(withInitialTable, initialQueries...)
				withInitialTable = append(withInitialTable, materializedQueries[insertionIndex:]...)
				materializedQueries = withInitialTable
			}
		}
	}

	var lastQuery *query.Query
	for _, queryString := range materializedQueries {
		queryObj := &query.Query{Query: queryString}
		if o.devEnv != nil {
			queryObj, err = o.devEnv.Modify(ctx, p, t, queryObj)
			if err != nil {
				return err
			}
		}

		ansisql.LogQueryIfVerbose(ctx, writer, queryObj.Query)

		err = conn.RunQueryWithoutResult(ctx, queryObj)
		if err != nil {
			return err
		}
		lastQuery = queryObj
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

func shouldInitializeIncrementalTable(asset *pipeline.Asset) bool {
	if asset.Materialization.Type != pipeline.MaterializationTypeTable {
		return false
	}

	switch asset.Materialization.Strategy {
	case pipeline.MaterializationStrategyDeleteInsert,
		pipeline.MaterializationStrategyMerge,
		pipeline.MaterializationStrategyTimeInterval:
		return true
	default:
		return false
	}
}

func isFullRefreshMaterializer(materializer materializer) bool {
	fullRefresh, ok := materializer.(interface{ IsFullRefresh() bool })
	return ok && fullRefresh.IsFullRefresh()
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

type renderer interface {
	Render(query string) (string, error)
}

type QuerySensor struct {
	connection     config.ConnectionGetter
	renderer       renderer
	secondsToSleep int64
}

func NewQuerySensor(conn config.ConnectionGetter, renderer renderer, secondsToSleep int64) *QuerySensor {
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
	qq, ok := t.Parameters.GetString("query")
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

	rawConn := o.connection.GetConnection(connName)
	if rawConn == nil {
		return config.NewConnectionNotFoundError(ctx, "", connName)
	}

	conn, ok := rawConn.(Client)
	if !ok {
		return errors.Errorf("connection '%s' is not an athena connection", connName)
	}

	for {
		res, err := conn.Select(ctx, &query.Query{Query: qq})
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
