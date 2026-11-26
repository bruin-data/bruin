package duck

import (
	"context"
	"io"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/devenv"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/schemaevolution"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/pkg/errors"
)

type materializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
	IsFullRefresh() bool
	LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error
}

type DuckDBClient interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error)
	CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error
	Close()
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
			Dialect: "duckdb",
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
	rawQuery := q.String()
	writer := ctx.Value(executor.KeyPrinter)
	if err := o.materializer.LogIfFullRefreshAndDDL(writer, t); err != nil {
		return err
	}

	fullRefresh := t.FullRefreshEnabled(o.materializer.IsFullRefresh())
	shouldEvolve := schemaevolution.ShouldEvolve(t, fullRefresh)
	if shouldEvolve {
		rawQuery = schemaevolution.StripTrailingSemicolonsAndComments(duckDBSQLDialect, rawQuery)
	}

	materialized, err := o.renderMaterializedSQL(t, rawQuery, shouldEvolve)
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

	rawConn := o.connection.GetConnection(connName)
	if rawConn == nil {
		return config.NewConnectionNotFoundError(ctx, "", connName)
	}

	conn, ok := rawConn.(DuckDBClient)
	if !ok {
		return errors.Errorf("connection '%s' is not a duckdb connection", connName)
	}

	if q, ok := rawConn.(interface{ HasQuack() bool }); ok && q.HasQuack() {
		return errors.Errorf("connection '%s' uses quack: true, which supports ingestr assets and quality checks but not duckdb.sql assets yet", connName)
	}

	if t.Materialization.Type != pipeline.MaterializationTypeNone {
		if err := conn.CreateSchemaIfNotExist(ctx, t); err != nil {
			return err
		}
	}

	defer conn.Close()

	queryObj := &query.Query{Query: q.Query}
	if o.devEnv != nil {
		queryObj, err = o.devEnv.Modify(ctx, p, t, queryObj)
		if err != nil {
			return err
		}
	}

	if shouldEvolve {
		printer, _ := writer.(io.Writer)
		executedMain, err := o.runWithSchemaEvolution(ctx, p, t, conn, rawQuery, queryObj.Query, fullRefresh, printer)
		if err != nil {
			return err
		}
		queryObj.Query = executedMain
	} else {
		ansisql.LogQueryIfVerbose(ctx, writer, queryObj.Query)
		if err := conn.RunQueryWithoutResult(ctx, queryObj); err != nil {
			return err
		}
	}

	if o.devEnv == nil {
		return nil
	}

	if err := o.devEnv.RegisterAssetForSchemaCache(ctx, p, t, queryObj); err != nil {
		return errors.Wrap(err, "cannot register asset for schema cache")
	}

	return nil
}

func (o BasicOperator) renderMaterializedSQL(asset *pipeline.Asset, rawQuery string, withoutHooks bool) (string, error) {
	if withoutHooks && !asset.Hooks.IsZero() {
		noHooksAsset := *asset
		noHooksAsset.Hooks = pipeline.Hooks{}
		return o.materializer.Render(&noHooksAsset, rawQuery)
	}
	return o.materializer.Render(asset, rawQuery)
}

func (o BasicOperator) runWithSchemaEvolution(
	ctx context.Context,
	p *pipeline.Pipeline,
	asset *pipeline.Asset,
	conn DuckDBClient,
	rawQuery string,
	mainSQL string,
	fullRefresh bool,
	printer io.Writer,
) (string, error) {
	inspectQuery := rawQuery
	if o.devEnv != nil && !schemaevolution.UsesDeclaredSchemaOnly(asset.Materialization.Strategy) {
		modifiedInspect, err := o.devEnv.Modify(ctx, p, asset, &query.Query{Query: rawQuery})
		if err != nil {
			return "", err
		}
		inspectQuery = modifiedInspect.Query
	}

	effectiveHooks := asset.Hooks
	if !asset.Hooks.IsZero() && o.devEnv != nil {
		var err error
		effectiveHooks, err = o.modifyHooksForDevEnv(ctx, p, asset, asset.Hooks)
		if err != nil {
			return "", err
		}
	}

	plan, executedMain, err := executeWithDuckDBSchemaEvolution(ctx, duckDBEvolutionRequest{
		Conn:         conn,
		Hooks:        effectiveHooks,
		Asset:        asset,
		InspectQuery: inspectQuery,
		MainSQL:      mainSQL,
		FullRefresh:  fullRefresh,
		Writer:       printer,
	})
	if err != nil {
		return "", err
	}

	schemaevolution.LogPlan(printer, asset.Name, plan)
	return executedMain, nil
}

func (o BasicOperator) modifyHooksForDevEnv(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset, hooks pipeline.Hooks) (pipeline.Hooks, error) {
	modifySlice := func(in []pipeline.Hook) ([]pipeline.Hook, error) {
		out := make([]pipeline.Hook, len(in))
		for i, h := range in {
			q, err := o.devEnv.Modify(ctx, p, t, &query.Query{Query: h.Query})
			if err != nil {
				return nil, err
			}
			out[i] = pipeline.Hook{Query: q.Query}
		}
		return out, nil
	}
	pre, err := modifySlice(hooks.Pre)
	if err != nil {
		return pipeline.Hooks{}, err
	}
	post, err := modifySlice(hooks.Post)
	if err != nil {
		return pipeline.Hooks{}, err
	}
	return pipeline.Hooks{
		ApplicableTypes: hooks.ApplicableTypes,
		Pre:             pre,
		Post:            post,
	}, nil
}

func NewColumnCheckOperator(manager config.ConnectionGetter) *ansisql.ColumnCheckOperator {
	return ansisql.NewColumnCheckOperator(map[string]ansisql.CheckRunner{
		"not_null":        ansisql.NewNotNullCheck(manager),
		"unique":          ansisql.NewUniqueCheck(manager),
		"relationships":   ansisql.NewRelationshipsCheck(manager, ansisql.QuoteIdentifierWithDoubleQuotes),
		"positive":        ansisql.NewPositiveCheck(manager),
		"non_negative":    ansisql.NewNonNegativeCheck(manager),
		"negative":        ansisql.NewNegativeCheck(manager),
		"min":             ansisql.NewMinCheck(manager),
		"max":             ansisql.NewMaxCheck(manager),
		"accepted_values": &AcceptedValuesCheck{conn: manager},
		"pattern":         &PatternCheck{conn: manager},
	})
}
