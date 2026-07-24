package spark

import (
	"context"
	"strings"

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

type queryMaterializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
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

func NewBasicOperator(conn config.ConnectionGetter, extractor query.QueryExtractor, fullRefresh bool, _ pipeline.DeclareHoister, parser *sqlparser.SQLParser) *BasicOperator {
	return &BasicOperator{
		connection: conn,
		extractor:  extractor,
		materializer: pipeline.HookWrapperMaterializer{
			Mat: NewMaterializer(fullRefresh),
		},
		devEnv: &devenv.DevEnvQueryModifier{
			Dialect: "spark",
			Conn:    conn,
			Parser:  parser,
		},
	}
}

func (o BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o BasicOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) error {
	ctx = query.WithQueryType(ctx, query.QueryTypeMain)
	extractor, err := o.extractor.CloneForAsset(ctx, p, asset)
	if err != nil {
		return errors.Wrapf(err, "failed to clone extractor for asset %s", asset.Name)
	}
	queries, err := extractor.ExtractQueriesFromString(asset.ExecutableFile.Content)
	if err != nil {
		return errors.Wrap(err, "cannot extract queries from the task file")
	}
	if len(queries) == 0 {
		if asset.Materialization.Strategy != pipeline.MaterializationStrategyDDL {
			return nil
		}
		queries = []*query.Query{{Query: ""}}
	}
	connectionName, err := p.GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}
	rawConnection := o.connection.GetConnection(connectionName)
	if rawConnection == nil {
		return config.NewConnectionNotFoundError(ctx, "", connectionName)
	}
	connection, ok := rawConnection.(*Client)
	if !ok {
		return errors.Errorf("connection '%s' is not a Spark connection", connectionName)
	}
	materialized, err := o.renderQueries(asset, queries)
	if err != nil {
		return err
	}
	if asset.Materialization.Type != pipeline.MaterializationTypeNone {
		if err := connection.CreateSchemaIfNotExist(ctx, asset, p.Name); err != nil {
			return err
		}
	}
	writer := ctx.Value(executor.KeyPrinter)
	if err := o.materializer.LogIfFullRefreshAndDDL(writer, asset); err != nil {
		return err
	}
	materializedQueries, err := extractor.ExtractQueriesFromString(materialized)
	if err != nil {
		return errors.Wrap(err, "cannot extract queries from materialized string")
	}

	queriesToRun := make([]*query.Query, 0, len(materializedQueries))
	var lastQuery *query.Query
	for _, queryObj := range materializedQueries {
		queryToRun := queryObj
		sessionStatement := isSparkSessionStatement(queryObj.Query)
		if o.devEnv != nil && !sessionStatement {
			queryToRun, err = o.devEnv.Modify(ctx, p, asset, queryObj)
			if err != nil {
				return err
			}
		}
		annotatedQuery, err := ansisql.AddAnnotationComment(ctx, queryToRun, asset.Name, "main", p.Name)
		if err != nil {
			return errors.Wrap(err, "failed to add Spark query annotation")
		}
		ansisql.LogQueryIfVerbose(ctx, writer, annotatedQuery.Query)
		queriesToRun = append(queriesToRun, annotatedQuery)
		if !sessionStatement {
			lastQuery = queryToRun
		}
	}
	if err := connection.RunQueriesWithoutResult(ctx, queriesToRun); err != nil {
		return err
	}

	if o.devEnv == nil || lastQuery == nil {
		return nil
	}
	if err := o.devEnv.RegisterAssetForSchemaCache(ctx, p, asset, lastQuery); err != nil {
		return errors.Wrap(err, "cannot register asset for schema cache")
	}
	return nil
}

func (o BasicOperator) renderQueries(asset *pipeline.Asset, queries []*query.Query) (string, error) {
	return renderSparkQueries(asset, queries, o.materializer)
}

func renderSparkQueries(
	asset *pipeline.Asset,
	queries []*query.Query,
	materializer queryMaterializer,
) (string, error) {
	if asset.Materialization.Type == pipeline.MaterializationTypeNone {
		return materializer.Render(asset, joinSparkQueries(queries))
	}
	if len(strings.Split(asset.Name, ".")) < 3 && sparkAssetUsesStatement(asset, queries, isSparkUseStatement) {
		return "", errors.New(
			"materialized Spark assets that use USE must have a fully qualified catalog.schema.table name",
		)
	}

	mainQueryIndex := -1
	for index, queryObj := range queries {
		if isSparkSessionStatement(queryObj.Query) {
			continue
		}
		if mainQueryIndex >= 0 {
			return "", errors.New("cannot enable materialization for tasks with multiple queries")
		}
		mainQueryIndex = index
	}
	if mainQueryIndex < 0 {
		if asset.Materialization.Strategy != pipeline.MaterializationStrategyDDL {
			return "", errors.New("materialization requires a non-session query")
		}
		queries = append(append([]*query.Query(nil), queries...), &query.Query{Query: ""})
		mainQueryIndex = len(queries) - 1
	}

	materialized, err := materializer.Render(asset, queries[mainQueryIndex].String())
	if err != nil {
		return "", err
	}

	statements := make([]string, 0, len(queries))
	for index, queryObj := range queries {
		if index == mainQueryIndex {
			statements = append(statements, materialized)
			continue
		}
		statements = append(statements, queryObj.String())
	}
	return joinSparkStatements(statements), nil
}

func sparkAssetUsesStatement(
	asset *pipeline.Asset,
	queries []*query.Query,
	matches func(string) bool,
) bool {
	for _, queryObj := range queries {
		if matches(queryObj.Query) {
			return true
		}
	}
	for _, hooks := range [][]pipeline.Hook{asset.Hooks.Pre, asset.Hooks.Post} {
		for _, hook := range hooks {
			for _, hookStatement := range query.SplitQueriesPreservingSessionStatements(hook.Query) {
				if matches(hookStatement.Query) {
					return true
				}
			}
		}
	}
	return false
}

func isSparkUseStatement(statement string) bool {
	return sparkLeadingKeyword(statement) == "USE"
}

func joinSparkQueries(queries []*query.Query) string {
	statements := make([]string, 0, len(queries))
	for _, queryObj := range queries {
		statements = append(statements, queryObj.String())
	}
	return joinSparkStatements(statements)
}

func joinSparkStatements(statements []string) string {
	normalized := make([]string, 0, len(statements))
	for _, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement != "" {
			normalized = append(normalized, strings.TrimSuffix(statement, ";"))
		}
	}
	return strings.Join(normalized, ";\n")
}

func isSparkSessionStatement(statement string) bool {
	switch sparkLeadingKeyword(statement) {
	case "DECLARE", "RESET", "SET", "USE":
		return true
	default:
		return false
	}
}

func sparkLeadingKeyword(statement string) string {
	statement = strings.TrimSpace(statement)
	for statement != "" {
		switch {
		case strings.HasPrefix(statement, "--"):
			newline := strings.IndexByte(statement, '\n')
			if newline < 0 {
				return ""
			}
			statement = strings.TrimSpace(statement[newline+1:])
		case strings.HasPrefix(statement, "/*"):
			commentEnd := strings.Index(statement[2:], "*/")
			if commentEnd < 0 {
				return ""
			}
			statement = strings.TrimSpace(statement[commentEnd+4:])
		default:
			fields := strings.Fields(statement)
			if len(fields) == 0 {
				return ""
			}
			return strings.ToUpper(fields[0])
		}
	}
	return ""
}
