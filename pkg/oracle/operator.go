package oracle

import (
	"context"
	"errors"
	"fmt"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/devenv"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/sqlparser"
)

type materializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
	LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error
}

// OracleClient is the interface for executing queries against Oracle.
// Note: table and column names are used as-is (unquoted uppercase identifiers).
// If your Oracle objects were created with quoted lowercase names, you must
// ensure asset names match the exact case used during creation.
type OracleClient interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
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
			Dialect: "oracle",
			Conn:    conn,
			Parser:  parser,
		},
	}
}

func (o BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o BasicOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) error {
	extractor, err := o.extractor.CloneForAsset(ctx, p, asset)
	if err != nil {
		return fmt.Errorf("failed to clone extractor for asset %s: %w", asset.Name, err)
	}

	queries, err := extractor.ExtractQueriesFromString(asset.ExecutableFile.Content)
	if err != nil {
		return fmt.Errorf("cannot extract queries from the task file: %w", err)
	}

	if len(queries) == 0 {
		return nil
	}

	if len(queries) > 1 && asset.Materialization.Type != pipeline.MaterializationTypeNone {
		return errors.New("Oracle operator can only handle a single query when materialization is enabled")
	}

	q := queries[0]
	materialized, err := o.materializer.Render(asset, q.String())
	if err != nil {
		return err
	}

	writer := ctx.Value(executor.KeyPrinter)
	if err := o.materializer.LogIfFullRefreshAndDDL(writer, asset); err != nil {
		return err
	}

	q.Query = materialized

	if asset.Materialization.Strategy == pipeline.MaterializationStrategyTimeInterval {
		renderedQueries, err := extractor.ExtractQueriesFromString(materialized)
		if err != nil {
			return fmt.Errorf("cannot re-extract rendered query for time_interval strategy: %w", err)
		}
		if len(renderedQueries) == 0 {
			return errors.New("rendered queries unexpectedly empty")
		}
		q.Query = renderedQueries[0].Query
	}

	connName, err := p.GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}

	conn, ok := o.connection.GetConnection(connName).(OracleClient)
	if !ok {
		return fmt.Errorf("'%s' either does not exist or is not an Oracle connection", connName)
	}

	// We don't have CreateSchemaIfNotExist required in the interface implemented natively yet on Oracle.

	if o.devEnv == nil {
		ansisql.LogQueryIfVerbose(ctx, writer, q.Query)
		return conn.RunQueryWithoutResult(ctx, q)
	}

	q, err = o.devEnv.Modify(ctx, p, asset, q)
	if err != nil {
		return err
	}

	ansisql.LogQueryIfVerbose(ctx, writer, q.Query)

	if err := conn.RunQueryWithoutResult(ctx, q); err != nil {
		return err
	}

	if err := o.devEnv.RegisterAssetForSchemaCache(ctx, p, asset, q); err != nil {
		return fmt.Errorf("cannot register asset for schema cache: %w", err)
	}

	return nil
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
