package postgres

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/pkg/errors"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/devenv"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/sqlparser"
)

const CharacterLimit = 10000

type materializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
	LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error
}

type PgClient interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error)
	Ping(ctx context.Context) error
	GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error)
	CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error
	PushColumnDescriptions(ctx context.Context, asset *pipeline.Asset) error
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
			Dialect: "postgres",
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

	if len(queries) > 1 {
		// if the code hits here this means that we used an extractor that splits the content into multiple queries.
		// this is not supported and we should not allow it, Postgres-like platforms accepts multiple SQL statements in a single query, that's what we use instead.
		return errors.New("Postgres-like operators can only handle one query at a time, this seems like a bug, please report the issue to the Bruin team")
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

	conn, ok := o.connection.GetConnection(connName).(PgClient)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not a postgres connection", connName)
	}

	if t.Materialization.Type != pipeline.MaterializationTypeNone {
		err = conn.CreateSchemaIfNotExist(ctx, t)
		if err != nil {
			return err
		}
	}

	// Print SQL query in verbose mode
	if verbose := ctx.Value(executor.KeyVerbose); verbose != nil && verbose.(bool) {
		if w, ok := writer.(io.Writer); ok {
			queryPreview := strings.TrimSpace(q.Query)
			if len(queryPreview) > CharacterLimit {
				queryPreview = queryPreview[:CharacterLimit] + "\n... (truncated)"
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

	client, ok := o.connection.GetConnection(connName).(PgClient)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not a postgres connection", connName)
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
		_, _ = writer.Write([]byte("Failed to push metadata to Postgres, skipping...\n"))
		return err
	}

	return nil
}
