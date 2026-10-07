package mssql

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/devenv"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	mssqldb "github.com/microsoft/go-mssqldb"
	"github.com/pkg/errors"
)

type materializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
}

type MsClient interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
}

type devEnvModifier interface {
	Modify(ctx context.Context, p *pipeline.Pipeline, a *pipeline.Asset, q *query.Query) (*query.Query, error)
	RegisterAssetForSchemaCache(ctx context.Context, p *pipeline.Pipeline, a *pipeline.Asset, q *query.Query) error
}

type BasicOperator struct {
	connection   config.ConnectionGetter
	extractor    query.QueryExtractor
	materializer materializer
	devEnv       devEnvModifier
}

func NewBasicOperator(conn config.ConnectionGetter, extractor query.QueryExtractor, materializer materializer, parser *sqlparser.SQLParser) *BasicOperator {
	return &BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: materializer,
		devEnv: &devenv.DevEnvQueryModifier{
			Dialect: "tsql",
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
		if t.Materialization.Strategy != pipeline.MaterializationStrategyDDL {
			return nil
		}
		queries = []*query.Query{{Query: ""}}
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

	conn, ok := rawConn.(MsClient)
	if !ok {
		return errors.Errorf("connection '%s' is not a mssql connection", connName)
	}

	writer := ctx.Value(executor.KeyPrinter)

	if o.devEnv == nil {
		ansisql.LogQueryIfVerbose(ctx, writer, q.Query)
		return runMaterializedQuery(ctx, conn, t, q)
	}

	q, err = o.devEnv.Modify(ctx, p, t, q)
	if err != nil {
		return err
	}

	ansisql.LogQueryIfVerbose(ctx, writer, q.Query)

	err = runMaterializedQuery(ctx, conn, t, q)
	if err != nil {
		return err
	}

	err = o.devEnv.RegisterAssetForSchemaCache(ctx, p, t, q)
	if err != nil {
		return errors.Wrap(err, "cannot register asset for schema cache")
	}

	return nil
}

func runMaterializedQuery(ctx context.Context, conn MsClient, asset *pipeline.Asset, q *query.Query) error {
	// Error 1205 rolls back the victim transaction. Replay only materializations
	// that use one transaction or one statement. Hooks, arbitrary scripts, and
	// DDL or append batches can commit work before a later statement deadlocks.
	retryable := false
	if len(asset.Hooks.Pre) == 0 && len(asset.Hooks.Post) == 0 {
		switch asset.Materialization.Type {
		case pipeline.MaterializationTypeView:
			retryable = true
		case pipeline.MaterializationTypeTable:
			switch asset.Materialization.Strategy {
			case pipeline.MaterializationStrategyNone, pipeline.MaterializationStrategyCreateReplace,
				pipeline.MaterializationStrategyMerge,
				pipeline.MaterializationStrategyDeleteInsert, pipeline.MaterializationStrategyTimeInterval,
				pipeline.MaterializationStrategyTruncateInsert:
				retryable = true
			default:
				// Only the explicitly listed strategies are safe to replay.
			}
		default:
			// Unmaterialized scripts may contain independently committed statements.
		}
	}

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := conn.RunQueryWithoutResult(ctx, q)
		var sqlErr mssqldb.Error
		if !retryable || attempt == 10 || !errors.As(err, &sqlErr) || sqlErr.Number != 1205 {
			return err
		}

		// Exponential backoff with jitter keeps concurrent victims from retrying
		// in lockstep. Cancellation also interrupts the wait.
		delay := (250 * time.Millisecond << attempt) + time.Duration(rand.IntN(250))*time.Millisecond //nolint:gosec // Retry jitter does not require cryptographic randomness.
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func NewColumnCheckOperator(manager config.ConnectionGetter) *ansisql.ColumnCheckOperator {
	return ansisql.NewColumnCheckOperator(map[string]ansisql.CheckRunner{
		"not_null":        ansisql.NewNotNullCheck(manager),
		"unique":          &UniqueCheck{conn: manager},
		"relationships":   ansisql.NewRelationshipsCheck(manager, ansisql.QuoteIdentifierWithBrackets),
		"positive":        ansisql.NewPositiveCheck(manager),
		"non_negative":    ansisql.NewNonNegativeCheck(manager),
		"negative":        ansisql.NewNegativeCheck(manager),
		"min":             ansisql.NewMinCheck(manager),
		"max":             ansisql.NewMaxCheck(manager),
		"accepted_values": &AcceptedValuesCheck{conn: manager},
		"pattern":         &PatternCheck{conn: manager},
	})
}
