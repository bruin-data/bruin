package synapse

import (
	"context"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/devenv"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/pkg/errors"
)

type materializer interface {
	Render(task *pipeline.Asset, query string) ([]string, error)
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
		return nil
	}

	if len(queries) > 1 && t.Materialization.Type != pipeline.MaterializationTypeNone {
		return errors.New("cannot enable materialization for tasks with multiple queries")
	}

	q := queries[0]
	materializedQueries, err := o.materializer.Render(t, q.String())
	if err != nil {
		return err
	}

	if t.Materialization.Strategy == pipeline.MaterializationStrategyTimeInterval {
		materializedQueries, err = extractor.ReextractQueriesFromSlice(materializedQueries)
		if err != nil {
			return err
		}
	}

	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	rawConn := o.connection.GetConnection(connName)
	if rawConn == nil {
		return config.NewConnectionNotFoundError(ctx, "", connName)
	}

	conn, ok := rawConn.(mssql.MsClient)
	if !ok {
		return errors.Errorf("connection '%s' is not a synapse connection", connName)
	}

	writer := ctx.Value(executor.KeyPrinter)
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
