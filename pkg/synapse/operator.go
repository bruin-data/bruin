package synapse

import (
	"context"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type materializer interface {
	Render(task *pipeline.Asset, query string) ([]string, error)
}

type queryExtractor interface {
	ExtractQueriesFromString(content string) ([]*query.Query, error)
	ExtractQueriesFromSlice(content []string) ([]*query.Query, error)
}

type connectionFetcher interface {
	GetMsConnection(name string) (mssql.MsClient, error)
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
	materializedQueries, err := o.materializer.Render(t, t.ExecutableFile.Content)
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

	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	conn, err := o.connection.GetMsConnection(connName)
	if err != nil {
		return err
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
