package snowflake

import (
	"context"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type materializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
}

type queryExtractor interface {
	ExtractQueriesFromFile(filepath string) ([]*query.Query, error)
}

type SfClient interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
}

type connectionFetcher interface {
	GetSfConnection(name string) (SfClient, error)
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
	queries, err := o.extractor.ExtractQueriesFromFile(t.ExecutableFile.Path)
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

	q.Query = materialized

	conn, err := o.connection.GetSfConnection(p.GetConnectionNameForAsset(t))
	if err != nil {
		return err
	}

	return conn.RunQueryWithoutResult(ctx, q)
}

type columnCheckRunner interface {
	Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error
}

type ColumnCheckOperator struct {
	testRunners map[string]columnCheckRunner
}

func NewColumnCheckOperator(manager connectionFetcher) (*ColumnCheckOperator, error) {
	return &ColumnCheckOperator{
		testRunners: map[string]columnCheckRunner{
			"not_null":        &NotNullCheck{conn: manager},
			"unique":          &UniqueCheck{conn: manager},
			"positive":        &PositiveCheck{conn: manager},
			"accepted_values": &AcceptedValuesCheck{conn: manager},
		},
	}, nil
}

func (o ColumnCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	test, ok := ti.(*scheduler.ColumnCheckInstance)
	if !ok {
		return errors.New("cannot run a non-column check instance")
	}

	executor, ok := o.testRunners[test.Check.Name]
	if !ok {
		return errors.New("there is no executor configured for the check type, check cannot be run: " + test.Check.Name)
	}

	return executor.Check(ctx, test)
}

type customCheckRunner interface {
	Check(ctx context.Context, ti *scheduler.CustomCheckInstance) error
}

func NewCustomCheckOperator(manager connectionFetcher) (*CustomCheckOperator, error) {
	return &CustomCheckOperator{
		checkRunner: &CustomCheck{conn: manager},
	}, nil
}

type CustomCheckOperator struct {
	checkRunner customCheckRunner
}

func (o *CustomCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	instance, ok := ti.(*scheduler.CustomCheckInstance)
	if !ok {
		return errors.New("cannot run a non-custom check instance")
	}

	return o.checkRunner.Check(ctx, instance)
}
