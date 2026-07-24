package spark

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/require"
)

type recordingConnection struct {
	queries    []string
	queryTypes []string
}

func (c *recordingConnection) ExecContext(ctx context.Context, statement string, _ ...any) (sql.Result, error) {
	c.queries = append(c.queries, statement)
	c.queryTypes = append(c.queryTypes, query.QueryTypeFromContext(ctx))
	return driver.ResultNoRows, nil
}

func (c *recordingConnection) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, nil
}

type sparkConnectionGetter struct {
	connection any
}

func (g sparkConnectionGetter) GetConnection(string) any {
	return g.connection
}

type sparkOperatorExtractor struct {
	extractCount int
}

func (e *sparkOperatorExtractor) ExtractQueriesFromString(string) ([]*query.Query, error) {
	e.extractCount++
	if e.extractCount == 1 {
		return []*query.Query{{Query: "SELECT 1"}}, nil
	}
	return []*query.Query{
		{Query: "SET spark.sql.shuffle.partitions = 8"},
		{Query: "SELECT 1"},
	}, nil
}

func (e *sparkOperatorExtractor) CloneForAsset(
	context.Context,
	*pipeline.Pipeline,
	*pipeline.Asset,
) (query.QueryExtractor, error) {
	return e, nil
}

func (e *sparkOperatorExtractor) ReextractQueriesFromSlice(content []string) ([]string, error) {
	return content, nil
}

type sparkOperatorMaterializer struct{}

func (sparkOperatorMaterializer) Render(*pipeline.Asset, string) (string, error) {
	return "SET spark.sql.shuffle.partitions = 8;\nSELECT 1;", nil
}

func (sparkOperatorMaterializer) LogIfFullRefreshAndDDL(interface{}, *pipeline.Asset) error {
	return nil
}

type queuedSparkExtractor struct {
	responses [][]*query.Query
	index     int
}

func (e *queuedSparkExtractor) ExtractQueriesFromString(string) ([]*query.Query, error) {
	response := e.responses[e.index]
	e.index++
	return response, nil
}

func (e *queuedSparkExtractor) CloneForAsset(
	context.Context,
	*pipeline.Pipeline,
	*pipeline.Asset,
) (query.QueryExtractor, error) {
	return e, nil
}

func (e *queuedSparkExtractor) ReextractQueriesFromSlice(content []string) ([]string, error) {
	return content, nil
}

type passthroughSparkMaterializer struct{}

func (passthroughSparkMaterializer) Render(_ *pipeline.Asset, statement string) (string, error) {
	return statement, nil
}

func (passthroughSparkMaterializer) LogIfFullRefreshAndDDL(interface{}, *pipeline.Asset) error {
	return nil
}

func TestBasicOperatorQueryAnnotations(t *testing.T) {
	t.Parallel()

	connection := &recordingConnection{}
	operator := BasicOperator{
		connection: sparkConnectionGetter{
			connection: &Client{connection: connection},
		},
		extractor:    &sparkOperatorExtractor{},
		materializer: sparkOperatorMaterializer{},
		devEnv:       nil,
	}
	asset := &pipeline.Asset{
		Name: "catalog.analytics.annotated_asset",
		Type: pipeline.AssetTypeSparkQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Content: "SELECT 1",
		},
	}
	pipelineDefinition := &pipeline.Pipeline{Name: "annotated_pipeline"}
	ctx := context.WithValue(
		t.Context(),
		pipeline.RunConfigQueryAnnotations,
		`{"environment":"integration","owner":"data-team"}`,
	)

	err := operator.RunTask(ctx, pipelineDefinition, asset)
	require.NoError(t, err)
	require.Len(t, connection.queries, 2)
	require.Equal(t, []string{query.QueryTypeMain, query.QueryTypeMain}, connection.queryTypes)

	for _, executedQuery := range connection.queries {
		require.True(t, strings.HasPrefix(executedQuery, "-- @bruin.config:"))
		require.Contains(t, executedQuery, `"asset":"catalog.analytics.annotated_asset"`)
		require.Contains(t, executedQuery, `"pipeline":"annotated_pipeline"`)
		require.Contains(t, executedQuery, `"type":"main"`)
		require.Contains(t, executedQuery, `"environment":"integration"`)
		require.Contains(t, executedQuery, `"owner":"data-team"`)
	}
	require.Contains(t, connection.queries[0], "SET spark.sql.shuffle.partitions = 8")
	require.Contains(t, connection.queries[1], "SELECT 1")
}

func TestBasicOperatorQueryAnnotationsDisabled(t *testing.T) {
	t.Parallel()

	connection := &recordingConnection{}
	operator := BasicOperator{
		connection: sparkConnectionGetter{
			connection: &Client{connection: connection},
		},
		extractor:    &sparkOperatorExtractor{},
		materializer: sparkOperatorMaterializer{},
		devEnv:       nil,
	}
	asset := &pipeline.Asset{
		Name: "analytics.asset",
		Type: pipeline.AssetTypeSparkQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Content: "SELECT 1",
		},
	}

	err := operator.RunTask(t.Context(), &pipeline.Pipeline{Name: "pipeline"}, asset)
	require.NoError(t, err)
	require.Len(t, connection.queries, 2)
	for _, executedQuery := range connection.queries {
		require.NotContains(t, executedQuery, ansisql.DefaultQueryAnnotations)
		require.NotContains(t, executedQuery, "-- @bruin.config:")
	}
}

func TestBasicOperatorExecutesMultipleStatementsInOneSession(t *testing.T) {
	t.Parallel()

	statements := []*query.Query{
		{Query: "USE analytics"},
		{Query: "SET spark.sql.shuffle.partitions = 8"},
		{Query: "CREATE TABLE events (id INT)"},
		{Query: "INSERT INTO events VALUES (1)"},
	}
	connection := &recordingConnection{}
	operator := BasicOperator{
		connection: sparkConnectionGetter{
			connection: &Client{connection: connection},
		},
		extractor: &queuedSparkExtractor{
			responses: [][]*query.Query{statements, statements},
		},
		materializer: passthroughSparkMaterializer{},
		devEnv:       nil,
	}
	asset := &pipeline.Asset{
		Name: "analytics.events",
		Type: pipeline.AssetTypeSparkQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Content: "multi-statement script",
		},
	}

	require.NoError(t, operator.RunTask(t.Context(), &pipeline.Pipeline{Name: "pipeline"}, asset))
	require.Equal(t, []string{
		"USE analytics",
		"SET spark.sql.shuffle.partitions = 8",
		"CREATE TABLE events (id INT)",
		"INSERT INTO events VALUES (1)",
	}, connection.queries)
}

func TestBasicOperatorMaterializationPreservesSessionStatementOrder(t *testing.T) {
	t.Parallel()

	operator := BasicOperator{materializer: sparkOperatorMaterializer{}}
	asset := &pipeline.Asset{
		Name: "local.analytics.events",
		Materialization: pipeline.Materialization{
			Type: pipeline.MaterializationTypeTable,
		},
	}

	got, err := operator.renderQueries(asset, []*query.Query{
		{Query: "USE analytics"},
		{Query: "SELECT * FROM source"},
		{Query: "RESET spark.sql.shuffle.partitions"},
	})
	require.NoError(t, err)
	require.Equal(
		t,
		"USE analytics;\nSET spark.sql.shuffle.partitions = 8;\nSELECT 1;\nRESET spark.sql.shuffle.partitions",
		got,
	)
}

func TestBasicOperatorMaterializationSupportsSessionOnlyDDL(t *testing.T) {
	t.Parallel()

	operator := BasicOperator{
		materializer: pipeline.HookWrapperMaterializer{
			Mat: NewMaterializer(false),
		},
	}
	asset := &pipeline.Asset{
		Name: "local.analytics.events",
		Type: pipeline.AssetTypeSparkQuery,
		Columns: []pipeline.Column{{
			Name: "event_id",
			Type: "integer",
		}},
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: pipeline.MaterializationStrategyDDL,
		},
	}

	got, err := operator.renderQueries(asset, []*query.Query{{
		Query: "SET spark.sql.adaptive.enabled = true",
	}})
	require.NoError(t, err)
	require.Less(
		t,
		strings.Index(got, "SET spark.sql.adaptive.enabled = true"),
		strings.Index(got, "CREATE TABLE IF NOT EXISTS `local`.`analytics`.`events`"),
	)
}

func TestBasicOperatorMaterializationRequiresQualifiedTargetWithUse(t *testing.T) {
	t.Parallel()

	operator := BasicOperator{materializer: sparkOperatorMaterializer{}}
	asset := &pipeline.Asset{
		Name: "analytics.events",
		Materialization: pipeline.Materialization{
			Type: pipeline.MaterializationTypeTable,
		},
	}

	_, err := operator.renderQueries(asset, []*query.Query{
		{Query: "USE local.analytics"},
		{Query: "SELECT * FROM source"},
	})
	require.EqualError(
		t,
		err,
		"materialized Spark assets that use USE must have a fully qualified catalog.schema.table name",
	)
}

func TestBasicOperatorMaterializationRequiresQualifiedTargetWithUseHook(t *testing.T) {
	t.Parallel()

	operator := BasicOperator{materializer: sparkOperatorMaterializer{}}
	asset := &pipeline.Asset{
		Name: "analytics.events",
		Materialization: pipeline.Materialization{
			Type: pipeline.MaterializationTypeTable,
		},
		Hooks: pipeline.Hooks{
			Pre: []pipeline.Hook{{Query: "USE local.analytics"}},
		},
	}

	_, err := operator.renderQueries(asset, []*query.Query{{Query: "SELECT * FROM source"}})
	require.EqualError(
		t,
		err,
		"materialized Spark assets that use USE must have a fully qualified catalog.schema.table name",
	)
}

func TestBasicOperatorMaterializationDetectsUseAnywhereInHooks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		hooks pipeline.Hooks
	}{
		{
			name: "pre-hook after another statement",
			hooks: pipeline.Hooks{
				Pre: []pipeline.Hook{{
					Query: "SET spark.sql.shuffle.partitions = 8; USE local.analytics",
				}},
			},
		},
		{
			name: "commented post-hook after another statement",
			hooks: pipeline.Hooks{
				Post: []pipeline.Hook{{
					Query: "-- configure the session\nSET spark.sql.shuffle.partitions = 8; /* select the namespace */ USE local.analytics",
				}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operator := BasicOperator{materializer: sparkOperatorMaterializer{}}
			asset := &pipeline.Asset{
				Name: "analytics.events",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
				Hooks: test.hooks,
			}

			_, err := operator.renderQueries(asset, []*query.Query{{Query: "SELECT * FROM source"}})
			require.EqualError(
				t,
				err,
				"materialized Spark assets that use USE must have a fully qualified catalog.schema.table name",
			)
		})
	}
}

func TestSparkSessionStatementDetectionSkipsLeadingComments(t *testing.T) {
	t.Parallel()

	require.True(t, isSparkSessionStatement("-- configure the session\nSET spark.sql.shuffle.partitions = 8"))
	require.True(t, isSparkSessionStatement("/* select the namespace */\nUSE local.analytics"))
	require.False(t, isSparkSessionStatement("-- SELECT 1"))
	require.False(t, isSparkSessionStatement("/* unterminated comment\nUSE local.analytics"))
}
