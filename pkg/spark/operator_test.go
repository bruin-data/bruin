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
	"github.com/pkg/errors"
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

// recordingDevEnv rewrites `analytics.` to `dev_analytics.` and records every
// statement it was asked to modify. Statements listed in unparseable fail, the
// way a real parser fails on Spark syntax it does not model.
type recordingDevEnv struct {
	modified    []string
	unparseable []string
}

func (d *recordingDevEnv) Modify(
	_ context.Context,
	_ *pipeline.Pipeline,
	_ *pipeline.Asset,
	q *query.Query,
) (*query.Query, error) {
	d.modified = append(d.modified, q.Query)
	for _, unparseable := range d.unparseable {
		if q.Query == unparseable {
			return nil, errors.New("cannot parse statement")
		}
	}
	return &query.Query{Query: strings.ReplaceAll(q.Query, "analytics.", "dev_analytics.")}, nil
}

func (d *recordingDevEnv) RegisterAssetForSchemaCache(
	context.Context,
	*pipeline.Pipeline,
	*pipeline.Asset,
	*query.Query,
) error {
	return nil
}

func TestBasicOperatorAppliesDevEnvToSessionStatementsWithSubqueries(t *testing.T) {
	t.Parallel()

	const setVar = "SET VAR cutoff = (SELECT MAX(event_ts) FROM analytics.events)"

	tests := []struct {
		name         string
		statements   []string
		unparseable  []string
		wantModified []string
		wantRun      []string
	}{
		{
			name:         "SET assigned from a subquery is rewritten",
			statements:   []string{setVar, "SELECT * FROM analytics.events"},
			wantModified: []string{setVar, "SELECT * FROM analytics.events"},
			wantRun: []string{
				"SET VAR cutoff = (SELECT MAX(event_ts) FROM dev_analytics.events)",
				"SELECT * FROM dev_analytics.events",
			},
		},
		{
			name:         "USE and plain SET are left alone",
			statements:   []string{"USE analytics", "SET spark.sql.shuffle.partitions = 8", "SELECT 1"},
			wantModified: []string{"SELECT 1"},
			wantRun:      []string{"USE analytics", "SET spark.sql.shuffle.partitions = 8", "SELECT 1"},
		},
		{
			name:         "an unparseable session statement runs as-is",
			statements:   []string{setVar, "SELECT 1"},
			unparseable:  []string{setVar},
			wantModified: []string{setVar, "SELECT 1"},
			wantRun:      []string{setVar, "SELECT 1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			queries := make([]*query.Query, 0, len(test.statements))
			for _, statement := range test.statements {
				queries = append(queries, &query.Query{Query: statement})
			}
			connection := &recordingConnection{}
			modifier := &recordingDevEnv{unparseable: test.unparseable}
			operator := BasicOperator{
				connection: sparkConnectionGetter{connection: &Client{connection: connection}},
				extractor: &queuedSparkExtractor{
					responses: [][]*query.Query{queries, queries},
				},
				materializer: passthroughSparkMaterializer{},
				devEnv:       modifier,
			}
			asset := &pipeline.Asset{
				Name: "catalog.analytics.events",
				Type: pipeline.AssetTypeSparkQuery,
			}

			require.NoError(t, operator.RunTask(t.Context(), &pipeline.Pipeline{Name: "p"}, asset))
			require.Equal(t, test.wantModified, modifier.modified)
			require.Equal(t, test.wantRun, connection.queries)
		})
	}
}

func TestSparkStatementCanReferenceTables(t *testing.T) {
	t.Parallel()

	require.True(t, sparkStatementCanReferenceTables("SET VAR x = (SELECT MAX(ts) FROM events)"))
	require.True(t, sparkStatementCanReferenceTables("DECLARE VARIABLE x INT DEFAULT (select 1 from events)"))
	require.False(t, sparkStatementCanReferenceTables("SET spark.sql.shuffle.partitions = 8"))
	require.False(t, sparkStatementCanReferenceTables("USE local.analytics"))
	require.False(t, sparkStatementCanReferenceTables("RESET spark.sql.ansi.enabled"))
	require.False(t, sparkStatementCanReferenceTables("SET spark.job.description = selected"))
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

func TestBasicOperatorMaterializationDetectsUseAnywhereInPreHooks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		hooks pipeline.Hooks
		error string
	}{
		{
			name: "pre-hook after another statement",
			hooks: pipeline.Hooks{
				Pre: []pipeline.Hook{{
					Query: "SET spark.sql.shuffle.partitions = 8; USE local.analytics",
				}},
			},
			error: "materialized Spark assets that use USE must have a fully qualified catalog.schema.table name",
		},
		{
			name: "commented pre-hook after another statement",
			hooks: pipeline.Hooks{
				Pre: []pipeline.Hook{{
					Query: "-- configure the session\nSET spark.sql.shuffle.partitions = 8; /* select the namespace */ USE local.analytics",
				}},
			},
			error: "materialized Spark assets that use USE must have a fully qualified catalog.schema.table name",
		},
		{
			// Post-hooks run after the materialized statement, so a USE in one
			// cannot change how the target table name resolved.
			name: "post-hook USE is allowed",
			hooks: pipeline.Hooks{
				Post: []pipeline.Hook{{
					Query: "USE maintenance_catalog; OPTIMIZE some_table",
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
			if test.error == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, test.error)
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
