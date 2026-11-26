package pipeline_test

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTask_SchemaEvolution(t *testing.T) {
	t.Parallel()

	t.Run("embedded YAML block", func(t *testing.T) {
		t.Parallel()
		fs := afero.NewMemMapFs()
		err := afero.WriteFile(fs, "asset.sql", []byte(strings.TrimSpace(`
/* @bruin
name: demo.events
type: duckdb.sql
materialization:
  type: table
  strategy: append
schema_evolution:
  enabled: true
@bruin */
SELECT 1 AS id
`)), 0o644)
		require.NoError(t, err)

		task, err := pipeline.CreateTaskFromFileComments(fs)("asset.sql")
		require.NoError(t, err)
		require.NotNil(t, task.SchemaEvolution)
		require.True(t, task.SchemaEvolution.IsEnabled())
		require.NoError(t, pipeline.ValidateAssetYAML(fs, "asset.sql", pipeline.CommentTask))
	})

	t.Run("single-line comments", func(t *testing.T) {
		t.Parallel()
		fs := afero.NewMemMapFs()
		err := afero.WriteFile(fs, "asset.sql", []byte(strings.TrimSpace(`
-- @bruin.name: demo.events
-- @bruin.type: duckdb.sql
-- @bruin.materialization.type: table
-- @bruin.materialization.strategy: append
-- @bruin.schema_evolution.enabled: true
SELECT 1 AS id
`)), 0o644)
		require.NoError(t, err)

		task, err := pipeline.CreateTaskFromFileComments(fs)("asset.sql")
		require.NoError(t, err)
		require.NotNil(t, task.SchemaEvolution)
		require.True(t, task.SchemaEvolution.IsEnabled())
	})

	t.Run("explicitly disabled", func(t *testing.T) {
		t.Parallel()
		task, err := pipeline.ConvertYamlToTask([]byte(`
name: demo.events
type: duckdb.sql
schema_evolution:
  enabled: false
`))
		require.NoError(t, err)
		require.NotNil(t, task.SchemaEvolution)
		require.NotNil(t, task.SchemaEvolution.Enabled)
		require.False(t, task.SchemaEvolution.IsEnabled())
	})

	t.Run("omitted stays nil", func(t *testing.T) {
		t.Parallel()
		task, err := pipeline.ConvertYamlToTask([]byte(`
name: demo.events
type: duckdb.sql
`))
		require.NoError(t, err)
		require.Nil(t, task.SchemaEvolution)
		require.False(t, task.SchemaEvolution.IsEnabled())
	})
}

func TestBuilder_SetupDefaultsFromPipeline_SchemaEvolution(t *testing.T) {
	t.Parallel()

	boolPtr := func(v bool) *bool { return &v }
	defaults := &pipeline.DefaultValues{
		SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
	}
	p := &pipeline.Pipeline{DefaultValues: defaults}
	builder := &pipeline.Builder{}

	t.Run("applies to supported DuckDB in-place table asset", func(t *testing.T) {
		t.Parallel()
		got, err := builder.SetupDefaultsFromPipeline(t.Context(), &pipeline.Asset{
			Type: pipeline.AssetTypeDuckDBQuery,
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
		}, p)
		require.NoError(t, err)
		require.NotNil(t, got.SchemaEvolution)
		assert.True(t, got.SchemaEvolution.IsEnabled())
	})

	t.Run("respects explicit asset opt-out", func(t *testing.T) {
		t.Parallel()
		got, err := builder.SetupDefaultsFromPipeline(t.Context(), &pipeline.Asset{
			Type: pipeline.AssetTypeDuckDBQuery,
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(false)},
		}, p)
		require.NoError(t, err)
		require.NotNil(t, got.SchemaEvolution)
		assert.False(t, got.SchemaEvolution.IsEnabled())
	})

	t.Run("skips create+replace, default strategy, views, and unsupported asset types", func(t *testing.T) {
		t.Parallel()
		createReplaceAsset, err := builder.SetupDefaultsFromPipeline(t.Context(), &pipeline.Asset{
			Type: pipeline.AssetTypeDuckDBQuery,
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyCreateReplace,
			},
		}, p)
		require.NoError(t, err)
		assert.Nil(t, createReplaceAsset.SchemaEvolution)

		defaultStrategyAsset, err := builder.SetupDefaultsFromPipeline(t.Context(), &pipeline.Asset{
			Type:            pipeline.AssetTypeDuckDBQuery,
			Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
		}, p)
		require.NoError(t, err)
		assert.Nil(t, defaultStrategyAsset.SchemaEvolution)

		viewAsset, err := builder.SetupDefaultsFromPipeline(t.Context(), &pipeline.Asset{
			Type:            pipeline.AssetTypeDuckDBQuery,
			Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeView},
		}, p)
		require.NoError(t, err)
		assert.Nil(t, viewAsset.SchemaEvolution)

		pgAsset, err := builder.SetupDefaultsFromPipeline(t.Context(), &pipeline.Asset{
			Type: pipeline.AssetTypePostgresQuery,
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
		}, p)
		require.NoError(t, err)
		assert.Nil(t, pgAsset.SchemaEvolution)
	})
}
