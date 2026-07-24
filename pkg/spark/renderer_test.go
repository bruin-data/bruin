package spark

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestRendererPreservesRuntimeStatementAndHookOrder(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "local.analytics.events",
		Type: pipeline.AssetTypeSparkQuery,
		Materialization: pipeline.Materialization{
			Type: pipeline.MaterializationTypeTable,
		},
		Hooks: pipeline.Hooks{
			Pre:  []pipeline.Hook{{Query: "SET spark.sql.adaptive.enabled = true"}},
			Post: []pipeline.Hook{{Query: "RESET spark.sql.adaptive.enabled"}},
		},
	}

	rendered, err := NewRenderer(false).Render(asset, `
-- select the namespace
USE local.analytics;
SELECT * FROM source;
SET spark.sql.shuffle.partitions = 8;
`)
	require.NoError(t, err)

	statements := []string{
		"USE local.analytics",
		"SET spark.sql.adaptive.enabled = true",
		"DROP TABLE IF EXISTS `local`.`analytics`.`events`",
		"CREATE TABLE `local`.`analytics`.`events`",
		"RESET spark.sql.adaptive.enabled",
		"SET spark.sql.shuffle.partitions = 8",
	}
	previousIndex := -1
	for _, statement := range statements {
		index := strings.Index(rendered, statement)
		require.Greater(t, index, previousIndex, "statement %q should preserve runtime order", statement)
		previousIndex = index
	}
}

func TestRendererValidatesUseBeforeMaterializing(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "analytics.events",
		Type: pipeline.AssetTypeSparkQuery,
		Materialization: pipeline.Materialization{
			Type: pipeline.MaterializationTypeTable,
		},
	}

	_, err := NewRenderer(false).Render(asset, "/* select the namespace */ USE local.analytics; SELECT 1;")
	require.EqualError(
		t,
		err,
		"materialized Spark assets that use USE must have a fully qualified catalog.schema.table name",
	)
}

func TestRendererSupportsEmptyDDLContent(t *testing.T) {
	t.Parallel()

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

	rendered, err := NewRenderer(false).Render(asset, "")
	require.NoError(t, err)
	require.Contains(t, rendered, "CREATE TABLE IF NOT EXISTS `local`.`analytics`.`events`")
}

func TestRendererSupportsSessionOnlyDDLContent(t *testing.T) {
	t.Parallel()

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

	rendered, err := NewRenderer(false).Render(asset, "SET spark.sql.adaptive.enabled = true;")
	require.NoError(t, err)
	require.Less(
		t,
		strings.Index(rendered, "SET spark.sql.adaptive.enabled = true"),
		strings.Index(rendered, "CREATE TABLE IF NOT EXISTS `local`.`analytics`.`events`"),
	)
}

func TestRendererValidatesUseForSessionOnlyDDL(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "analytics.events",
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

	_, err := NewRenderer(false).Render(asset, "SET spark.sql.adaptive.enabled = true; USE local.analytics;")
	require.EqualError(
		t,
		err,
		"materialized Spark assets that use USE must have a fully qualified catalog.schema.table name",
	)
}

func TestRendererIgnoresTrailingCommentWithoutNewline(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "local.analytics.events",
		Type: pipeline.AssetTypeSparkQuery,
		Materialization: pipeline.Materialization{
			Type: pipeline.MaterializationTypeTable,
		},
	}

	rendered, err := NewRenderer(false).Render(asset, "SELECT 1;\n-- trailing comment")
	require.NoError(t, err)
	require.Contains(t, rendered, "CREATE TABLE `local`.`analytics`.`events`")
}
