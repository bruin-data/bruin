package clickhouse

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func declarationTestConfig() pipeline.ClickHouseConfig {
	return pipeline.ClickHouseConfig{
		Engine:  "MergeTree",
		OrderBy: []string{"id"},
		Indexes: []pipeline.ClickHouseIndex{
			{Name: "idx_id", Expression: "id", Type: "minmax"},
			{Name: "`idx name`", Expression: "lower(name)", Type: "bloom_filter(0.01)", Granularity: 4},
		},
		Projections: []pipeline.ClickHouseProjection{{Name: "`p by id`", Select: "SELECT id, name ORDER BY id"}},
	}
}

const declarationTestAlter = " ADD INDEX idx_id id TYPE minmax, ADD INDEX `idx name` lower(name) TYPE bloom_filter(0.01) GRANULARITY 4, ADD PROJECTION `p by id` (SELECT id, name ORDER BY id)"

func TestMaterializer_DeclarationsReplacement(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		table      string
		strategy   pipeline.MaterializationStrategy
		full       bool
		parameters pipeline.ParameterMap
	}{
		{name: "implicit", table: "events"},
		{name: "create replace", table: "analytics.events", strategy: pipeline.MaterializationStrategyCreateReplace},
		{name: "global full refresh", table: "analytics.events", strategy: pipeline.MaterializationStrategyTimeInterval, full: true},
		{name: "asset full refresh", table: "analytics.events", strategy: pipeline.MaterializationStrategyAppend, parameters: pipeline.ParameterMap{"full_refresh": true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			asset := &pipeline.Asset{
				Name: tt.table, Parameters: tt.parameters,
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: tt.strategy},
				ClickHouse:      declarationTestConfig(),
			}
			stage := "__bruin_tmp_abcefghi_replacement"
			if strings.Contains(tt.table, ".") {
				stage = "analytics." + stage
			}
			const query = "SELECT id, name FROM source"
			actual, cleanup, err := NewMaterializer(tt.full).RenderWithCleanup(asset, query+";\n")
			require.NoError(t, err)
			assert.Equal(t, []string{
				"CREATE TABLE " + stage + " ENGINE = MergeTree ORDER BY (id) EMPTY AS " + query,
				"ALTER TABLE " + stage + declarationTestAlter,
				"INSERT INTO " + stage + " " + query,
				"CREATE TABLE IF NOT EXISTS " + tt.table + " AS " + stage,
				"EXCHANGE TABLES " + tt.table + " AND " + stage,
				"DROP TABLE IF EXISTS " + stage,
			}, actual)
			assert.Equal(t, []string{"DROP TABLE IF EXISTS " + stage}, cleanup)
			assert.NotContains(t, cleanup, "DROP TABLE IF EXISTS "+tt.table)
			assert.Empty(t, asset.Columns, "declarations must support query-inferred columns")
			assert.Equal(t, tt.strategy, asset.Materialization.Strategy)
		})
	}
}

func TestMaterializer_DeclarationsDDL(t *testing.T) {
	t.Parallel()
	for _, cluster := range []string{"", "analytics"} {
		t.Run("cluster="+cluster, func(t *testing.T) {
			t.Parallel()
			asset := &pipeline.Asset{
				Name:            "events",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyDDL},
				ClickHouse:      declarationTestConfig(),
				Columns:         []pipeline.Column{{Name: "id", Type: "UInt64"}, {Name: "name", Type: "String"}},
			}
			clusterClause := ""
			if cluster != "" {
				clusterClause = " ON CLUSTER `analytics`"
			}
			actual, cleanup, err := NewMaterializer(true, cluster).RenderWithCleanup(asset, "")
			require.NoError(t, err)
			assert.Equal(t, []string{
				"CREATE TABLE IF NOT EXISTS events" + clusterClause + " (\n" +
					"id UInt64,\nname String,\n" +
					"INDEX idx_id id TYPE minmax,\n" +
					"INDEX `idx name` lower(name) TYPE bloom_filter(0.01) GRANULARITY 4,\n" +
					"PROJECTION `p by id` (SELECT id, name ORDER BY id)\n" +
					")\nENGINE = MergeTree\nORDER BY (id)",
			}, actual)
			assert.Empty(t, cleanup)
		})
	}
}

func TestMaterializer_DeclarationsClusterReplacement(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		strategy pipeline.MaterializationStrategy
		full     bool
	}{
		{name: "implicit"},
		{name: "create replace", strategy: pipeline.MaterializationStrategyCreateReplace},
		{name: "full refresh", strategy: pipeline.MaterializationStrategyAppend, full: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			asset := &pipeline.Asset{
				Name:            "analytics.events",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: tt.strategy},
				ClickHouse:      declarationTestConfig(),
			}
			asset.ClickHouse.Engine = "ReplicatedMergeTree()"
			const query = "SELECT id, name FROM source"
			actual, cleanup, err := NewMaterializer(tt.full, "analytics").RenderWithCleanup(asset, query+";")
			require.NoError(t, err)
			assert.Equal(t, []string{
				"DROP TABLE IF EXISTS analytics.events ON CLUSTER `analytics` SYNC",
				"CREATE TABLE analytics.events ON CLUSTER `analytics` ENGINE = ReplicatedMergeTree() ORDER BY (id) EMPTY AS " + query,
				"ALTER TABLE analytics.events ON CLUSTER `analytics`" + declarationTestAlter,
				"INSERT INTO analytics.events " + query,
			}, actual)
			assert.Empty(t, cleanup, "failure cleanup must not drop the cluster target")
		})
	}
}

func TestMaterializer_DeclarationsSCD2(t *testing.T) {
	t.Parallel()
	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategySCD2ByColumn, pipeline.MaterializationStrategySCD2ByTime,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			t.Parallel()
			asset := scd2TestAsset(strategy)
			asset.ClickHouse = declarationTestConfig()
			const stage = "my.__bruin_tmp_abcefghi"
			actual, cleanup, err := NewMaterializer(false).RenderWithCleanup(asset, "SELECT id, name, updated_at FROM source")
			require.NoError(t, err)
			require.Len(t, actual, 11)
			assert.Contains(t, actual[0], "CREATE TABLE "+stage+"_source ENGINE = MergeTree() PRIMARY KEY (id) AS SELECT")
			assert.Equal(t, []string{
				"CREATE TABLE " + stage + "_schema ENGINE = MergeTree() ORDER BY tuple() EMPTY AS SELECT * FROM " + stage + "_source",
				"ALTER TABLE " + stage + "_schema" + declarationTestAlter,
				"CREATE TABLE IF NOT EXISTS my.history AS " + stage + "_schema ENGINE = MergeTree PRIMARY KEY (id) ORDER BY (id)",
			}, actual[1:4])
			assert.Contains(t, actual[4], "CREATE TABLE "+stage+"_changes ENGINE = MergeTree() PRIMARY KEY (id) AS SELECT")
			assert.Contains(t, actual[5], "INSERT INTO my.history")
			assert.Contains(t, actual[6], "DELETE FROM my.history")
			assert.Contains(t, actual[7], "INSERT INTO my.history")
			assert.Equal(t, []string{
				"DROP TABLE IF EXISTS " + stage + "_changes",
				"DROP TABLE IF EXISTS " + stage + "_source",
				"DROP TABLE IF EXISTS " + stage + "_schema",
			}, cleanup)
			assert.Equal(t, cleanup, actual[8:])
			assert.NotContains(t, strings.Join(actual, "\n"), "ALTER TABLE my.history")
			for _, query := range []string{actual[0], actual[4]} {
				assert.NotContains(t, query, "INDEX")
				assert.NotContains(t, query, "PROJECTION")
			}
		})
	}
}

func TestMaterializer_DeclarationsSCD2FullRefreshCleanup(t *testing.T) {
	t.Parallel()
	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategySCD2ByColumn, pipeline.MaterializationStrategySCD2ByTime,
	} {
		for _, global := range []bool{false, true} {
			name := "asset full refresh"
			if global {
				name = "global full refresh"
			}
			t.Run(string(strategy)+"/"+name, func(t *testing.T) {
				t.Parallel()
				asset := scd2TestAsset(strategy)
				asset.ClickHouse = declarationTestConfig()
				if !global {
					asset.Parameters = pipeline.ParameterMap{"full_refresh": true}
				}
				const stage = "my.__bruin_tmp_abcefghi_replacement"
				actual, cleanup, err := NewMaterializer(global).RenderWithCleanup(asset, "SELECT id, name, updated_at FROM source")
				require.NoError(t, err)
				require.Len(t, actual, 6)
				assert.Contains(t, actual[0], "CREATE TABLE "+stage+" ENGINE = MergeTree PRIMARY KEY (id) ORDER BY (id) EMPTY AS SELECT src.id, src.name, src.updated_at")
				assert.Contains(t, actual[0], "AS _valid_from")
				assert.Contains(t, actual[0], "AS _valid_until")
				assert.Contains(t, actual[0], "TRUE AS _is_current")
				assert.Equal(t, "ALTER TABLE "+stage+declarationTestAlter, actual[1])
				assert.Contains(t, actual[2], "INSERT INTO "+stage+" SELECT src.id, src.name, src.updated_at")
				assert.Equal(t, "CREATE TABLE IF NOT EXISTS my.history AS "+stage, actual[3])
				assert.Equal(t, "EXCHANGE TABLES my.history AND "+stage, actual[4])
				assert.Equal(t, []string{"DROP TABLE IF EXISTS " + stage}, cleanup)
				assert.Equal(t, cleanup[0], actual[5])
				assert.NotContains(t, strings.Join(actual, "\n"), "DELETE FROM my.history")
			})
		}
	}
}

func TestMaterializer_DeclarationsSCD2SettingsWithoutEngine(t *testing.T) {
	t.Parallel()
	asset := scd2TestAsset(pipeline.MaterializationStrategySCD2ByColumn)
	asset.ClickHouse = pipeline.ClickHouseConfig{
		OrderBy: []string{"id", "_valid_from"},
		Settings: map[string]string{
			"lightweight_mutation_projection_mode": "'drop'",
		},
		Projections: []pipeline.ClickHouseProjection{{Name: "p_by_id", Select: "SELECT id, count() GROUP BY id"}},
	}
	queries, err := NewMaterializer(false).Render(asset, "SELECT id, name, updated_at FROM source")
	require.NoError(t, err)
	require.Len(t, queries, 11)
	assert.Equal(t, "CREATE TABLE IF NOT EXISTS my.history AS my.__bruin_tmp_abcefghi_schema ENGINE = MergeTree() PRIMARY KEY (id) ORDER BY (id, _valid_from) SETTINGS lightweight_mutation_projection_mode = 'drop'", queries[3])
	queries, err = NewMaterializer(true).Render(asset, "SELECT id, name, updated_at FROM source")
	require.NoError(t, err)
	require.Len(t, queries, 6)
	assert.Contains(t, queries[0], "ENGINE = MergeTree() PRIMARY KEY (id) ORDER BY (id, _valid_from) SETTINGS lightweight_mutation_projection_mode = 'drop' EMPTY AS SELECT")
	asset.ClickHouse.Projections = nil
	queries, err = NewMaterializer(false).Render(asset, "SELECT id, name, updated_at FROM source")
	require.NoError(t, err)
	assert.Contains(t, queries[1], "ENGINE = MergeTree() PRIMARY KEY (id) ORDER BY (id, _valid_from) SETTINGS lightweight_mutation_projection_mode = 'drop' EMPTY AS SELECT")
	assert.Empty(t, asset.ClickHouse.Engine)
}
