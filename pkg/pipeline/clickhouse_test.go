package pipeline_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestClickHouseConfigSerialization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config pipeline.ClickHouseConfig
		want   string
	}{
		{name: "empty", want: "null"},
		{
			name:   "empty collections",
			config: pipeline.ClickHouseConfig{OrderBy: []string{}, Settings: map[string]string{}},
			want:   "null",
		},
		{
			name:   "engine",
			config: pipeline.ClickHouseConfig{Engine: "ReplacingMergeTree(version)"},
			want:   `{"engine":"ReplacingMergeTree(version)"}`,
		},
		{
			name:   "order by",
			config: pipeline.ClickHouseConfig{OrderBy: []string{"tenant_id", "id"}},
			want:   `{"order_by":["tenant_id","id"]}`,
		},
		{
			name:   "ttl",
			config: pipeline.ClickHouseConfig{TTL: "created_at + INTERVAL 30 DAY"},
			want:   `{"ttl":"created_at + INTERVAL 30 DAY"}`,
		},
		{
			name:   "settings",
			config: pipeline.ClickHouseConfig{Settings: map[string]string{"index_granularity": "8192", "storage_policy": "'hot'"}},
			want:   `{"settings":{"index_granularity":"8192","storage_policy":"'hot'"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := json.Marshal(tt.config)
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(got))
			assert.Equal(t, tt.want == "null", tt.config.IsZero())

			asset := pipeline.Asset{ClickHouse: tt.config}
			assetJSON, err := json.Marshal(asset)
			require.NoError(t, err)
			assetYAML, err := yaml.Marshal(asset)
			require.NoError(t, err)
			if tt.config.IsZero() {
				assert.NotContains(t, string(assetJSON), `"clickhouse"`)
				assert.NotContains(t, string(assetYAML), "clickhouse:")
				return
			}

			var fromJSON, fromYAML pipeline.Asset
			require.NoError(t, json.Unmarshal(assetJSON, &fromJSON))
			require.NoError(t, yaml.Unmarshal(assetYAML, &fromYAML))
			assert.Equal(t, tt.config, fromJSON.ClickHouse)
			assert.Equal(t, tt.config, fromYAML.ClickHouse)
		})
	}
}

func TestParseClickHouseConfig(t *testing.T) {
	t.Parallel()

	definition := strings.TrimSpace(`
name: analytics.events
type: clickhouse.sql
materialization:
  type: table
  partition_by: toYYYYMM(created_at)
clickhouse:
  engine: ReplacingMergeTree(version)
  order_by: [tenant_id, id]
  ttl: created_at + INTERVAL 30 DAY
  settings:
    index_granularity: 8192
    storage_policy: "'hot'"
`)
	want := pipeline.ClickHouseConfig{
		Engine:   "ReplacingMergeTree(version)",
		OrderBy:  []string{"tenant_id", "id"},
		TTL:      "created_at + INTERVAL 30 DAY",
		Settings: map[string]string{"index_granularity": "8192", "storage_policy": "'hot'"},
	}

	tests := []struct {
		name  string
		parse func(*testing.T) (*pipeline.Asset, error)
	}{
		{
			name: "yaml",
			parse: func(_ *testing.T) (*pipeline.Asset, error) {
				return pipeline.ConvertYamlToTask([]byte(definition))
			},
		},
		{
			name: "sql comment",
			parse: func(t *testing.T) (*pipeline.Asset, error) {
				fs := afero.NewMemMapFs()
				content := "/* @bruin\n" + definition + "\n@bruin */\nSELECT 1"
				require.NoError(t, afero.WriteFile(fs, "events.sql", []byte(content), 0o644))
				require.NoError(t, pipeline.ValidateAssetYAML(fs, "events.sql", pipeline.CommentTask))
				return pipeline.CreateTaskFromFileComments(fs)("events.sql")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			asset, err := tt.parse(t)
			require.NoError(t, err)
			assert.Equal(t, pipeline.AssetTypeClickHouse, asset.Type)
			assert.Equal(t, want, asset.ClickHouse)
			assert.Equal(t, "toYYYYMM(created_at)", asset.Materialization.PartitionBy)
		})
	}
}

func TestPipelineClickHouseDefaults(t *testing.T) {
	t.Parallel()

	var p pipeline.Pipeline
	err := yaml.Unmarshal([]byte(`
name: clickhouse-pipeline
default:
  type: clickhouse.sql
  materialization:
    type: table
    partition_by: toYYYYMM(created_at)
  clickhouse:
    engine: ReplacingMergeTree(version)
    order_by: [tenant_id, id]
    ttl: created_at + INTERVAL 30 DAY
    settings:
      index_granularity: 8192
      storage_policy: "'hot'"
`), &p)
	require.NoError(t, err)
	require.NotNil(t, p.DefaultValues)
	defaults := p.DefaultValues.ClickHouse
	assert.Equal(t, pipeline.ClickHouseConfig{
		Engine:   "ReplacingMergeTree(version)",
		OrderBy:  []string{"tenant_id", "id"},
		TTL:      "created_at + INTERVAL 30 DAY",
		Settings: map[string]string{"index_granularity": "8192", "storage_policy": "'hot'"},
	}, defaults)

	builder := &pipeline.Builder{}
	first, err := builder.SetupDefaultsFromPipeline(t.Context(), &pipeline.Asset{}, &p)
	require.NoError(t, err)
	second, err := builder.SetupDefaultsFromPipeline(t.Context(), &pipeline.Asset{}, &p)
	require.NoError(t, err)
	assert.Equal(t, defaults, first.ClickHouse)
	assert.Equal(t, defaults, second.ClickHouse)
	assert.Equal(t, pipeline.AssetTypeClickHouse, first.Type)
	assert.Equal(t, "toYYYYMM(created_at)", first.Materialization.PartitionBy)

	first.ClickHouse.OrderBy[0] = "other_id"
	first.ClickHouse.Settings["index_granularity"] = "4096"
	assert.Equal(t, "tenant_id", defaults.OrderBy[0])
	assert.Equal(t, "8192", defaults.Settings["index_granularity"])
	assert.Equal(t, defaults, second.ClickHouse)

	partial, err := builder.SetupDefaultsFromPipeline(t.Context(), &pipeline.Asset{
		ClickHouse: pipeline.ClickHouseConfig{
			Engine:   "SummingMergeTree()",
			OrderBy:  []string{},
			Settings: map[string]string{},
		},
	}, &p)
	require.NoError(t, err)
	wantPartial := defaults
	wantPartial.Engine = "SummingMergeTree()"
	assert.Equal(t, wantPartial, partial.ClickHouse)

	overridden, err := builder.SetupDefaultsFromPipeline(t.Context(), &pipeline.Asset{
		ClickHouse: pipeline.ClickHouseConfig{
			Engine:  "SummingMergeTree()",
			OrderBy: []string{"id"},
			TTL:     "created_at + INTERVAL 7 DAY",
			Settings: map[string]string{
				"index_granularity":              "2048",
				"enable_mixed_granularity_parts": "1",
			},
		},
	}, &p)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ClickHouseConfig{
		Engine:  "SummingMergeTree()",
		OrderBy: []string{"id"},
		TTL:     "created_at + INTERVAL 7 DAY",
		Settings: map[string]string{
			"index_granularity":              "2048",
			"storage_policy":                 "'hot'",
			"enable_mixed_granularity_parts": "1",
		},
	}, overridden.ClickHouse)
	assert.Equal(t, "8192", defaults.Settings["index_granularity"])
	assert.NotContains(t, defaults.Settings, "enable_mixed_granularity_parts")
}

func TestPipelineClickHouseDefaultsOnlyApplyToNativeTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		assetType              pipeline.AssetType
		materialization        pipeline.MaterializationType
		defaultType            pipeline.AssetType
		defaultMaterialization pipeline.MaterializationType
		wantInherited          bool
	}{
		{
			name: "native table", assetType: pipeline.AssetTypeClickHouse,
			materialization: pipeline.MaterializationTypeTable, wantInherited: true,
		},
		{
			name: "inherited native type", materialization: pipeline.MaterializationTypeTable,
			defaultType: pipeline.AssetTypeClickHouse, wantInherited: true,
		},
		{
			name: "inherited table materialization", assetType: pipeline.AssetTypeClickHouse,
			defaultMaterialization: pipeline.MaterializationTypeTable, wantInherited: true,
		},
		{
			name: "inherited native type and table materialization", defaultType: pipeline.AssetTypeClickHouse,
			defaultMaterialization: pipeline.MaterializationTypeTable, wantInherited: true,
		},
		{
			name: "postgres table", assetType: pipeline.AssetTypePostgresQuery,
			defaultType: pipeline.AssetTypeClickHouse, defaultMaterialization: pipeline.MaterializationTypeTable,
		},
		{
			name: "python", assetType: pipeline.AssetTypePython,
			defaultType: pipeline.AssetTypeClickHouse, defaultMaterialization: pipeline.MaterializationTypeTable,
		},
		{
			name: "ingestr", assetType: pipeline.AssetTypeIngestr,
			defaultType: pipeline.AssetTypeClickHouse, defaultMaterialization: pipeline.MaterializationTypeTable,
		},
		{
			name: "native view", assetType: pipeline.AssetTypeClickHouse,
			materialization: pipeline.MaterializationTypeView, defaultMaterialization: pipeline.MaterializationTypeTable,
		},
		{
			name: "unmaterialized native SQL", assetType: pipeline.AssetTypeClickHouse,
		},
		{
			name: "inherited non-ClickHouse type", defaultType: pipeline.AssetTypePostgresQuery,
			defaultMaterialization: pipeline.MaterializationTypeTable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for _, explicit := range []bool{false, true} {
				name := "without explicit options"
				if explicit {
					name = "preserves explicit options"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					asset := &pipeline.Asset{
						Type:            tt.assetType,
						Materialization: pipeline.Materialization{Type: tt.materialization},
					}
					defaults := &pipeline.DefaultValues{
						Type:            string(tt.defaultType),
						Materialization: pipeline.Materialization{Type: tt.defaultMaterialization},
						ClickHouse: pipeline.ClickHouseConfig{
							Engine:   "ReplacingMergeTree(version)",
							OrderBy:  []string{"id"},
							TTL:      "created_at + INTERVAL 30 DAY",
							Settings: map[string]string{"index_granularity": "8192"},
						},
					}
					var want pipeline.ClickHouseConfig
					if tt.wantInherited {
						want = defaults.ClickHouse
					}
					if explicit {
						asset.ClickHouse = pipeline.ClickHouseConfig{
							Engine:   "SummingMergeTree()",
							Settings: map[string]string{"storage_policy": "'cold'"},
						}
						want.Engine = "SummingMergeTree()"
						want.Settings = map[string]string{"storage_policy": "'cold'"}
						if tt.wantInherited {
							want.Settings["index_granularity"] = "8192"
						}
					}

					got, err := (&pipeline.Builder{}).SetupDefaultsFromPipeline(t.Context(), asset, &pipeline.Pipeline{DefaultValues: defaults})
					require.NoError(t, err)
					assert.Equal(t, want, got.ClickHouse)
				})
			}
		})
	}
}
