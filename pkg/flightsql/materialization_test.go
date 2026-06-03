package flightsql

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaterializer_Render(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		asset       *pipeline.Asset
		query       string
		fullRefresh bool
		want        string
		wantErr     bool
	}{
		{
			name: "no materialization returns query as-is",
			asset: &pipeline.Asset{
				Name:            "my_table",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeNone},
			},
			query: "SELECT 1",
			want:  "SELECT 1",
		},
		{
			name: "table create+replace",
			asset: &pipeline.Asset{
				Name: "schema.my_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyCreateReplace,
				},
			},
			query: "SELECT * FROM source",
			want: `
DROP TABLE IF EXISTS "schema"."my_table";
CREATE TABLE "schema"."my_table" AS
SELECT * FROM source;`,
		},
		{
			name: "table append",
			asset: &pipeline.Asset{
				Name: "my_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query: "SELECT * FROM source",
			want:  `INSERT INTO "my_table" SELECT * FROM source`,
		},
		{
			name: "view",
			asset: &pipeline.Asset{
				Name: "my_view",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeView,
					Strategy: pipeline.MaterializationStrategyNone,
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE VIEW \"my_view\" AS\nSELECT 1",
		},
		{
			name: "truncate+insert",
			asset: &pipeline.Asset{
				Name: "my_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyTruncateInsert,
				},
			},
			query: "SELECT * FROM source",
			want:  "TRUNCATE TABLE \"my_table\";\nINSERT INTO \"my_table\" SELECT * FROM source;",
		},
		{
			name: "incremental requires key",
			asset: &pipeline.Asset{
				Name: "my_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDeleteInsert,
				},
			},
			query:   "SELECT * FROM source",
			wantErr: true,
		},
		{
			name: "merge is unsupported",
			asset: &pipeline.Asset{
				Name: "my_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query:   "SELECT * FROM source",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := NewMaterializer(tt.fullRefresh)
			got, err := m.Render(tt.asset, tt.query)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestMaterializer_Dialect verifies that the configured dialect changes how
// identifiers are quoted: Dremio (default) uses ANSI double quotes, while Sail
// (Spark SQL) uses backticks.
func TestMaterializer_Dialect(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "schema.my_table",
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: pipeline.MaterializationStrategyCreateReplace,
		},
	}

	tests := []struct {
		dialect string
		want    string
	}{
		{
			dialect: "dremio",
			want: `
DROP TABLE IF EXISTS "schema"."my_table";
CREATE TABLE "schema"."my_table" AS
SELECT * FROM source;`,
		},
		{
			dialect: "sail",
			want:    "\nDROP TABLE IF EXISTS `schema`.`my_table`;\nCREATE TABLE `schema`.`my_table` AS\nSELECT * FROM source;",
		},
		{
			dialect: "pysail",
			want:    "\nDROP TABLE IF EXISTS `schema`.`my_table`;\nCREATE TABLE `schema`.`my_table` AS\nSELECT * FROM source;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.dialect, func(t *testing.T) {
			t.Parallel()
			m := NewMaterializerForDialect(tt.dialect, false)
			got, err := m.Render(asset, "SELECT * FROM source")
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
