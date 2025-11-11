package mysql

import (
	"regexp"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaterializer_Render(t *testing.T) {
	t.Parallel()

	falsePtr := func() *bool {
		v := false
		return &v
	}()

	tests := []struct {
		name        string
		asset       *pipeline.Asset
		query       string
		fullRefresh bool
		wantErr     bool
		expectedErr string
		wantExact   string
		wantRegex   *regexp.Regexp
	}{
		{
			name: "returns raw query when materialization disabled",
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeNone},
			},
			query:     "SELECT 1",
			wantExact: "SELECT 1",
		},
		{
			name: "renders view",
			asset: &pipeline.Asset{
				Name:            "analytics.daily_orders",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeView},
			},
			query:     "SELECT 1",
			wantExact: "CREATE OR REPLACE VIEW analytics.daily_orders AS\nSELECT 1",
		},
		{
			name: "table defaults to create replace",
			asset: &pipeline.Asset{
				Name:            "analytics.orders",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
			},
			query: "SELECT * FROM source",
			wantExact: "DROP TABLE IF EXISTS analytics.orders;\n" +
				"CREATE TABLE analytics.orders AS\n" +
				"SELECT * FROM source;",
		},
		{
			name: "full refresh overrides strategy",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query:       "SELECT * FROM source",
			fullRefresh: true,
			wantExact: "DROP TABLE IF EXISTS analytics.orders;\n" +
				"CREATE TABLE analytics.orders AS\n" +
				"SELECT * FROM source;",
		},
		{
			name: "append emits insert",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query:     "SELECT * FROM staging",
			wantExact: "INSERT INTO analytics.orders SELECT * FROM staging",
		},
		{
			name: "incremental requires key",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDeleteInsert,
				},
			},
			query:       "SELECT id FROM source",
			wantErr:     true,
			expectedErr: "requires the `incremental_key` field to be set",
		},
		{
			name: "incremental builds transaction",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "id",
				},
			},
			query: "SELECT id, value FROM source",
			wantRegex: regexp.MustCompile(
				`(?s)^START TRANSACTION;
DROP TEMPORARY TABLE IF EXISTS __bruin_tmp_[^;\n]+;
CREATE TEMPORARY TABLE __bruin_tmp_[^;\n]+ AS SELECT id, value FROM source;
DELETE FROM analytics\.orders WHERE id IN \(SELECT DISTINCT id FROM __bruin_tmp_[^;\n]+\);
INSERT INTO analytics\.orders SELECT \* FROM __bruin_tmp_[^;\n]+;
DROP TEMPORARY TABLE IF EXISTS __bruin_tmp_[^;\n]+;
COMMIT;$`),
		},
		{
			name: "merge with custom expressions",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "col1", MergeSQL: "COALESCE(source.col1, target.col1)"},
					{Name: "col2", UpdateOnMerge: true},
					{Name: "col3"},
				},
			},
			query: "SELECT id, col1, col2, col3 FROM source",
			wantExact: "INSERT INTO analytics.orders (id, col1, col2, col3)\n" +
				"SELECT id, col1, col2, col3 FROM source\n" +
				"ON DUPLICATE KEY UPDATE col1 = COALESCE(VALUES(col1), col1), col2 = VALUES(col2);",
		},
		{
			name: "merge requires columns",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query:       "SELECT 1",
			wantErr:     true,
			expectedErr: "requires the `columns` field to be set",
		},
		{
			name: "time interval",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					IncrementalKey:  "event_time",
					TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp,
				},
			},
			query: "SELECT * FROM staging",
			wantExact: "START TRANSACTION;\n" +
				"DELETE FROM analytics.orders WHERE event_time BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}';\n" +
				"INSERT INTO analytics.orders SELECT * FROM staging;\n" +
				"COMMIT;",
		},
		{
			name: "ddl builds create table",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", PrimaryKey: true, Nullable: pipeline.DefaultTrueBool{Value: falsePtr}},
					{Name: "description", Type: "VARCHAR(255)", Description: "product info"},
				},
			},
			wantExact: "CREATE TABLE IF NOT EXISTS analytics.orders (\n" +
				"id INT NOT NULL,\n" +
				"description VARCHAR(255) COMMENT 'product info',\n" +
				"PRIMARY KEY (id)\n" +
				");",
		},
		{
			name: "scd2 by time unsupported",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByTime,
				},
			},
			query:       "SELECT 1",
			wantErr:     true,
			expectedErr: "materialization strategy scd2_by_time is not supported",
		},
		{
			name: "scd2 by column unsupported",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
			},
			query:       "SELECT 1",
			wantErr:     true,
			expectedErr: "materialization strategy scd2_by_column is not supported",
		},
		{
			name: "unsupported view strategy",
			asset: &pipeline.Asset{
				Name: "analytics.daily_orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeView,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query:       "SELECT 1",
			wantErr:     true,
			expectedErr: "materialization strategy merge is not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mat := NewMaterializer(tt.fullRefresh)
			got, err := mat.Render(tt.asset, tt.query)

			if tt.wantErr {
				require.Error(t, err)
				if tt.expectedErr != "" {
					assert.Contains(t, err.Error(), tt.expectedErr)
				}
				return
			}

			require.NoError(t, err)
			if tt.wantRegex != nil {
				assert.Regexp(t, tt.wantRegex, got)
			} else {
				assert.Equal(t, tt.wantExact, got)
			}
		})
	}
}
