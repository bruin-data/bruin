package mysql

import (
	"fmt"
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
		name         string
		asset        *pipeline.Asset
		query        string
		fullRefresh  bool
		wantErr      bool
		expectedErr  string
		wantExact    string
		wantTemplate string
		wantRegex    *regexp.Regexp
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
			name: "merge generates on duplicate key upsert",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "value", UpdateOnMerge: true},
				},
			},
			query: "SELECT id, value FROM source",
			wantExact: "INSERT INTO analytics.orders (id, value)\n" +
				"SELECT source.id, source.value\n" +
				"FROM (\n" +
				"SELECT id, value FROM source\n" +
				") AS source\n" +
				"ON DUPLICATE KEY UPDATE\n" +
				"    value = VALUES(value);",
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
			expectedErr: "incremental_key is required for SCD2_by_time strategy",
		},
		{
			name: "scd2 by time incremental",
			asset: &pipeline.Asset{
				Name: "analytics.history",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "event_time",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "event_time", Type: "TIMESTAMP"},
					{Name: "country", Type: "VARCHAR(16)"},
				},
			},
			query: "SELECT id, event_time, country FROM source",
			wantTemplate: "START TRANSACTION;\n" +
				"DROP TEMPORARY TABLE IF EXISTS %[1]s;\n" +
				"CREATE TEMPORARY TABLE %[1]s AS SELECT id, event_time, country FROM source;\n" +
				"UPDATE analytics.history AS target JOIN %[1]s AS source ON target.id = source.id SET target._valid_until = CAST(source.event_time AS DATETIME), target._is_current = FALSE WHERE target._is_current = TRUE AND target._valid_from < CAST(source.event_time AS DATETIME);\n" +
				"UPDATE analytics.history AS target LEFT JOIN %[1]s AS source ON target.id = source.id SET target._valid_until = CURRENT_TIMESTAMP, target._is_current = FALSE WHERE target._is_current = TRUE AND source.id IS NULL;\n" +
				"INSERT INTO analytics.history (id, event_time, country, _valid_from, _valid_until, _is_current)\n" +
				"SELECT source.id, source.event_time, source.country, CAST(source.event_time AS DATETIME), '9999-12-31 23:59:59', TRUE\n" +
				"FROM %[1]s AS source\n" +
				"LEFT JOIN analytics.history AS current ON current.id = source.id AND current._is_current = TRUE\n" +
				"WHERE current.id IS NULL OR current._valid_from < CAST(source.event_time AS DATETIME);\n" +
				"DROP TEMPORARY TABLE IF EXISTS %[1]s;\n" +
				"COMMIT;",
		},
		{
			name: "scd2 by time full refresh",
			asset: &pipeline.Asset{
				Name: "analytics.history",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "event_time",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "event_time", Type: "DATETIME"},
					{Name: "country", Type: "VARCHAR(16)"},
				},
			},
			query:       "SELECT id, event_time, country FROM source",
			fullRefresh: true,
			wantExact: "DROP TABLE IF EXISTS analytics.history;\n" +
				"CREATE TABLE analytics.history AS\n" +
				"SELECT\n" +
				"  src.id,\n" +
				"  src.event_time,\n" +
				"  src.country,\n" +
				"  CAST(src.event_time AS DATETIME) AS _valid_from,\n" +
				"  '9999-12-31 23:59:59' AS _valid_until,\n" +
				"  TRUE AS _is_current\n" +
				"FROM (\n" +
				"SELECT id, event_time, country FROM source\n" +
				") AS src;",
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
		{
			name: "truncate insert builds transaction",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyTruncateInsert,
					IncrementalKey: "ignored",
				},
			},
			query: "SELECT * FROM staging",
			wantExact: "START TRANSACTION;\n" +
				"TRUNCATE TABLE analytics.orders;\n" +
				"INSERT INTO analytics.orders SELECT * FROM staging;\n" +
				"COMMIT;",
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
			switch {
			case tt.wantRegex != nil:
				assert.Regexp(t, tt.wantRegex, got)
			case tt.wantTemplate != "":
				re := regexp.MustCompile(`__bruin_scd2_time_tmp_[a-z0-9]+`)
				tempName := re.FindString(got)
				require.NotEmpty(t, tempName)
				expected := fmt.Sprintf(tt.wantTemplate, tempName)
				assert.Equal(t, expected, got)
			default:
				assert.Equal(t, tt.wantExact, got)
			}
		})
	}
}
