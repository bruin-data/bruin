package starrocks

import (
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
		wantErr     string
		want        string
	}{
		{
			name: "returns raw query when materialization disabled",
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeNone},
			},
			query: "SELECT 1",
			want:  "SELECT 1",
		},
		{
			name: "renders view with drop and create",
			asset: &pipeline.Asset{
				Name:            "analytics.daily_orders",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeView},
			},
			query: "SELECT 1",
			want:  "DROP VIEW IF EXISTS `analytics`.`daily_orders`;\nCREATE VIEW `analytics`.`daily_orders` AS\nSELECT 1;",
		},
		{
			name: "table defaults to create replace with single replica",
			asset: &pipeline.Asset{
				Name:            "analytics.orders",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
			},
			query: "SELECT * FROM source",
			want: "DROP TABLE IF EXISTS `analytics`.`orders`;\n" +
				"CREATE TABLE `analytics`.`orders`\n" +
				"PROPERTIES (\"replication_num\" = \"1\")\n" +
				"AS\n" +
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
			query: "SELECT * FROM staging",
			want:  "INSERT INTO `analytics`.`orders` SELECT * FROM staging;",
		},
		{
			name: "truncate insert swaps replacement table",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyTruncateInsert,
				},
			},
			query: "SELECT * FROM staging",
			want: "DROP TABLE IF EXISTS `analytics`.`__bruin_tmp_orders_abcefghi_replacement`;\n" +
				"CREATE TABLE `analytics`.`__bruin_tmp_orders_abcefghi_replacement`\n" +
				"PROPERTIES (\"replication_num\" = \"1\")\n" +
				"AS\n" +
				"SELECT * FROM staging;\n" +
				"CREATE TABLE IF NOT EXISTS `analytics`.`orders` LIKE `analytics`.`__bruin_tmp_orders_abcefghi_replacement`;\n" +
				"ALTER TABLE `analytics`.`orders` SWAP WITH `__bruin_tmp_orders_abcefghi_replacement`;",
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
			query:   "SELECT id FROM source",
			wantErr: "requires the `incremental_key` field to be set",
		},
		{
			name: "incremental builds delete insert with swap",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "id",
				},
			},
			query: "SELECT id, value FROM source",
			want: "DROP TABLE IF EXISTS `analytics`.`__bruin_tmp_orders_abcefghi_new`;\n" +
				"CREATE TABLE `analytics`.`__bruin_tmp_orders_abcefghi_new`\n" +
				"PROPERTIES (\"replication_num\" = \"1\")\n" +
				"AS\n" +
				"SELECT id, value FROM source;\n" +
				"DROP TABLE IF EXISTS `analytics`.`__bruin_tmp_orders_abcefghi_replacement`;\n" +
				"CREATE TABLE `analytics`.`__bruin_tmp_orders_abcefghi_replacement`\n" +
				"PROPERTIES (\"replication_num\" = \"1\")\n" +
				"AS\n" +
				"SELECT `target`.*\n" +
				"FROM `analytics`.`orders` AS `target`\n" +
				"WHERE NOT EXISTS (\n" +
				"  SELECT 1\n" +
				"  FROM `analytics`.`__bruin_tmp_orders_abcefghi_new` AS `new_rows`\n" +
				"  WHERE (`target`.`id` = `new_rows`.`id` OR (`target`.`id` IS NULL AND `new_rows`.`id` IS NULL))\n" +
				")\n" +
				"UNION ALL\n" +
				"SELECT * FROM `analytics`.`__bruin_tmp_orders_abcefghi_new`;\n" +
				"ALTER TABLE `analytics`.`orders` SWAP WITH `__bruin_tmp_orders_abcefghi_replacement`;\n" +
				"DROP TABLE IF EXISTS `analytics`.`__bruin_tmp_orders_abcefghi_new`;",
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
			want: "DELETE FROM `analytics`.`orders` WHERE `event_time` BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}';\n" +
				"INSERT INTO `analytics`.`orders` SELECT * FROM staging;",
		},
		{
			name: "ddl builds duplicate key table",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", Nullable: pipeline.DefaultTrueBool{Value: falsePtr}},
					{Name: "description", Type: "VARCHAR(255)", Description: "product info"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS `analytics`.`orders` (\n" +
				"`id` INT NOT NULL,\n" +
				"`description` VARCHAR(255) COMMENT 'product info'\n" +
				")\n" +
				"DUPLICATE KEY(`id`)\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
				"PROPERTIES (\"replication_num\" = \"1\");",
		},
		{
			name: "ddl emits partition by clause",
			asset: &pipeline.Asset{
				Name: "analytics.events",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
				StarRocks: pipeline.StarRocksConfig{
					DistributedBy: []string{"id"},
					PartitionBy:   []string{"event_date"},
					Buckets:       4,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT"},
					{Name: "event_date", Type: "DATE"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS `analytics`.`events` (\n" +
				"`id` INT,\n" +
				"`event_date` DATE\n" +
				")\n" +
				"DUPLICATE KEY(`id`)\n" +
				"PARTITION BY (`event_date`)\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 4\n" +
				"PROPERTIES (\"replication_num\" = \"1\");",
		},
		{
			name: "merge creates primary key table and upserts",
			asset: &pipeline.Asset{
				Name: "analytics.accounts",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "status", Type: "VARCHAR(20)", UpdateOnMerge: true},
					{Name: "amount", Type: "INT"},
				},
			},
			query: "SELECT id, status, amount FROM staging",
			want: "CREATE TABLE IF NOT EXISTS `analytics`.`accounts` (\n" +
				"`id` INT NOT NULL,\n" +
				"`status` VARCHAR(20),\n" +
				"`amount` INT\n" +
				")\n" +
				"PRIMARY KEY(`id`)\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
				"PROPERTIES (\"replication_num\" = \"1\");\n" +
				"INSERT INTO `analytics`.`accounts` (`id`, `status`, `amount`)\n" +
				"SELECT id, status, amount FROM staging;",
		},
		{
			name: "merge rejects per-column merge expressions",
			asset: &pipeline.Asset{
				Name: "analytics.accounts",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "balance", Type: "INT", MergeSQL: "target.`balance` + source.`balance`"},
				},
			},
			query:   "SELECT id, balance FROM staging",
			wantErr: "does not support per-column merge expressions",
		},
		{
			name: "merge requires primary key",
			asset: &pipeline.Asset{
				Name: "analytics.accounts",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT"},
				},
			},
			query:   "SELECT id FROM staging",
			wantErr: "requires the `primary_key` field to be set",
		},
		{
			name: "merge rejects non primary key table model",
			asset: &pipeline.Asset{
				Name: "analytics.accounts",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				StarRocks: pipeline.StarRocksConfig{TableModel: "duplicate_key"},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", PrimaryKey: true},
				},
			},
			query:   "SELECT id FROM staging",
			wantErr: "requires StarRocks table_model \"primary_key\"",
		},
		{
			name: "full refresh merge creates primary key table before insert",
			asset: &pipeline.Asset{
				Name: "analytics.accounts",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				StarRocks: pipeline.StarRocksConfig{
					DistributedBy: []string{"account_id"},
					Buckets:       2,
					Properties:    map[string]string{"compression": "zstd"},
				},
				Columns: []pipeline.Column{
					{Name: "status", Type: "VARCHAR(20)", UpdateOnMerge: true},
					{Name: "account_id", Type: "STRING", PrimaryKey: true},
					{Name: "balance", Type: "INT"},
				},
			},
			query:       "SELECT status, account_id, balance FROM staging",
			fullRefresh: true,
			want: "DROP TABLE IF EXISTS `analytics`.`accounts`;\n" +
				"CREATE TABLE `analytics`.`accounts` (\n" +
				"`account_id` VARCHAR(65533) NOT NULL,\n" +
				"`status` VARCHAR(20),\n" +
				"`balance` INT\n" +
				")\n" +
				"PRIMARY KEY(`account_id`)\n" +
				"DISTRIBUTED BY HASH(`account_id`) BUCKETS 2\n" +
				"PROPERTIES (\"compression\" = \"zstd\", \"replication_num\" = \"1\");\n" +
				"INSERT INTO `analytics`.`accounts` (`status`, `account_id`, `balance`)\n" +
				"SELECT status, account_id, balance FROM staging;",
		},
		{
			name: "scd2 by column is not supported",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
			},
			query:   "SELECT id FROM source",
			wantErr: "materialization strategy scd2_by_column is not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := NewMaterializer(tt.fullRefresh).Render(tt.asset, tt.query)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "`analytics`.`orders`", quoteIdentifier("analytics.orders"))
	assert.Equal(t, "`odd``name`", quoteIdentifier("odd`name"))
}
