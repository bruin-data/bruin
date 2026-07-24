package spark

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaterializerMerge(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "catalog.analytics.accounts",
		Columns: []pipeline.Column{
			{Name: "account_id", PrimaryKey: true},
			{Name: "account_type", PrimaryKey: true},
			{Name: "account_name", UpdateOnMerge: true},
			{Name: "score", MergeSQL: "GREATEST(target.score, source.score)"},
			{Name: "created_at"},
		},
		Materialization: pipeline.Materialization{
			Type:                 pipeline.MaterializationTypeTable,
			Strategy:             pipeline.MaterializationStrategyMerge,
			IncrementalPredicate: "target.created_at >= DATE '2026-01-01'",
		},
	}

	actual, err := NewMaterializer(false).Render(asset, "SELECT * FROM updates;")
	require.NoError(t, err)
	require.Equal(
		t, "MERGE INTO `catalog`.`analytics`.`accounts` target\n"+
			"USING (SELECT * FROM updates) source\n"+
			"ON source.`account_id` <=> target.`account_id` AND source.`account_type` <=> target.`account_type` AND (target.created_at >= DATE '2026-01-01')\n"+
			"WHEN MATCHED THEN UPDATE SET target.`account_name` = source.`account_name`, target.`score` = GREATEST(target.score, source.score)\n"+
			"WHEN NOT MATCHED THEN INSERT (`account_id`, `account_type`, `account_name`, `score`, `created_at`) VALUES (source.`account_id`, source.`account_type`, source.`account_name`, source.`score`, source.`created_at`);",
		actual,
	)
}

func TestMaterializerMergeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		columns []pipeline.Column
		error   string
	}{
		{name: "columns are required", error: "requires the `columns` field"},
		{
			name:    "primary key is required",
			columns: []pipeline.Column{{Name: "account_id"}},
			error:   "requires the `primary_key` field",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			asset := &pipeline.Asset{
				Name:    "analytics.accounts",
				Columns: test.columns,
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			}

			_, err := NewMaterializer(false).Render(asset, "SELECT 1")
			require.ErrorContains(t, err, test.error)
		})
	}
}

func TestMaterializerMergeFullRefreshCreatesTable(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name:    "analytics.accounts",
		Columns: []pipeline.Column{{Name: "account_id", PrimaryKey: true}},
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: pipeline.MaterializationStrategyMerge,
		},
	}

	actual, err := NewMaterializer(true).Render(asset, "SELECT 1 AS account_id")
	require.NoError(t, err)
	require.Equal(
		t, "DROP TABLE IF EXISTS `analytics`.`accounts`;\n"+
			"CREATE TABLE `analytics`.`accounts`\n"+
			"AS\n"+
			"SELECT 1 AS account_id;",
		actual,
	)
}

func TestMaterializerCreateReplaceLayout(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "catalog.analytics.events",
		Materialization: pipeline.Materialization{
			Type:        pipeline.MaterializationTypeTable,
			Strategy:    pipeline.MaterializationStrategyCreateReplace,
			PartitionBy: "days(event_at)",
			ClusterBy:   []string{"tenant_id", "event_at DESC"},
		},
	}

	actual, err := NewMaterializer(false).Render(asset, "SELECT * FROM incoming_events;")
	require.NoError(t, err)
	require.Equal(
		t,
		"DROP TABLE IF EXISTS `catalog`.`analytics`.`events`;\n"+
			"CREATE TABLE `catalog`.`analytics`.`events`\n"+
			"PARTITIONED BY (days(event_at))\n"+
			"AS\n"+
			"SELECT * FROM incoming_events;\n"+
			"ALTER TABLE `catalog`.`analytics`.`events` WRITE ORDERED BY tenant_id, event_at DESC;",
		actual,
	)
}

func TestMaterializerDDLLayout(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "catalog.analytics.events",
		Columns: []pipeline.Column{
			{Name: "event_id", Type: "BIGINT", Description: "event's identifier"},
			{Name: "event_at", Type: "TIMESTAMP"},
			{Name: "category", Type: "STRING"},
		},
		Materialization: pipeline.Materialization{
			Type:        pipeline.MaterializationTypeTable,
			Strategy:    pipeline.MaterializationStrategyDDL,
			PartitionBy: "days(event_at)",
			ClusterBy:   []string{"category", "event_id"},
		},
	}

	actual, err := NewMaterializer(false).Render(asset, "")
	require.NoError(t, err)
	require.Equal(
		t,
		"CREATE TABLE IF NOT EXISTS `catalog`.`analytics`.`events` (\n"+
			"    `event_id` BIGINT COMMENT 'event''s identifier',\n"+
			"    `event_at` TIMESTAMP,\n"+
			"    `category` STRING\n"+
			")\n"+
			"PARTITIONED BY (days(event_at));\n"+
			"ALTER TABLE `catalog`.`analytics`.`events` WRITE ORDERED BY category, event_id;",
		actual,
	)
}

func TestMaterializerDDLRequiresColumns(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "analytics.events",
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: pipeline.MaterializationStrategyDDL,
		},
	}

	_, err := NewMaterializer(false).Render(asset, "")
	require.ErrorContains(t, err, "requires the `columns` field")
}

func TestMaterializerSCD2ByColumnFullRefresh(t *testing.T) {
	t.Parallel()

	asset := scd2Asset(pipeline.MaterializationStrategySCD2ByColumn)
	asset.Materialization.IncrementalKey = "updated_at"

	actual, err := NewMaterializer(true).Render(asset, "SELECT * FROM customer_updates;")
	require.NoError(t, err)
	assert.Contains(t, actual, "DROP TABLE IF EXISTS `catalog`.`analytics`.`customers`;")
	assert.Contains(t, actual, "PARTITIONED BY (days(_valid_from))")
	assert.Contains(t, actual, "CAST(src.`updated_at` AS TIMESTAMP) AS _valid_from")
	assert.Contains(t, actual, "TIMESTAMP '9999-12-31 00:00:00' AS _valid_until")
	assert.Contains(
		t,
		actual,
		"ALTER TABLE `catalog`.`analytics`.`customers` WRITE ORDERED BY _is_current, customer_id;",
	)
}

func TestMaterializerSCD2ByTimeFullRefreshWithCustomLayout(t *testing.T) {
	t.Parallel()

	asset := scd2Asset(pipeline.MaterializationStrategySCD2ByTime)
	asset.Materialization.IncrementalKey = "updated_at"
	asset.Materialization.PartitionBy = "months(updated_at)"
	asset.Materialization.ClusterBy = []string{"customer_name", "_is_current"}

	actual, err := NewMaterializer(true).Render(asset, "SELECT * FROM customer_updates")
	require.NoError(t, err)
	assert.Contains(t, actual, "PARTITIONED BY (months(updated_at))")
	assert.Contains(t, actual, "CAST(src.`updated_at` AS TIMESTAMP) AS _valid_from")
	assert.Contains(
		t,
		actual,
		"ALTER TABLE `catalog`.`analytics`.`customers` WRITE ORDERED BY customer_name, _is_current;",
	)
}

func TestMaterializerSCD2ByColumnIncremental(t *testing.T) {
	t.Parallel()

	asset := scd2Asset(pipeline.MaterializationStrategySCD2ByColumn)
	asset.Materialization.IncrementalKey = "updated_at"

	actual, err := NewMaterializer(false).Render(asset, "SELECT * FROM customer_updates;")
	require.NoError(t, err)
	assert.Contains(t, actual, "MERGE INTO `catalog`.`analytics`.`customers` AS target")
	assert.Contains(t, actual, "t1.`customer_id` <=> s1.`customer_id`")
	assert.Contains(t, actual, "NOT (t1.`customer_name` <=> s1.`customer_name`)")
	assert.Contains(t, actual, "NOT (target.`customer_name` <=> source.`customer_name`)")
	assert.Contains(t, actual, "target._valid_until = CAST(source.`updated_at` AS TIMESTAMP)")
	assert.Contains(
		t,
		actual,
		"INSERT (`customer_id`, `customer_name`, `updated_at`, _valid_from, _valid_until, _is_current)",
	)
	assert.Contains(t, actual, "WHEN NOT MATCHED BY SOURCE AND target._is_current THEN")
}

func TestMaterializerSCD2ByTimeIncremental(t *testing.T) {
	t.Parallel()

	asset := scd2Asset(pipeline.MaterializationStrategySCD2ByTime)
	asset.Materialization.IncrementalKey = "updated_at"

	actual, err := NewMaterializer(false).Render(asset, "SELECT * FROM customer_updates;")
	require.NoError(t, err)
	assert.Contains(t, actual, "t1._valid_from < CAST(s1.`updated_at` AS TIMESTAMP)")
	assert.Contains(t, actual, "target._valid_from < CAST(source.`updated_at` AS TIMESTAMP)")
	assert.Contains(t, actual, "target._valid_until = CAST(source.`updated_at` AS TIMESTAMP)")
	assert.Contains(
		t,
		actual,
		"VALUES (source.`customer_id`, source.`customer_name`, source.`updated_at`, CAST(source.`updated_at` AS TIMESTAMP), TIMESTAMP '9999-12-31 00:00:00', TRUE)",
	)
}

func TestMaterializerSCD2Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		asset *pipeline.Asset
		error string
	}{
		{
			name: "primary key is required",
			asset: &pipeline.Asset{
				Name:    "analytics.customers",
				Columns: []pipeline.Column{{Name: "customer_id", Type: "BIGINT"}},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
			},
			error: "requires the `primary_key` field",
		},
		{
			name:  "time incremental key is required",
			asset: scd2Asset(pipeline.MaterializationStrategySCD2ByTime),
			error: "incremental_key is required",
		},
		{
			name: "time incremental key must be declared",
			asset: func() *pipeline.Asset {
				asset := scd2Asset(pipeline.MaterializationStrategySCD2ByTime)
				asset.Materialization.IncrementalKey = "missing_at"
				return asset
			}(),
			error: "must reference a declared column",
		},
		{
			name: "time incremental key type is validated",
			asset: func() *pipeline.Asset {
				asset := scd2Asset(pipeline.MaterializationStrategySCD2ByTime)
				asset.Materialization.IncrementalKey = "customer_name"
				return asset
			}(),
			error: "must be TIMESTAMP or DATE",
		},
		{
			name: "SCD2 columns are reserved case insensitively",
			asset: func() *pipeline.Asset {
				asset := scd2Asset(pipeline.MaterializationStrategySCD2ByColumn)
				asset.Columns = append(asset.Columns, pipeline.Column{Name: "_VALID_UNTIL", Type: "TIMESTAMP"})
				return asset
			}(),
			error: "is reserved for SCD2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewMaterializer(false).Render(test.asset, "SELECT 1")
			require.ErrorContains(t, err, test.error)
		})
	}
}

func scd2Asset(strategy pipeline.MaterializationStrategy) *pipeline.Asset {
	return &pipeline.Asset{
		Name: "catalog.analytics.customers",
		Columns: []pipeline.Column{
			{Name: "customer_id", Type: "BIGINT", PrimaryKey: true},
			{Name: "customer_name", Type: "STRING"},
			{Name: "updated_at", Type: "TIMESTAMP"},
		},
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: strategy,
		},
	}
}
