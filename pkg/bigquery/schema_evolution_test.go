package bigquery

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/schemaevolution"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(v bool) *bool { return &v }

type fakeBigQueryConnGetter struct {
	conn any
}

func (f fakeBigQueryConnGetter) GetConnection(name string) any {
	return f.conn
}

type fakeBigQueryEvolutionDB struct {
	tableMeta       *bigquery.TableMetadata
	tableExists     bool
	querySchema     bigquery.Schema
	queryDryRunFunc func(sql string) (bigquery.Schema, error)
	executedQueries []string
	datasetsCreated int
	limitCheckCalls int
}

func (f *fakeBigQueryEvolutionDB) RunQueryWithoutResult(_ context.Context, q *query.Query) error {
	f.executedQueries = append(f.executedQueries, q.Query)
	return nil
}

func (f *fakeBigQueryEvolutionDB) Ping(_ context.Context) error { return nil }

func (f *fakeBigQueryEvolutionDB) Select(_ context.Context, _ *query.Query) ([][]interface{}, error) {
	return nil, nil
}

func (f *fakeBigQueryEvolutionDB) SelectWithSchema(_ context.Context, _ *query.Query) (*query.QueryResult, error) {
	return nil, nil
}

func (f *fakeBigQueryEvolutionDB) UpdateTableMetadataIfNotExist(_ context.Context, _ *pipeline.Asset) error {
	return nil
}

func (f *fakeBigQueryEvolutionDB) IsPartitioningOrClusteringMismatch(_ context.Context, _ *bigquery.TableMetadata, _ *pipeline.Asset) bool {
	return false
}

func (f *fakeBigQueryEvolutionDB) CreateDataSetIfNotExist(_ *pipeline.Asset, _ context.Context) error {
	f.datasetsCreated++
	return nil
}

func (f *fakeBigQueryEvolutionDB) IsMaterializationTypeMismatch(_ context.Context, _ *bigquery.TableMetadata, _ *pipeline.Asset) bool {
	return false
}

func (f *fakeBigQueryEvolutionDB) DropTableOnMismatch(_ context.Context, _ string, _ *pipeline.Asset) error {
	return nil
}

func (f *fakeBigQueryEvolutionDB) EnsureIcebergTable(_ context.Context, _ string, _ *pipeline.Asset) error {
	return nil
}

func (f *fakeBigQueryEvolutionDB) BuildTableExistsQuery(tableName string) (string, error) {
	return "SELECT 1", nil
}

func (f *fakeBigQueryEvolutionDB) UsesApplicationDefaultCredentials() bool { return false }

func (f *fakeBigQueryEvolutionDB) CheckQueryLimits(_ context.Context, _ *query.Query) error {
	f.limitCheckCalls++
	return nil
}

func (f *fakeBigQueryEvolutionDB) TableMetadataForEvolution(_ context.Context, _ string) (*bigquery.TableMetadata, bool, error) {
	return f.tableMeta, f.tableExists, nil
}

func (f *fakeBigQueryEvolutionDB) QuerySchemaForEvolution(_ context.Context, q *query.Query) (bigquery.Schema, error) {
	if f.queryDryRunFunc != nil {
		return f.queryDryRunFunc(q.Query)
	}
	return f.querySchema, nil
}

func newTestBigQueryOperator(db DB) *BasicOperator {
	return &BasicOperator{
		connection: fakeBigQueryConnGetter{conn: db},
		extractor: &query.WholeFileExtractor{
			Renderer: jinja.NewRendererWithYesterday("test-pipeline", "test-run"),
		},
		materializer: pipeline.HookWrapperMaterializer{
			Mat: NewMaterializer(false),
		},
	}
}

func TestBigQueryDialect_ResolveTypeChangeAndDDL(t *testing.T) {
	t.Parallel()

	d := newBigQueryDialect()

	tests := []struct {
		name       string
		existing   schemaevolution.Column
		incoming   schemaevolution.Column
		wantRes    schemaevolution.TypeResolution
		wantTarget string
	}{
		// Aliases & canonicalization
		{"int alias to int64", schemaevolution.Column{Type: "INT"}, schemaevolution.Column{Type: "INT64"}, schemaevolution.TypeChangeNone, ""},
		{"smallint alias to integer", schemaevolution.Column{Type: "SMALLINT"}, schemaevolution.Column{Type: "INTEGER"}, schemaevolution.TypeChangeNone, ""},
		{"byteint alias to int64", schemaevolution.Column{Type: "BYTEINT"}, schemaevolution.Column{Type: "INT64"}, schemaevolution.TypeChangeNone, ""},
		{"float alias to float64", schemaevolution.Column{Type: "FLOAT"}, schemaevolution.Column{Type: "FLOAT64"}, schemaevolution.TypeChangeNone, ""},
		{"boolean alias to bool", schemaevolution.Column{Type: "BOOLEAN"}, schemaevolution.Column{Type: "BOOL"}, schemaevolution.TypeChangeNone, ""},
		{"decimal alias to numeric", schemaevolution.Column{Type: "DECIMAL(38,9)"}, schemaevolution.Column{Type: "NUMERIC"}, schemaevolution.TypeChangeNone, ""},
		{"bigdecimal alias to bignumeric", schemaevolution.Column{Type: "BIGDECIMAL"}, schemaevolution.Column{Type: "BIGNUMERIC"}, schemaevolution.TypeChangeNone, ""},

		// BigQuery native widenings
		{"int64 to numeric", schemaevolution.Column{Type: "INT64"}, schemaevolution.Column{Type: "NUMERIC"}, schemaevolution.TypeChangeWiden, "NUMERIC"},
		{"int64 to parameterized numeric preserves 19 int digits", schemaevolution.Column{Type: "INT64"}, schemaevolution.Column{Type: "NUMERIC(10,2)"}, schemaevolution.TypeChangeWiden, "NUMERIC(21,2)"},
		{"numeric to bignumeric", schemaevolution.Column{Type: "NUMERIC"}, schemaevolution.Column{Type: "BIGNUMERIC"}, schemaevolution.TypeChangeWiden, "BIGNUMERIC"},
		{"numeric(10,2) to numeric(12,4)", schemaevolution.Column{Type: "NUMERIC(10,2)"}, schemaevolution.Column{Type: "NUMERIC(12,4)"}, schemaevolution.TypeChangeWiden, "NUMERIC(12,4)"},
		{"numeric scale overflow upgrades to bignumeric", schemaevolution.Column{Type: "NUMERIC"}, schemaevolution.Column{Type: "BIGNUMERIC(20,12)"}, schemaevolution.TypeChangeWiden, "BIGNUMERIC(41,12)"},
		{"string(10) to string(50)", schemaevolution.Column{Type: "STRING(10)"}, schemaevolution.Column{Type: "STRING(50)"}, schemaevolution.TypeChangeWiden, "STRING(50)"},
		{"string(50) to unbounded string", schemaevolution.Column{Type: "STRING(50)"}, schemaevolution.Column{Type: "STRING"}, schemaevolution.TypeChangeWiden, "STRING"},
		{"unbounded string absorbs string(20)", schemaevolution.Column{Type: "STRING"}, schemaevolution.Column{Type: "STRING(20)"}, schemaevolution.TypeChangeNone, ""},
		{"bytes(8) to bytes(32)", schemaevolution.Column{Type: "BYTES(8)"}, schemaevolution.Column{Type: "BYTES(32)"}, schemaevolution.TypeChangeWiden, "BYTES(32)"},
		{"array<int64> to array<numeric>", schemaevolution.Column{Type: "ARRAY<INT64>"}, schemaevolution.Column{Type: "ARRAY<NUMERIC>"}, schemaevolution.TypeChangeWiden, "ARRAY<NUMERIC>"},
		{"float64 absorbs int64", schemaevolution.Column{Type: "FLOAT64"}, schemaevolution.Column{Type: "INT64"}, schemaevolution.TypeChangeNone, ""},
		{"float64 absorbs numeric", schemaevolution.Column{Type: "FLOAT64"}, schemaevolution.Column{Type: "NUMERIC"}, schemaevolution.TypeChangeNone, ""},
		{"struct wider field absorbs int alias", schemaevolution.Column{Type: "STRUCT<a INT64, b STRING>"}, schemaevolution.Column{Type: "STRUCT<a INT, b STRING>"}, schemaevolution.TypeChangeNone, ""},

		// BigQuery rejections
		{"string does NOT absorb int64 in BigQuery", schemaevolution.Column{Type: "STRING"}, schemaevolution.Column{Type: "INT64"}, schemaevolution.TypeChangeIncompatible, ""},
		{"date to timestamp rejected in BigQuery", schemaevolution.Column{Type: "DATE"}, schemaevolution.Column{Type: "TIMESTAMP"}, schemaevolution.TypeChangeIncompatible, ""},
		{"timestamp does NOT absorb date in BigQuery", schemaevolution.Column{Type: "TIMESTAMP"}, schemaevolution.Column{Type: "DATE"}, schemaevolution.TypeChangeIncompatible, ""},
		{"datetime to timestamp rejected in BigQuery", schemaevolution.Column{Type: "DATETIME"}, schemaevolution.Column{Type: "TIMESTAMP"}, schemaevolution.TypeChangeIncompatible, ""},
		{"timestamp to datetime rejected in BigQuery", schemaevolution.Column{Type: "TIMESTAMP"}, schemaevolution.Column{Type: "DATETIME"}, schemaevolution.TypeChangeIncompatible, ""},
		{"int64 to float64 rejected as lossy", schemaevolution.Column{Type: "INT64"}, schemaevolution.Column{Type: "FLOAT64"}, schemaevolution.TypeChangeIncompatible, ""},
		{"pk widening rejected", schemaevolution.Column{Type: "INT64", PrimaryKey: true}, schemaevolution.Column{Type: "NUMERIC"}, schemaevolution.TypeChangeIncompatible, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotRes, gotTarget, _ := d.ResolveTypeChange(tt.existing, tt.incoming)
			assert.Equal(t, tt.wantRes, gotRes)
			assert.Equal(t, tt.wantTarget, gotTarget)
		})
	}

	// Verify NOT NULL new column rejection
	_, reason, ok := d.ValidateNewColumn(schemaevolution.Column{Name: "req", Type: "STRING", NotNull: true})
	require.False(t, ok)
	assert.Contains(t, reason, "REQUIRED")

	// Verify batched ALTER TABLE + follow-up SET DEFAULT DDL rendering
	plan := &schemaevolution.Plan{
		Actions: []schemaevolution.Action{
			{Type: schemaevolution.ActionAddColumn, ColumnName: "status", ToType: "STRING", Column: schemaevolution.Column{Name: "status", Type: "STRING", Default: "'active'", Collation: "und:ci"}},
			{Type: schemaevolution.ActionAlterColumnType, ColumnName: "amount", FromType: "INT64", ToType: "NUMERIC"},
			{Type: schemaevolution.ActionAddColumn, ColumnName: "tags", ToType: "ARRAY<STRING>", Column: schemaevolution.Column{Name: "tags", Type: "ARRAY<STRING>"}},
		},
	}
	assert.Equal(t, []string{
		"ALTER TABLE `my-proj`.`analytics`.`orders` ALTER COLUMN `amount` SET DATA TYPE NUMERIC, ADD COLUMN `status` STRING COLLATE 'und:ci', ADD COLUMN `tags` ARRAY<STRING>",
		"ALTER TABLE `my-proj`.`analytics`.`orders` ALTER COLUMN `status` SET DEFAULT ('active')",
	}, d.RenderStatements("my-proj.analytics.orders", plan))
}

func TestBigQuerySchemaEvolution_EndToEndOperator(t *testing.T) {
	t.Parallel()
	p := &pipeline.Pipeline{Name: "bq-pipeline"}

	t.Run("append: batches ALTER TABLE before CheckQueryLimits and injects explicit column list for omitted/reordered columns", func(t *testing.T) {
		t.Parallel()
		db := &fakeBigQueryEvolutionDB{
			tableExists: true,
			tableMeta: &bigquery.TableMetadata{
				Schema: bigquery.Schema{
					{Name: "id", Type: bigquery.IntegerFieldType},
					{Name: "legacy_col", Type: bigquery.StringFieldType},
					{Name: "amount", Type: bigquery.IntegerFieldType},
				},
			},
			querySchema: bigquery.Schema{
				{Name: "amount", Type: bigquery.NumericFieldType},
				{Name: "id", Type: bigquery.IntegerFieldType},
				{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
			},
		}
		op := newTestBigQueryOperator(db)

		var logBuf bytes.Buffer
		ctx := context.WithValue(context.Background(), executor.KeyPrinter, &logBuf)

		asset := &pipeline.Asset{
			Name:            "my-proj.analytics.events",
			Type:            pipeline.AssetTypeBigqueryQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT NUMERIC '12.34' AS amount, 1 AS id, ['a', 'b'] AS tags; -- trailing comment",
			},
		}

		require.NoError(t, op.RunTask(ctx, p, asset))
		require.Len(t, db.executedQueries, 2)
		assert.Equal(t, 1, db.datasetsCreated)
		assert.Equal(t, 1, db.limitCheckCalls)

		assert.Contains(t, db.executedQueries[0], "ALTER TABLE `my-proj`.`analytics`.`events` ALTER COLUMN `amount` SET DATA TYPE NUMERIC, ADD COLUMN `tags` ARRAY<STRING>;")
		assert.Contains(t, db.executedQueries[1], "INSERT INTO my-proj.analytics.events (`amount`, `id`, `tags`) SELECT NUMERIC '12.34' AS amount, 1 AS id, ['a', 'b'] AS tags")
		assert.Contains(t, logBuf.String(), `widening column "amount" (INT64 -> NUMERIC)`)
		assert.Contains(t, logBuf.String(), `adding column "tags" (ARRAY<STRING>)`)
	})

	t.Run("delete+insert: runs pre-hooks once before inspection and rewrites SELECT * FROM temp table to explicit columns", func(t *testing.T) {
		t.Parallel()
		db := &fakeBigQueryEvolutionDB{
			tableExists: true,
			tableMeta: &bigquery.TableMetadata{
				Schema: bigquery.Schema{
					{Name: "dt", Type: bigquery.DateFieldType},
					{Name: "legacy", Type: bigquery.StringFieldType},
				},
			},
			querySchema: bigquery.Schema{
				{Name: "dt", Type: bigquery.DateFieldType},
				{Name: "metric", Type: bigquery.FloatFieldType},
			},
		}
		op := newTestBigQueryOperator(db)

		asset := &pipeline.Asset{
			Name:            "analytics.daily",
			Type:            pipeline.AssetTypeBigqueryQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:           pipeline.MaterializationTypeTable,
				Strategy:       pipeline.MaterializationStrategyDeleteInsert,
				IncrementalKey: "dt",
			},
			Hooks: pipeline.Hooks{
				Pre:  []pipeline.Hook{{Query: "INSERT INTO analytics.audit VALUES ('pre')"}},
				Post: []pipeline.Hook{{Query: "INSERT INTO analytics.audit VALUES ('post')"}},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT DATE '2026-01-01' AS dt, 3.5 AS metric",
			},
		}

		require.NoError(t, op.RunTask(context.Background(), p, asset))
		// 1: pre-hook, 2: ALTER TABLE, 3: main transaction + post-hook (without duplicate pre-hook!)
		require.Len(t, db.executedQueries, 3)
		assert.Contains(t, db.executedQueries[0], "INSERT INTO analytics.audit VALUES ('pre')")
		assert.Contains(t, db.executedQueries[1], "ALTER TABLE `analytics`.`daily` ADD COLUMN `metric` FLOAT64;")
		assert.NotContains(t, db.executedQueries[2], "VALUES ('pre')")
		assert.Contains(t, db.executedQueries[2], "INSERT INTO analytics.daily (`dt`, `metric`) SELECT `dt`, `metric` FROM __bruin_tmp_")
		assert.Contains(t, db.executedQueries[2], "INSERT INTO analytics.audit VALUES ('post')")
	})

	t.Run("scd2_by_column: evolves declared columns and switches comparison to IS DISTINCT FROM", func(t *testing.T) {
		t.Parallel()
		db := &fakeBigQueryEvolutionDB{
			tableExists: true,
			tableMeta: &bigquery.TableMetadata{
				Schema: bigquery.Schema{
					{Name: "id", Type: bigquery.IntegerFieldType},
					{Name: "label", Type: bigquery.StringFieldType, MaxLength: 10},
					{Name: "_valid_from", Type: bigquery.TimestampFieldType},
					{Name: "_valid_until", Type: bigquery.TimestampFieldType},
					{Name: "_is_current", Type: bigquery.BooleanFieldType},
				},
			},
			querySchema: bigquery.Schema{
				{Name: "id", Type: bigquery.IntegerFieldType},
				{Name: "label", Type: bigquery.StringFieldType, MaxLength: 50},
				{Name: "email", Type: bigquery.StringFieldType},
				{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
				{Name: "undeclared_extra", Type: bigquery.StringFieldType},
			},
		}
		op := newTestBigQueryOperator(db)

		asset := &pipeline.Asset{
			Name:            "analytics.dim_users",
			Type:            pipeline.AssetTypeBigqueryQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategySCD2ByColumn,
			},
			Columns: []pipeline.Column{
				{Name: "id", Type: "INT64", PrimaryKey: true},
				{Name: "label", Type: "STRING"},
				{Name: "email", Type: "STRING"},
				{Name: "tags", Type: "ARRAY<STRING>"},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 1 AS id, 'longer_label' AS label, 'a@b.com' AS email, ['vip'] AS tags, 'skip' AS undeclared_extra",
			},
		}

		require.NoError(t, op.RunTask(context.Background(), p, asset))
		require.Len(t, db.executedQueries, 2)
		assert.Contains(t, db.executedQueries[0], "ALTER TABLE `analytics`.`dim_users` ALTER COLUMN `label` SET DATA TYPE STRING(50), ADD COLUMN `email` STRING, ADD COLUMN `tags` ARRAY<STRING>;")
		assert.NotContains(t, db.executedQueries[0], "undeclared_extra")
		assert.Contains(t, db.executedQueries[1], "target.email IS DISTINCT FROM source.email")
		assert.Contains(t, db.executedQueries[1], "FORMAT('%T', target.tags) != FORMAT('%T', source.tags)")
	})

	t.Run("untyped NULL and empty array rejected on new columns, accepted on compatible existing columns", func(t *testing.T) {
		t.Parallel()
		db := &fakeBigQueryEvolutionDB{
			tableExists: true,
			tableMeta: &bigquery.TableMetadata{
				Schema: bigquery.Schema{
					{Name: "id", Type: bigquery.IntegerFieldType},
					{Name: "note", Type: bigquery.StringFieldType},
					{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
				},
			},
			querySchema: bigquery.Schema{
				{Name: "id", Type: bigquery.IntegerFieldType},
				{Name: "note", Type: bigquery.IntegerFieldType},
				{Name: "tags", Type: bigquery.IntegerFieldType, Repeated: true},
			},
		}
		op := newTestBigQueryOperator(db)

		asset := &pipeline.Asset{
			Name:            "analytics.null_test",
			Type:            pipeline.AssetTypeBigqueryQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 1 AS id, NULL AS note, [] AS tags",
			},
		}
		require.NoError(t, op.RunTask(context.Background(), p, asset))
		require.Len(t, db.executedQueries, 1) // No ALTER TABLE needed!

		// Brand-new untyped NULL or [] column is rejected before executing any SQL.
		db.executedQueries = nil
		db.querySchema = append(
			db.querySchema,
			&bigquery.FieldSchema{Name: "bad_null", Type: bigquery.IntegerFieldType},
			&bigquery.FieldSchema{Name: "bad_arr", Type: bigquery.IntegerFieldType, Repeated: true},
		)
		asset.ExecutableFile.Content = "SELECT 1 AS id, NULL AS note, [] AS tags, NULL AS bad_null, [] AS bad_arr"
		err := op.RunTask(context.Background(), p, asset)
		var incompatErr *schemaevolution.IncompatibleSchemaError
		require.ErrorAs(t, err, &incompatErr)
		assert.Len(t, incompatErr.Changes, 2)
		assert.Empty(t, db.executedQueries)
	})

	t.Run("non-existent 404 table skips schema evolution and disabled config leaves SQL unchanged", func(t *testing.T) {
		t.Parallel()
		db := &fakeBigQueryEvolutionDB{tableExists: false}
		op := newTestBigQueryOperator(db)

		asset := &pipeline.Asset{
			Name:            "analytics.missing",
			Type:            pipeline.AssetTypeBigqueryQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{Content: "SELECT 1 AS id"},
		}
		require.NoError(t, op.RunTask(context.Background(), p, asset))
		require.Len(t, db.executedQueries, 1)
		assert.True(t, strings.HasSuffix(strings.TrimSpace(db.executedQueries[0]), "INSERT INTO analytics.missing SELECT 1 AS id"))
	})
}
