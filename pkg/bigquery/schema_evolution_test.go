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
	querySessions   []string
	datasetsCreated int
	limitCheckCalls int
	abortedSessions []string
}

func (f *fakeBigQueryEvolutionDB) RunQueryWithoutResult(ctx context.Context, q *query.Query) error {
	f.executedQueries = append(f.executedQueries, q.Query)
	f.querySessions = append(f.querySessions, sessionIDFromContext(ctx))
	return nil
}

func (f *fakeBigQueryEvolutionDB) StartSession(ctx context.Context, q *query.Query) (string, error) {
	f.executedQueries = append(f.executedQueries, q.Query)
	f.querySessions = append(f.querySessions, "new:"+sessionIDFromContext(ctx))
	return "session-1", nil
}

func (f *fakeBigQueryEvolutionDB) AbortSession(_ context.Context, sessionID string) {
	f.abortedSessions = append(f.abortedSessions, sessionID)
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

	// Verify DDL rendering: one ADD COLUMN statement, one SET DATA TYPE statement per column,
	// then the follow-up SET DEFAULT statements.
	plan := &schemaevolution.Plan{
		Actions: []schemaevolution.Action{
			{Type: schemaevolution.ActionAddColumn, ColumnName: "status", ToType: "STRING", Column: schemaevolution.Column{Name: "status", Type: "STRING", Default: "'active'", Collation: "und:ci"}},
			{Type: schemaevolution.ActionAlterColumnType, ColumnName: "amount", FromType: "INT64", ToType: "NUMERIC"},
			{Type: schemaevolution.ActionAddColumn, ColumnName: "tags", ToType: "ARRAY<STRING>", Column: schemaevolution.Column{Name: "tags", Type: "ARRAY<STRING>"}},
			{Type: schemaevolution.ActionAlterColumnType, ColumnName: "code", FromType: "STRING(10)", ToType: "STRING(20)"},
		},
	}
	assert.Equal(t, []string{
		"ALTER TABLE `my-proj`.`analytics`.`orders` ADD COLUMN `status` STRING COLLATE 'und:ci', ADD COLUMN `tags` ARRAY<STRING>",
		"ALTER TABLE `my-proj`.`analytics`.`orders` ALTER COLUMN `amount` SET DATA TYPE NUMERIC",
		"ALTER TABLE `my-proj`.`analytics`.`orders` ALTER COLUMN `code` SET DATA TYPE STRING(20)",
		"ALTER TABLE `my-proj`.`analytics`.`orders` ALTER COLUMN `status` SET DEFAULT ('active')",
	}, d.RenderStatements("my-proj.analytics.orders", plan))

	// Only widenings: no ADD COLUMN statement.
	assert.Equal(t, []string{
		"ALTER TABLE `orders` ALTER COLUMN `amount` SET DATA TYPE NUMERIC",
	}, d.RenderStatements("orders", &schemaevolution.Plan{Actions: []schemaevolution.Action{
		{Type: schemaevolution.ActionAlterColumnType, ColumnName: "amount", FromType: "INT64", ToType: "NUMERIC"},
	}}))
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

		assert.Contains(t, db.executedQueries[0], "ALTER TABLE `my-proj`.`analytics`.`events` ADD COLUMN `tags` ARRAY<STRING>;\nALTER TABLE `my-proj`.`analytics`.`events` ALTER COLUMN `amount` SET DATA TYPE NUMERIC;")
		assert.Contains(t, db.executedQueries[1], "INSERT INTO my-proj.analytics.events (`amount`, `id`, `tags`) SELECT NUMERIC '12.34' AS amount, 1 AS id, ['a', 'b'] AS tags")
		assert.Contains(t, logBuf.String(), `widening column "amount" (INT64 -> NUMERIC)`)
		assert.Contains(t, logBuf.String(), `adding column "tags" (ARRAY<STRING>)`)
	})

	t.Run("delete+insert: runs pre-hooks once in a session before inspection and rewrites SELECT * FROM temp table to explicit columns", func(t *testing.T) {
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
				Pre: []pipeline.Hook{
					{Query: "DECLARE cutoff DATE DEFAULT CURRENT_DATE()"},
					{Query: "INSERT INTO analytics.audit VALUES ('pre')"},
				},
				Post: []pipeline.Hook{
					{Query: "INSERT INTO analytics.audit VALUES ('post')"},
					{Query: "INSERT INTO analytics.daily SELECT DATE '2020-01-01', 'seed', 0.0"},
				},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT DATE '2026-01-01' AS dt, 3.5 AS metric",
			},
		}

		require.NoError(t, op.RunTask(context.Background(), p, asset))
		// 1: pre-hooks starting the session, 2: ALTER TABLE, 3: main transaction + post-hooks (without duplicate pre-hooks!)
		require.Len(t, db.executedQueries, 3)
		assert.Equal(t, []string{"new:", "session-1", "session-1"}, db.querySessions)
		assert.Equal(t, []string{"session-1"}, db.abortedSessions)
		assert.Contains(t, db.executedQueries[0], "DECLARE cutoff DATE DEFAULT CURRENT_DATE();\nINSERT INTO analytics.audit VALUES ('pre');")
		assert.Contains(t, db.executedQueries[1], "ALTER TABLE `analytics`.`daily` ADD COLUMN `metric` FLOAT64;")
		assert.NotContains(t, db.executedQueries[2], "VALUES ('pre')")
		assert.NotContains(t, db.executedQueries[2], "DECLARE cutoff")
		assert.Contains(t, db.executedQueries[2], "INSERT INTO analytics.daily (`dt`, `metric`) SELECT `dt`, `metric` FROM __bruin_tmp_")
		assert.Contains(t, db.executedQueries[2], "INSERT INTO analytics.audit VALUES ('post')")
		// Post-hooks keep their own column order instead of getting the main query's column list.
		assert.Contains(t, db.executedQueries[2], "INSERT INTO analytics.daily SELECT DATE '2020-01-01', 'seed', 0.0;")
	})

	t.Run("no pre-hooks runs without a session", func(t *testing.T) {
		t.Parallel()
		db := &fakeBigQueryEvolutionDB{
			tableExists: true,
			tableMeta: &bigquery.TableMetadata{
				Schema: bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
			},
			querySchema: bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
		}
		op := newTestBigQueryOperator(db)

		asset := &pipeline.Asset{
			Name:            "analytics.ids",
			Type:            pipeline.AssetTypeBigqueryQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			Hooks: pipeline.Hooks{
				Post: []pipeline.Hook{{Query: "INSERT INTO analytics.audit VALUES ('post')"}},
			},
			ExecutableFile: pipeline.ExecutableFile{Content: "SELECT 1 AS id"},
		}

		require.NoError(t, op.RunTask(context.Background(), p, asset))
		require.Len(t, db.executedQueries, 1)
		assert.Equal(t, []string{""}, db.querySessions)
		assert.Empty(t, db.abortedSessions)
		assert.Contains(t, db.executedQueries[0], "INSERT INTO analytics.ids (`id`) SELECT 1 AS id;\nINSERT INTO analytics.audit VALUES ('post');")
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
				{Name: "profile", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
					{Name: "tier", Type: bigquery.StringFieldType},
				}},
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
				{Name: "profile", Type: "STRUCT<tier STRING>"},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 1 AS id, 'longer_label' AS label, 'a@b.com' AS email, ['vip'] AS tags, STRUCT('gold' AS tier) AS profile, 'skip' AS undeclared_extra",
			},
		}

		require.NoError(t, op.RunTask(context.Background(), p, asset))
		require.Len(t, db.executedQueries, 2)
		assert.Contains(t, db.executedQueries[0], "ALTER TABLE `analytics`.`dim_users` ADD COLUMN `email` STRING, ADD COLUMN `tags` ARRAY<STRING>, ADD COLUMN `profile` STRUCT<`tier` STRING>;\nALTER TABLE `analytics`.`dim_users` ALTER COLUMN `label` SET DATA TYPE STRING(50);")
		assert.NotContains(t, db.executedQueries[0], "undeclared_extra")
		assert.Contains(t, db.executedQueries[1], "target.email IS DISTINCT FROM source.email")
		assert.Contains(t, db.executedQueries[1], "FORMAT('%T', target.tags) != FORMAT('%T', source.tags)")
		// BigQuery rejects IS DISTINCT FROM on STRUCT values.
		assert.Contains(t, db.executedQueries[1], "FORMAT('%T', target.profile) != FORMAT('%T', source.profile)")
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

func TestBigQueryEvolutionEngine_QueryColumnsBehindPreHooks(t *testing.T) {
	t.Parallel()

	var inspected []string
	db := &fakeBigQueryEvolutionDB{
		queryDryRunFunc: func(sql string) (bigquery.Schema, error) {
			inspected = append(inspected, sql)
			if strings.Contains(sql, "FROM stg") {
				return nil, nil
			}
			return bigquery.Schema{{Name: "c", Type: bigquery.DateFieldType}}, nil
		},
	}
	engine := &bigQueryEvolutionEngine{
		BigQueryDialect: newBigQueryDialect(),
		inspector:       db,
		preHookSQL:      "DECLARE cutoff DATE DEFAULT CURRENT_DATE();",
	}

	cols, err := engine.QueryColumns(t.Context(), "SELECT cutoff AS c")
	require.NoError(t, err)
	require.Len(t, cols, 1)
	assert.Equal(t, "DECLARE cutoff DATE DEFAULT CURRENT_DATE();\nSELECT cutoff AS c;", inspected[0])

	_, err = engine.QueryColumns(t.Context(), "SELECT id FROM stg")
	require.ErrorIs(t, err, errQuerySchemaUnavailable)
}

func TestBigQuerySchemaEvolution_QueryTypesWithoutParametersKeepExistingParameters(t *testing.T) {
	t.Parallel()

	existing := bigquery.Schema{
		{Name: "code", Type: bigquery.StringFieldType, MaxLength: 10},
		{Name: "amount", Type: bigquery.NumericFieldType, Precision: 10, Scale: 2},
		{Name: "big", Type: bigquery.BigNumericFieldType, Precision: 40, Scale: 2},
		{Name: "raw", Type: bigquery.BytesFieldType, MaxLength: 16},
		{Name: "codes", Type: bigquery.StringFieldType, MaxLength: 5, Repeated: true},
		{Name: "info", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
			{Name: "code", Type: bigquery.StringFieldType, MaxLength: 10},
			{Name: "price", Type: bigquery.NumericFieldType, Precision: 10, Scale: 2},
		}},
	}
	// A dry run reports every column above without its type parameters.
	query := bigquery.Schema{
		{Name: "code", Type: bigquery.StringFieldType},
		{Name: "amount", Type: bigquery.NumericFieldType},
		{Name: "big", Type: bigquery.BigNumericFieldType},
		{Name: "raw", Type: bigquery.BytesFieldType},
		{Name: "codes", Type: bigquery.StringFieldType, Repeated: true},
		{Name: "info", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
			{Name: "code", Type: bigquery.StringFieldType},
			{Name: "price", Type: bigquery.NumericFieldType},
		}},
	}

	engine := &bigQueryEvolutionEngine{
		BigQueryDialect: newBigQueryDialect(),
		inspector: &fakeBigQueryEvolutionDB{
			tableExists: true,
			tableMeta:   &bigquery.TableMetadata{Schema: existing},
			querySchema: query,
		},
	}

	for _, strategy := range []pipeline.MaterializationStrategy{pipeline.MaterializationStrategyAppend, pipeline.MaterializationStrategyMerge} {
		asset := &pipeline.Asset{
			Name:            "analytics.params",
			Type:            pipeline.AssetTypeBigqueryQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: strategy},
		}
		if strategy == pipeline.MaterializationStrategyMerge {
			for _, f := range query {
				asset.Columns = append(asset.Columns, pipeline.Column{Name: f.Name})
			}
		}
		plan, err := schemaevolution.BuildPlan(t.Context(), engine, asset, asset.Name, "SELECT 1", false)
		require.NoError(t, err, strategy)
		assert.Empty(t, plan.Actions, strategy)
	}

	// A different base type in the query is still a real widening.
	engine.inspector = &fakeBigQueryEvolutionDB{
		tableExists: true,
		tableMeta:   &bigquery.TableMetadata{Schema: bigquery.Schema{{Name: "amount", Type: bigquery.NumericFieldType, Precision: 10, Scale: 2}}},
		querySchema: bigquery.Schema{{Name: "amount", Type: bigquery.BigNumericFieldType}},
	}
	appendAsset := &pipeline.Asset{
		Name:            "analytics.params",
		Type:            pipeline.AssetTypeBigqueryQuery,
		SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyAppend},
	}
	plan, err := schemaevolution.BuildPlan(t.Context(), engine, appendAsset, appendAsset.Name, "SELECT 1", false)
	require.NoError(t, err)
	require.Len(t, plan.Actions, 1)
	assert.Equal(t, "BIGNUMERIC", plan.Actions[0].ToType)
}

func TestBigQuerySchemaEvolution_DDLDeclaredTypesWidenParameters(t *testing.T) {
	t.Parallel()

	engine := &bigQueryEvolutionEngine{
		BigQueryDialect: newBigQueryDialect(),
		inspector: &fakeBigQueryEvolutionDB{
			tableExists: true,
			tableMeta: &bigquery.TableMetadata{Schema: bigquery.Schema{
				{Name: "code", Type: bigquery.StringFieldType, MaxLength: 10},
				{Name: "note", Type: bigquery.StringFieldType, MaxLength: 10},
			}},
		},
	}
	asset := &pipeline.Asset{
		Name:            "analytics.declared",
		Type:            pipeline.AssetTypeBigqueryQuery,
		SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyDDL},
		Columns: []pipeline.Column{
			{Name: "code", Type: "STRING(20)"},
			{Name: "note", Type: "STRING"},
		},
	}
	plan, err := schemaevolution.BuildPlan(t.Context(), engine, asset, asset.Name, "", false)
	require.NoError(t, err)
	require.Len(t, plan.Actions, 2)
	assert.Equal(t, "STRING(20)", plan.Actions[0].ToType)
	assert.Equal(t, "STRING", plan.Actions[1].ToType)
}

func TestBigQuerySchemaEvolution_RenderErrorStopsBeforePreHooksAndDDL(t *testing.T) {
	t.Parallel()

	db := &fakeBigQueryEvolutionDB{
		tableExists: true,
		tableMeta:   &bigquery.TableMetadata{Schema: bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}}},
		querySchema: bigquery.Schema{
			{Name: "id", Type: bigquery.IntegerFieldType},
			{Name: "extra", Type: bigquery.StringFieldType},
		},
	}
	op := newTestBigQueryOperator(db)

	asset := &pipeline.Asset{
		Name:            "analytics.no_key",
		Type:            pipeline.AssetTypeBigqueryQuery,
		SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: pipeline.MaterializationStrategyDeleteInsert,
		},
		Hooks: pipeline.Hooks{
			Pre: []pipeline.Hook{{Query: "INSERT INTO analytics.audit VALUES ('pre')"}},
		},
		ExecutableFile: pipeline.ExecutableFile{Content: "SELECT 1 AS id, 'x' AS extra"},
	}

	err := op.RunTask(context.Background(), &pipeline.Pipeline{Name: "bq-pipeline"}, asset)
	require.ErrorContains(t, err, "incremental_key")
	assert.Empty(t, db.executedQueries)
	assert.Zero(t, db.datasetsCreated)
}
