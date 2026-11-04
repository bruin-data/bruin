package fabric

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestViewMaterializer(t *testing.T) {
	t.Parallel()
	asset := &pipeline.Asset{Name: "dbo.MyView"}
	result, err := viewMaterializer(asset, "SELECT 1;")
	require.NoError(t, err)
	assert.Contains(t, result, "CREATE OR ALTER VIEW [dbo].[MyView] AS")
}

func TestBuildCreateReplaceQuery(t *testing.T) {
	t.Parallel()

	t.Run("simple query", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{Name: "dbo.Table"}
		result, err := buildCreateReplaceQuery(asset, "SELECT 1;")
		require.NoError(t, err)
		expected := "DROP TABLE IF EXISTS [dbo].[Table__bruin_tmp];\n" +
			"DROP TABLE IF EXISTS [dbo].[Table__bruin_backup];\n" +
			"CREATE TABLE [dbo].[Table__bruin_tmp] AS\n" +
			"SELECT 1;\n" +
			"IF OBJECT_ID('dbo.Table', 'U') IS NOT NULL BEGIN EXEC sp_rename 'dbo.Table', 'Table__bruin_backup' END;\n" +
			"EXEC sp_rename 'dbo.Table__bruin_tmp', 'Table';\n" +
			"DROP TABLE IF EXISTS [dbo].[Table__bruin_backup];"
		assert.Equal(t, expected, result)
	})

	t.Run("query with CTE", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{Name: "dbo.Table"}
		result, err := buildCreateReplaceQuery(asset, "WITH monthly AS (SELECT id, amount FROM sales) SELECT * FROM monthly;")
		require.NoError(t, err)
		expected := "DROP TABLE IF EXISTS [dbo].[Table__bruin_tmp];\n" +
			"DROP TABLE IF EXISTS [dbo].[Table__bruin_backup];\n" +
			"CREATE TABLE [dbo].[Table__bruin_tmp] AS\n" +
			"WITH monthly AS (SELECT id, amount FROM sales) SELECT * FROM monthly;\n" +
			"IF OBJECT_ID('dbo.Table', 'U') IS NOT NULL BEGIN EXEC sp_rename 'dbo.Table', 'Table__bruin_backup' END;\n" +
			"EXEC sp_rename 'dbo.Table__bruin_tmp', 'Table';\n" +
			"DROP TABLE IF EXISTS [dbo].[Table__bruin_backup];"
		assert.Equal(t, expected, result)
	})
}

func TestBuildDeleteInsertQuery(t *testing.T) {
	t.Parallel()

	t.Run("error without columns", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{Name: "dbo.Table"}
		_, err := buildDeleteInsertQuery(asset, "SELECT 1")
		require.Error(t, err)
	})

	t.Run("simple query", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name:    "dbo.Table",
			Columns: []pipeline.Column{{Name: "id", PrimaryKey: true}},
		}
		result, err := buildDeleteInsertQuery(asset, "SELECT 1")
		require.NoError(t, err)
		expected := "DROP TABLE IF EXISTS [dbo].[Table__bruin_tmp];\n" +
			"CREATE TABLE [dbo].[Table__bruin_tmp] AS\n" +
			"SELECT 1;\n" +
			"DELETE FROM [dbo].[Table] WHERE EXISTS (\n" +
			"  SELECT 1 FROM [dbo].[Table__bruin_tmp] WHERE [dbo].[Table].[id] = [dbo].[Table__bruin_tmp].[id]\n" +
			");\n" +
			"INSERT INTO [dbo].[Table] SELECT * FROM [dbo].[Table__bruin_tmp];\n" +
			"DROP TABLE IF EXISTS [dbo].[Table__bruin_tmp];"
		assert.Equal(t, expected, result)
	})

	t.Run("query with CTE", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name:    "dbo.Table",
			Columns: []pipeline.Column{{Name: "id", PrimaryKey: true}},
		}
		result, err := buildDeleteInsertQuery(asset, "WITH cte AS (SELECT id, val FROM src) SELECT * FROM cte")
		require.NoError(t, err)
		expected := "DROP TABLE IF EXISTS [dbo].[Table__bruin_tmp];\n" +
			"CREATE TABLE [dbo].[Table__bruin_tmp] AS\n" +
			"WITH cte AS (SELECT id, val FROM src) SELECT * FROM cte;\n" +
			"DELETE FROM [dbo].[Table] WHERE EXISTS (\n" +
			"  SELECT 1 FROM [dbo].[Table__bruin_tmp] WHERE [dbo].[Table].[id] = [dbo].[Table__bruin_tmp].[id]\n" +
			");\n" +
			"INSERT INTO [dbo].[Table] SELECT * FROM [dbo].[Table__bruin_tmp];\n" +
			"DROP TABLE IF EXISTS [dbo].[Table__bruin_tmp];"
		assert.Equal(t, expected, result)
	})
}

func TestBuildMergeQueryWithoutOptInUsesDeleteInsert(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "dbo.Table",
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: pipeline.MaterializationStrategyMerge,
		},
		Columns: []pipeline.Column{
			{Name: "id", PrimaryKey: true},
			{Name: "val", UpdateOnMerge: true},
		},
	}

	for _, params := range []pipeline.ParameterMap{nil, {NativeMergeParameter: "false"}} {
		asset.Parameters = params
		merge, err := buildMergeQuery(asset, "SELECT 1")
		require.NoError(t, err)
		deleteInsert, err := buildDeleteInsertQuery(asset, "SELECT 1")
		require.NoError(t, err)
		assert.Equal(t, deleteInsert, merge)
	}
}

func TestBuildMergeQueryWithoutOptInRejectsIncrementalPredicate(t *testing.T) {
	t.Parallel()
	asset := &pipeline.Asset{
		Name: "dbo.Table",
		Materialization: pipeline.Materialization{
			Type:                 pipeline.MaterializationTypeTable,
			Strategy:             pipeline.MaterializationStrategyMerge,
			IncrementalPredicate: "target.event_date >= '2026-07-01'",
		},
		Columns: []pipeline.Column{{Name: "id", PrimaryKey: true}},
	}
	_, err := buildMergeQuery(asset, "SELECT 1")
	require.ErrorContains(t, err, "incremental_predicate on Fabric merge requires `parameters.fabric_native_merge: true`")
}

func nativeMergeAsset(columns []pipeline.Column) *pipeline.Asset {
	return &pipeline.Asset{
		Name: "dbo.Table",
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: pipeline.MaterializationStrategyMerge,
		},
		Parameters: pipeline.ParameterMap{NativeMergeParameter: true},
		Columns:    columns,
	}
}

const (
	nativeMergeStaging = "CREATE TABLE [dbo].[Table__bruin_tmp_abcefghi] AS\n"
	nativeMergeGuard   = "IF EXISTS (SELECT 1 FROM [dbo].[Table__bruin_tmp_abcefghi] GROUP BY [id] HAVING COUNT(*) > 1)\n" +
		"BEGIN\n" +
		"DROP TABLE IF EXISTS [dbo].[Table__bruin_tmp_abcefghi];\n" +
		"THROW 50000, N'Fabric merge into dbo.Table failed: the query returned more than one row for the same primary key (id). Deduplicate the query so that each key appears once.', 1;\n" +
		"END;\n"
	nativeMergeCleanup = "DROP TABLE IF EXISTS [dbo].[Table__bruin_tmp_abcefghi];"
)

func TestBuildNativeMergeQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		columns   []pipeline.Column
		predicate string
		query     string
		want      string
	}{
		{
			name: "plain merge only inserts new keys",
			columns: []pipeline.Column{
				{Name: "id", PrimaryKey: true},
				{Name: "name"},
			},
			query: "SELECT id, name FROM src",
			want: nativeMergeStaging +
				"SELECT id, name FROM src;\n" +
				nativeMergeGuard +
				"MERGE INTO [dbo].[Table] target\n" +
				"USING [dbo].[Table__bruin_tmp_abcefghi] source\n" +
				"ON target.[id] = source.[id]\n" +
				"WHEN NOT MATCHED BY TARGET THEN INSERT ([id], [name]) VALUES (source.[id], source.[name]);\n" +
				nativeMergeCleanup,
		},
		{
			name: "update_on_merge updates only the flagged columns",
			columns: []pipeline.Column{
				{Name: "id", PrimaryKey: true, UpdateOnMerge: true},
				{Name: "name", UpdateOnMerge: true},
				{Name: "created_at"},
				{Name: "status", UpdateOnMerge: true},
			},
			query: "SELECT id, name, created_at, status FROM src;",
			want: nativeMergeStaging +
				"SELECT id, name, created_at, status FROM src;\n" +
				nativeMergeGuard +
				"MERGE INTO [dbo].[Table] target\n" +
				"USING [dbo].[Table__bruin_tmp_abcefghi] source\n" +
				"ON target.[id] = source.[id]\n" +
				"WHEN MATCHED THEN UPDATE SET target.[name] = source.[name], target.[status] = source.[status]\n" +
				"WHEN NOT MATCHED BY TARGET THEN INSERT ([id], [name], [created_at], [status]) VALUES (source.[id], source.[name], source.[created_at], source.[status]);\n" +
				nativeMergeCleanup,
		},
		{
			name: "merge_sql CASE expressions update if newer",
			columns: []pipeline.Column{
				{Name: "id", PrimaryKey: true},
				{Name: "amount", MergeSQL: "CASE WHEN source.updated_at > target.updated_at THEN source.amount ELSE target.amount END"},
				{Name: "updated_at", MergeSQL: "CASE WHEN source.updated_at > target.updated_at THEN source.updated_at ELSE target.updated_at END"},
				{Name: "created_at"},
			},
			query: "SELECT id, amount, updated_at, created_at FROM src",
			want: nativeMergeStaging +
				"SELECT id, amount, updated_at, created_at FROM src;\n" +
				nativeMergeGuard +
				"MERGE INTO [dbo].[Table] target\n" +
				"USING [dbo].[Table__bruin_tmp_abcefghi] source\n" +
				"ON target.[id] = source.[id]\n" +
				"WHEN MATCHED THEN UPDATE SET " +
				"target.[amount] = CASE WHEN source.updated_at > target.updated_at THEN source.amount ELSE target.amount END, " +
				"target.[updated_at] = CASE WHEN source.updated_at > target.updated_at THEN source.updated_at ELSE target.updated_at END\n" +
				"WHEN NOT MATCHED BY TARGET THEN INSERT ([id], [amount], [updated_at], [created_at]) VALUES (source.[id], source.[amount], source.[updated_at], source.[created_at]);\n" +
				nativeMergeCleanup,
		},
		{
			name: "incremental_predicate is added to WHEN MATCHED",
			columns: []pipeline.Column{
				{Name: "id", PrimaryKey: true},
				{Name: "val", UpdateOnMerge: true},
			},
			predicate: "  source.updated_at > target.updated_at  ",
			query:     "SELECT id, val, updated_at FROM src",
			want: nativeMergeStaging +
				"SELECT id, val, updated_at FROM src;\n" +
				nativeMergeGuard +
				"MERGE INTO [dbo].[Table] target\n" +
				"USING [dbo].[Table__bruin_tmp_abcefghi] source\n" +
				"ON target.[id] = source.[id]\n" +
				"WHEN MATCHED AND (source.updated_at > target.updated_at) THEN UPDATE SET target.[val] = source.[val]\n" +
				"WHEN NOT MATCHED BY TARGET THEN INSERT ([id], [val]) VALUES (source.[id], source.[val]);\n" +
				nativeMergeCleanup,
		},
		{
			name: "CTE-bodied query is staged instead of inlined in USING",
			columns: []pipeline.Column{
				{Name: "id", PrimaryKey: true},
				{Name: "val", UpdateOnMerge: true},
			},
			query: "WITH latest AS (SELECT id, val FROM src) SELECT id, val FROM latest;\n",
			want: nativeMergeStaging +
				"WITH latest AS (SELECT id, val FROM src) SELECT id, val FROM latest;\n" +
				nativeMergeGuard +
				"MERGE INTO [dbo].[Table] target\n" +
				"USING [dbo].[Table__bruin_tmp_abcefghi] source\n" +
				"ON target.[id] = source.[id]\n" +
				"WHEN MATCHED THEN UPDATE SET target.[val] = source.[val]\n" +
				"WHEN NOT MATCHED BY TARGET THEN INSERT ([id], [val]) VALUES (source.[id], source.[val]);\n" +
				nativeMergeCleanup,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			asset := nativeMergeAsset(tt.columns)
			asset.Materialization.IncrementalPredicate = tt.predicate

			result, err := NewMaterializer(false).Render(asset, tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
			assert.NotContains(t, result, "OUTPUT")
			assert.NotContains(t, result, "TOP")
		})
	}
}

func TestBuildNativeMergeQueryCompositeKeyAndEscaping(t *testing.T) {
	t.Parallel()

	asset := nativeMergeAsset([]pipeline.Column{
		{Name: "tenant", PrimaryKey: true},
		{Name: "id", PrimaryKey: true},
		{Name: "val", UpdateOnMerge: true},
	})
	asset.Name = "dbo.it's_100%"

	result, err := buildMergeQuery(asset, "SELECT 1")
	require.NoError(t, err)
	assert.Contains(t, result, "GROUP BY [tenant], [id] HAVING COUNT(*) > 1")
	assert.Contains(t, result, "ON target.[tenant] = source.[tenant] AND target.[id] = source.[id]")
	assert.Contains(t, result, "THROW 50000, N'Fabric merge into dbo.it''s_100%% failed: the query returned more than one row for the same primary key (tenant, id).")
}

func TestBuildNativeMergeQueryErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		asset     *pipeline.Asset
		wantError string
	}{
		{
			name:      "no columns",
			asset:     nativeMergeAsset(nil),
			wantError: "requires the `columns` field to be set",
		},
		{
			name:      "no primary key",
			asset:     nativeMergeAsset([]pipeline.Column{{Name: "id"}}),
			wantError: "requires the `primary_key` field to be set on at least one column",
		},
		{
			name: "incremental_predicate without an updated column",
			asset: func() *pipeline.Asset {
				a := nativeMergeAsset([]pipeline.Column{{Name: "id", PrimaryKey: true}, {Name: "val"}})
				a.Materialization.IncrementalPredicate = "target.val > 0"
				return a
			}(),
			wantError: "requires at least one column with `update_on_merge` or `merge_sql`",
		},
		{
			name: "invalid opt-in value",
			asset: func() *pipeline.Asset {
				a := nativeMergeAsset([]pipeline.Column{{Name: "id", PrimaryKey: true}})
				a.Parameters = pipeline.ParameterMap{NativeMergeParameter: "sometimes"}
				return a
			}(),
			wantError: `parameters.fabric_native_merge must be true or false, got "sometimes"`,
		},
		{
			name: "list opt-in value",
			asset: func() *pipeline.Asset {
				a := nativeMergeAsset([]pipeline.Column{{Name: "id", PrimaryKey: true}})
				a.Parameters = pipeline.ParameterMap{NativeMergeParameter: []interface{}{true}}
				return a
			}(),
			wantError: "parameters.fabric_native_merge must be true or false, got [true]",
		},
		{
			name: "empty opt-in value",
			asset: func() *pipeline.Asset {
				a := nativeMergeAsset([]pipeline.Column{{Name: "id", PrimaryKey: true}})
				a.Parameters = pipeline.ParameterMap{NativeMergeParameter: nil}
				return a
			}(),
			wantError: "parameters.fabric_native_merge must be true or false, got <nil>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := buildMergeQuery(tt.asset, "SELECT 1")
			require.ErrorContains(t, err, tt.wantError)
		})
	}
}

func TestNativeMergeFullRefreshUsesCreateReplace(t *testing.T) {
	t.Parallel()

	asset := nativeMergeAsset([]pipeline.Column{{Name: "id", PrimaryKey: true}})
	result, err := NewMaterializer(true).Render(asset, "SELECT 1 AS id")
	require.NoError(t, err)
	expected, err := buildCreateReplaceQuery(asset, "SELECT 1 AS id")
	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestNativeMergeCreatesMissingTableForRefreshRestrictedAssets(t *testing.T) {
	t.Parallel()

	restricted := true
	notRestricted := false
	nullable := false
	typedColumns := []pipeline.Column{
		{Name: "id", Type: "INT", PrimaryKey: true},
		{Name: "name", Type: "VARCHAR(100)", UpdateOnMerge: true},
		{Name: "updated_at", Type: "DATETIME2(6)", Nullable: pipeline.DefaultTrueBool{Value: &nullable}},
	}
	newAsset := func(refreshRestricted *bool, columns []pipeline.Column) *pipeline.Asset {
		asset := nativeMergeAsset(columns)
		asset.RefreshRestricted = refreshRestricted
		return asset
	}
	// The query reads its own target and returns columns in a different order than
	// they are declared. The table is created before the query runs, and MERGE
	// inserts by column name, so both work on the first run and every later one.
	query := "SELECT name, id, updated_at FROM src WHERE id > (SELECT COALESCE(MAX(id), 0) FROM dbo.Table)"

	// Fabric rejects key constraints in CREATE TABLE, so there is no PRIMARY KEY clause.
	want := "IF OBJECT_ID('dbo.Table', 'U') IS NULL\n" +
		"BEGIN\n" +
		"CREATE TABLE [dbo].[Table] (\n" +
		"    [id] INT NOT NULL,\n" +
		"    [name] VARCHAR(100),\n" +
		"    [updated_at] DATETIME2(6) NOT NULL\n" +
		");\n" +
		"END;\n" +
		"CREATE TABLE [dbo].[Table__bruin_tmp_abcefghi] AS\n" +
		query + ";\n" +
		"IF EXISTS (SELECT 1 FROM [dbo].[Table__bruin_tmp_abcefghi] GROUP BY [id] HAVING COUNT(*) > 1)\n" +
		"BEGIN\n" +
		"DROP TABLE IF EXISTS [dbo].[Table__bruin_tmp_abcefghi];\n" +
		"THROW 50000, N'Fabric merge into dbo.Table failed: the query returned more than one row for the same primary key (id). Deduplicate the query so that each key appears once.', 1;\n" +
		"END;\n" +
		"MERGE INTO [dbo].[Table] target\n" +
		"USING [dbo].[Table__bruin_tmp_abcefghi] source\n" +
		"ON target.[id] = source.[id]\n" +
		"WHEN MATCHED THEN UPDATE SET target.[name] = source.[name]\n" +
		"WHEN NOT MATCHED BY TARGET THEN INSERT ([id], [name], [updated_at]) VALUES (source.[id], source.[name], source.[updated_at]);\n" +
		"DROP TABLE IF EXISTS [dbo].[Table__bruin_tmp_abcefghi];"

	// Even a full refresh keeps a refresh_restricted asset on its strategy.
	for _, fullRefresh := range []bool{false, true} {
		result, err := NewMaterializer(fullRefresh).Render(newAsset(&restricted, typedColumns), query)
		require.NoError(t, err)
		assert.Equal(t, want, result)
	}

	for _, unchanged := range []*pipeline.Asset{
		newAsset(nil, typedColumns),
		newAsset(&notRestricted, typedColumns),
		newAsset(&restricted, []pipeline.Column{{Name: "id", PrimaryKey: true}, {Name: "name", Type: "VARCHAR(100)", UpdateOnMerge: true}}),
	} {
		result, err := NewMaterializer(false).Render(unchanged, query)
		require.NoError(t, err)
		assert.NotContains(t, result, "OBJECT_ID")
	}
}

// Strategies that copy staged rows by position don't create a missing target:
// one created from the declared columns could order them differently from the
// query. refresh_restricted leaves their SQL unchanged.
func TestPositionalStrategiesIgnoreRefreshRestricted(t *testing.T) {
	t.Parallel()

	restricted := true
	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategyAppend,
		pipeline.MaterializationStrategyDeleteInsert,
		pipeline.MaterializationStrategyMerge,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			t.Parallel()

			newAsset := func(refreshRestricted *bool) *pipeline.Asset {
				return &pipeline.Asset{
					Name:              "dbo.Table",
					Materialization:   pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: strategy},
					RefreshRestricted: refreshRestricted,
					Columns: []pipeline.Column{
						{Name: "id", Type: "INT", PrimaryKey: true},
						{Name: "amount", Type: "INT"},
					},
				}
			}

			restrictedResult, err := NewMaterializer(false).Render(newAsset(&restricted), "SELECT id, amount FROM src")
			require.NoError(t, err)
			plainResult, err := NewMaterializer(false).Render(newAsset(nil), "SELECT id, amount FROM src")
			require.NoError(t, err)
			assert.Equal(t, plainResult, restrictedResult)
			assert.NotContains(t, restrictedResult, "OBJECT_ID")
		})
	}
}

func TestAppendQuery(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name:            "dbo.Table",
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyAppend},
		Columns:         []pipeline.Column{{Name: "id", Type: "INT"}},
	}
	result, err := NewMaterializer(false).Render(asset, "SELECT 1 AS id;")
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO [dbo].[Table]\nSELECT 1 AS id", result)
}

func TestBuildTruncateInsertQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "simple query",
			query: "SELECT id, val FROM src;",
		},
		{
			name:  "query with CTE",
			query: "WITH cte AS (SELECT id, val FROM src) SELECT * FROM cte;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			asset := &pipeline.Asset{Name: "dbo.Table"}
			result, err := buildTruncateInsertQuery(asset, tt.query)
			require.NoError(t, err)
			expected := "CREATE TABLE [dbo].[__bruin_tmp_abcefghi] AS\n" +
				strings.TrimSuffix(tt.query, ";") + ";\n" +
				"TRUNCATE TABLE [dbo].[Table];\n" +
				"INSERT INTO [dbo].[Table] SELECT * FROM [dbo].[__bruin_tmp_abcefghi];\n" +
				"DROP TABLE IF EXISTS [dbo].[__bruin_tmp_abcefghi];"
			assert.Equal(t, expected, result)
		})
	}
}

func TestBuildDDLQuery(t *testing.T) {
	t.Parallel()

	nullable := false
	asset := &pipeline.Asset{
		Name: "dbo.Table",
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: pipeline.MaterializationStrategyDDL,
		},
		Columns: []pipeline.Column{
			{Name: "id", Type: "INT", PrimaryKey: true},
			{Name: "name", Type: "VARCHAR(100)", Nullable: pipeline.DefaultTrueBool{Value: &nullable}},
		},
	}

	result, err := buildDDLQuery(asset, "")
	require.NoError(t, err)

	expected := "IF OBJECT_ID('dbo.Table', 'U') IS NULL\n" +
		"BEGIN\n" +
		"CREATE TABLE [dbo].[Table] (\n" +
		"    [id] INT NOT NULL,\n" +
		"    [name] VARCHAR(100) NOT NULL,\n" +
		"    PRIMARY KEY ([id])\n" +
		")\n" +
		"END;"
	assert.Equal(t, expected, result)
}

func intp(i int) *int { return &i }

func TestColumnMetadataDDL(t *testing.T) {
	t.Parallel()
	asset := &pipeline.Asset{
		Name:            "orders",
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyDDL},
		Columns: []pipeline.Column{
			{Name: "amount", Type: "decimal", Precision: intp(10), Scale: intp(2)},
			{Name: "name", Type: "varchar", Length: intp(255)},
		},
	}
	render, err := NewMaterializer(false).Render(asset, "SELECT 1")
	require.NoError(t, err)
	assert.Contains(t, render, "decimal(10, 2)")
	assert.Contains(t, render, "varchar(255)")
}
