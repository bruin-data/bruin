package fabric

import (
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
			"SELECT 1\n;\n" +
			"IF OBJECT_ID('dbo.Table', 'U') IS NOT NULL BEGIN EXEC sp_rename 'dbo.Table', 'Table__bruin_backup' END;\n" +
			"EXEC sp_rename 'dbo.Table__bruin_tmp', 'Table';\n" +
			"DROP TABLE IF EXISTS [dbo].[Table__bruin_backup];"
		assert.Equal(t, expected, result)
	})

	t.Run("query with CTE", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{Name: "dbo.Table"}
		result, err := buildCreateReplaceQuery(asset, "WITH monthly AS (SELECT id, amount FROM sales) SELECT * FROM monthly\n;")
		require.NoError(t, err)
		expected := "DROP TABLE IF EXISTS [dbo].[Table__bruin_tmp];\n" +
			"DROP TABLE IF EXISTS [dbo].[Table__bruin_backup];\n" +
			"CREATE TABLE [dbo].[Table__bruin_tmp] AS\n" +
			"WITH monthly AS (SELECT id, amount FROM sales) SELECT * FROM monthly\n;\n" +
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
			"SELECT 1\n;\n" +
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
			"WITH cte AS (SELECT id, val FROM src) SELECT * FROM cte\n;\n" +
			"DELETE FROM [dbo].[Table] WHERE EXISTS (\n" +
			"  SELECT 1 FROM [dbo].[Table__bruin_tmp] WHERE [dbo].[Table].[id] = [dbo].[Table__bruin_tmp].[id]\n" +
			");\n" +
			"INSERT INTO [dbo].[Table] SELECT * FROM [dbo].[Table__bruin_tmp];\n" +
			"DROP TABLE IF EXISTS [dbo].[Table__bruin_tmp];"
		assert.Equal(t, expected, result)
	})
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
