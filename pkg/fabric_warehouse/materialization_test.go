package fabric_warehouse

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestViewMaterializer(t *testing.T) {
	t.Parallel()
	asset := &pipeline.Asset{Name: "dbo.MyView"}
	result, err := viewMaterializer(asset, "SELECT 1;")
	assert.NoError(t, err)
	assert.Contains(t, result, "CREATE OR ALTER VIEW [dbo].[MyView] AS")
}

func TestBuildCreateReplaceQuery(t *testing.T) {
	t.Parallel()
	asset := &pipeline.Asset{Name: "dbo.Table"}
	result, err := buildCreateReplaceQuery(asset, "SELECT 1;")
	assert.NoError(t, err)
	assert.Contains(t, result, "SELECT * INTO [dbo].[Table__bruin_tmp]")
	assert.Contains(t, result, "EXEC sp_rename 'dbo.Table', 'Table__bruin_backup'")
	assert.Contains(t, result, "EXEC sp_rename 'dbo.Table__bruin_tmp', 'Table'")
}

func TestBuildDeleteInsertQuery(t *testing.T) {
	t.Parallel()
	asset := &pipeline.Asset{Name: "dbo.Table"}

	_, err := buildDeleteInsertQuery(asset, "SELECT 1")
	assert.Error(t, err)

	asset.Columns = []pipeline.Column{{Name: "id", PrimaryKey: true}}
	result, err := buildDeleteInsertQuery(asset, "SELECT 1")
	assert.NoError(t, err)
	assert.Contains(t, result, "DELETE FROM [dbo].[Table]")
	assert.Contains(t, result, "[dbo].[Table].[id] = [dbo].[Table__bruin_tmp].[id]")
}
