package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAsset_FirstRun(t *testing.T) {
	t.Parallel()

	restricted := true
	asset := &Asset{
		Name:              "analytics.orders",
		Parameters:        ParameterMap{"full_refresh": "false"},
		RefreshRestricted: &restricted,
	}

	firstRun := asset.FirstRun()

	assert.NotSame(t, asset, firstRun)
	assert.True(t, firstRun.IsFirstRun())
	assert.True(t, firstRun.FullRefreshEnabled(false))
	assert.Equal(t, asset.Name, firstRun.Name)
	assert.False(t, asset.IsFirstRun())
	assert.False(t, asset.FullRefreshEnabled(true))

	var nilAsset *Asset
	assert.False(t, nilAsset.IsFirstRun())
}

func TestMaterialization_LoadsExistingTable(t *testing.T) {
	t.Parallel()

	loads := map[MaterializationStrategy]bool{
		MaterializationStrategyNone:               false,
		MaterializationStrategyCreateReplace:      false,
		MaterializationStrategyDDL:                false,
		MaterializationStrategyDataVaultHub:       false,
		MaterializationStrategyDataVaultLink:      false,
		MaterializationStrategyDataVaultSatellite: false,
		MaterializationStrategyAppend:             true,
		MaterializationStrategyMerge:              true,
		MaterializationStrategyDeleteInsert:       true,
		MaterializationStrategyTimeInterval:       true,
		MaterializationStrategyTruncateInsert:     true,
		MaterializationStrategySCD2ByColumn:       true,
		MaterializationStrategySCD2ByTime:         true,
	}
	for _, strategy := range AllAvailableMaterializationStrategies {
		_, ok := loads[strategy]
		assert.True(t, ok, "LoadsExistingTable is not decided for strategy %q", strategy)
	}

	for strategy, want := range loads {
		table := Materialization{Type: MaterializationTypeTable, Strategy: strategy}
		assert.Equal(t, want, table.LoadsExistingTable(), "table with %q", strategy)

		view := Materialization{Type: MaterializationTypeView, Strategy: strategy}
		assert.False(t, view.LoadsExistingTable(), "view with %q", strategy)
	}
}
