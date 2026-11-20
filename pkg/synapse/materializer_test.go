package synapse

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestMaterializer_RenderFullRefreshRejectsDataVaultStrategies(t *testing.T) {
	t.Parallel()

	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategyDataVaultHub,
		pipeline.MaterializationStrategyDataVaultLink,
		pipeline.MaterializationStrategyDataVaultSatellite,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			t.Parallel()

			asset := &pipeline.Asset{
				Name: "dv.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: strategy,
				},
			}

			queries, err := NewMaterializer(true).Render(asset, "SELECT 1 AS id")
			require.ErrorContains(t, err, "unsupported materialization type")
			require.Empty(t, queries)
		})
	}
}

func TestMaterializer_RenderFullRefreshStillRendersCreateReplace(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "dv.asset",
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: pipeline.MaterializationStrategyCreateReplace,
		},
	}

	queries, err := NewMaterializer(true).Render(asset, "SELECT 1 AS id")
	require.NoError(t, err)
	require.NotEmpty(t, queries)
}
