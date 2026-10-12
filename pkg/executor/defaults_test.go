package executor

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

func TestSQLSourceAssetsUseNoOpExecutors(t *testing.T) {
	t.Parallel()

	for _, assetType := range []pipeline.AssetType{
		pipeline.AssetTypeDremioSource,
		pipeline.AssetTypeFabricSource,
		pipeline.AssetTypeMsSQLSource,
	} {
		t.Run(string(assetType), func(t *testing.T) {
			t.Parallel()

			config, ok := DefaultExecutorsV2[assetType]
			require.True(t, ok)
			for _, taskType := range []scheduler.TaskInstanceType{
				scheduler.TaskInstanceTypeMain,
				scheduler.TaskInstanceTypeMetadataPush,
				scheduler.TaskInstanceTypeColumnCheck,
				scheduler.TaskInstanceTypeCustomCheck,
			} {
				_, ok := config[taskType].(NoOpOperator)
				require.True(t, ok)
			}
		})
	}
}
