package quicksight

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestBasicOperator_RunTask_NoRefresh(t *testing.T) {
	t.Parallel()

	op := NewBasicOperator(nil)

	assert.NotNil(t, op)
}

func TestBasicOperator_AssetTypes(t *testing.T) {
	t.Parallel()

	assert.Equal(t, pipeline.AssetTypeQuicksightDataset, pipeline.AssetType("quicksight.dataset"))
	assert.Equal(t, pipeline.AssetTypeQuicksightDashboard, pipeline.AssetType("quicksight.dashboard"))
}
