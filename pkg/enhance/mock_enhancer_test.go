package enhance

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestMockEnhancer_SetAPIKey(t *testing.T) {
	t.Parallel()

	var capturedKey string
	mock := &MockEnhancer{
		SetAPIKeyFunc: func(apiKey string) {
			capturedKey = apiKey
		},
	}

	mock.SetAPIKey("test-key")
	assert.Equal(t, "test-key", capturedKey)
}

func TestMockEnhancer_SetDebug(t *testing.T) {
	t.Parallel()

	var capturedDebug bool
	mock := &MockEnhancer{
		SetDebugFunc: func(debug bool) {
			capturedDebug = debug
		},
	}

	mock.SetDebug(true)
	assert.True(t, capturedDebug)
}

func TestMockEnhancer_EnsureClaudeCLI(t *testing.T) {
	t.Parallel()

	t.Run("returns nil by default", func(t *testing.T) {
		t.Parallel()
		mock := &MockEnhancer{}
		err := mock.EnsureClaudeCLI()
		assert.NoError(t, err)
	})

	t.Run("returns custom error when set", func(t *testing.T) {
		t.Parallel()
		expectedErr := errors.New("claude not found")
		mock := &MockEnhancer{
			EnsureClaudeCLIFunc: func() error {
				return expectedErr
			},
		}
		err := mock.EnsureClaudeCLI()
		assert.Equal(t, expectedErr, err)
	})
}

func TestMockEnhancer_EnhanceAsset(t *testing.T) {
	t.Parallel()

	t.Run("returns nil by default", func(t *testing.T) {
		t.Parallel()
		mock := &MockEnhancer{}
		asset := &pipeline.Asset{Name: "test_asset"}
		err := mock.EnhanceAsset(context.Background(), asset, "test_pipeline", "")
		assert.NoError(t, err)
	})

	t.Run("calls custom function when set", func(t *testing.T) {
		t.Parallel()
		var capturedAssetName string
		var capturedPipelineName string
		var capturedTableSummary string

		mock := &MockEnhancer{
			EnhanceAssetFunc: func(ctx context.Context, asset *pipeline.Asset, pipelineName, tableSummaryJSON string) error {
				capturedAssetName = asset.Name
				capturedPipelineName = pipelineName
				capturedTableSummary = tableSummaryJSON
				return nil
			},
		}

		asset := &pipeline.Asset{Name: "my_asset"}
		err := mock.EnhanceAsset(context.Background(), asset, "my_pipeline", `{"table_name": "test"}`)

		assert.NoError(t, err)
		assert.Equal(t, "my_asset", capturedAssetName)
		assert.Equal(t, "my_pipeline", capturedPipelineName)
		assert.Equal(t, `{"table_name": "test"}`, capturedTableSummary)
	})

	t.Run("returns custom error when set", func(t *testing.T) {
		t.Parallel()
		expectedErr := errors.New("enhancement failed")
		mock := &MockEnhancer{
			EnhanceAssetFunc: func(ctx context.Context, asset *pipeline.Asset, pipelineName, tableSummaryJSON string) error {
				return expectedErr
			},
		}

		asset := &pipeline.Asset{Name: "test_asset"}
		err := mock.EnhanceAsset(context.Background(), asset, "test_pipeline", "")
		assert.Equal(t, expectedErr, err)
	})
}

func TestMockEnhancer_ImplementsInterface(t *testing.T) {
	t.Parallel()

	// This test verifies that MockEnhancer implements EnhancerInterface
	var _ EnhancerInterface = (*MockEnhancer)(nil)
}
