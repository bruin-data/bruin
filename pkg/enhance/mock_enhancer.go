package enhance

import (
	"context"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// MockEnhancer is a mock implementation of EnhancerInterface for testing.
type MockEnhancer struct {
	SetAPIKeyFunc      func(apiKey string)
	SetDebugFunc       func(debug bool)
	EnsureClaudeCLIFunc func() error
	EnhanceAssetFunc   func(ctx context.Context, asset *pipeline.Asset, pipelineName, tableSummaryJSON string) error
}

// SetAPIKey sets the API key.
func (m *MockEnhancer) SetAPIKey(apiKey string) {
	if m.SetAPIKeyFunc != nil {
		m.SetAPIKeyFunc(apiKey)
	}
}

// SetDebug sets debug mode.
func (m *MockEnhancer) SetDebug(debug bool) {
	if m.SetDebugFunc != nil {
		m.SetDebugFunc(debug)
	}
}

// EnsureClaudeCLI ensures Claude CLI is available.
func (m *MockEnhancer) EnsureClaudeCLI() error {
	if m.EnsureClaudeCLIFunc != nil {
		return m.EnsureClaudeCLIFunc()
	}
	return nil
}

// EnhanceAsset enhances an asset.
func (m *MockEnhancer) EnhanceAsset(ctx context.Context, asset *pipeline.Asset, pipelineName, tableSummaryJSON string) error {
	if m.EnhanceAssetFunc != nil {
		return m.EnhanceAssetFunc(ctx, asset, pipelineName, tableSummaryJSON)
	}
	return nil
}
