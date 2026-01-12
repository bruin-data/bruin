package enhance

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewEnhancer(t *testing.T) {
	t.Parallel()

	t.Run("creates Claude provider by default", func(t *testing.T) {
		t.Parallel()
		enhancer := NewEnhancer(ProviderClaude, "")

		assert.NotNil(t, enhancer)
		assert.NotNil(t, enhancer.provider)
		assert.Equal(t, "claude", enhancer.provider.Name())
	})

	t.Run("creates OpenCode provider when specified", func(t *testing.T) {
		t.Parallel()
		enhancer := NewEnhancer(ProviderOpenCode, "")

		assert.NotNil(t, enhancer)
		assert.NotNil(t, enhancer.provider)
		assert.Equal(t, "opencode", enhancer.provider.Name())
	})

	t.Run("creates Codex provider when specified", func(t *testing.T) {
		t.Parallel()
		enhancer := NewEnhancer(ProviderCodex, "")

		assert.NotNil(t, enhancer)
		assert.NotNil(t, enhancer.provider)
		assert.Equal(t, "codex", enhancer.provider.Name())
	})

	t.Run("defaults to Claude when invalid provider type", func(t *testing.T) {
		t.Parallel()
		enhancer := NewEnhancer(ProviderType("invalid"), "")

		assert.NotNil(t, enhancer)
		assert.NotNil(t, enhancer.provider)
		assert.Equal(t, "claude", enhancer.provider.Name())
	})
}

func TestEnhancer_SetAPIKey(t *testing.T) {
	t.Parallel()
	enhancer := NewEnhancer(ProviderClaude, "")

	// SetAPIKey should not panic
	assert.NotPanics(t, func() {
		enhancer.SetAPIKey("sk-new-key")
	})
}

func TestEnhancer_SetDebug(t *testing.T) {
	t.Parallel()
	enhancer := NewEnhancer(ProviderClaude, "")

	// SetDebug should not panic
	assert.NotPanics(t, func() {
		enhancer.SetDebug(true)
	})
}
