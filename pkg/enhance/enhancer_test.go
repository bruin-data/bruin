package enhance

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewEnhancer(t *testing.T) {
	t.Parallel()

	t.Run("uses default model when empty", func(t *testing.T) {
		t.Parallel()
		enhancer := NewEnhancer("")

		assert.Equal(t, defaultModel, enhancer.model)
	})

	t.Run("uses provided model", func(t *testing.T) {
		t.Parallel()
		enhancer := NewEnhancer("claude-opus-4-20250514")

		assert.Equal(t, "claude-opus-4-20250514", enhancer.model)
	})
}

func TestEnhancer_SetAPIKey(t *testing.T) {
	t.Parallel()
	enhancer := NewEnhancer("")

	enhancer.SetAPIKey("sk-new-key")

	assert.Equal(t, "sk-new-key", enhancer.apiKey)
}

func TestEnhancer_SetDebug(t *testing.T) {
	t.Parallel()
	enhancer := NewEnhancer("")

	enhancer.SetDebug(true)

	assert.True(t, enhancer.debug)
}
