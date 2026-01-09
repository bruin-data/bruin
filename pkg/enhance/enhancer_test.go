package enhance

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestNewEnhancer(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()

	t.Run("uses default model when empty", func(t *testing.T) {
		t.Parallel()
		enhancer := NewEnhancer(fs, "")

		assert.Equal(t, defaultModel, enhancer.GetModel())
	})

	t.Run("uses provided model", func(t *testing.T) {
		t.Parallel()
		enhancer := NewEnhancer(fs, "claude-opus-4-20250514")

		assert.Equal(t, "claude-opus-4-20250514", enhancer.GetModel())
	})
}

func TestNewEnhancerWithAPIKey(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()

	enhancer := NewEnhancerWithAPIKey(fs, "claude-sonnet-4-20250514", "sk-test-key")

	assert.Equal(t, "claude-sonnet-4-20250514", enhancer.GetModel())
	assert.Equal(t, "sk-test-key", enhancer.apiKey)
}

func TestEnhancer_SetAPIKey(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()
	enhancer := NewEnhancer(fs, "")

	enhancer.SetAPIKey("sk-new-key")

	assert.Equal(t, "sk-new-key", enhancer.apiKey)
}

func TestEnhancer_IsClaudeCLIInstalled(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()

	t.Run("returns false when path is empty", func(t *testing.T) {
		t.Parallel()
		enhancer := &Enhancer{fs: fs, model: defaultModel, claudePath: ""}

		assert.False(t, enhancer.IsClaudeCLIInstalled())
	})

	t.Run("returns true when path is set", func(t *testing.T) {
		t.Parallel()
		enhancer := &Enhancer{fs: fs, model: defaultModel, claudePath: "/usr/local/bin/claude"}

		assert.True(t, enhancer.IsClaudeCLIInstalled())
	})
}

func TestEnhancer_SetDebug(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()
	enhancer := NewEnhancer(fs, "")

	enhancer.SetDebug(true)

	assert.True(t, enhancer.debug)
}
