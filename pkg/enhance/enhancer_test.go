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

func TestEnhancer_BuildMCPConfig(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()

	t.Run("builds valid MCP config JSON", func(t *testing.T) {
		t.Parallel()
		enhancer := &Enhancer{
			fs:        fs,
			model:     defaultModel,
			bruinPath: "/usr/local/bin/bruin",
			useMCP:    true,
		}

		config := enhancer.buildMCPConfig()

		assert.Contains(t, config, "mcpServers")
		assert.Contains(t, config, "bruin")
		assert.Contains(t, config, "/usr/local/bin/bruin")
		assert.Contains(t, config, "mcp")
	})

	t.Run("includes environment variables when set", func(t *testing.T) {
		t.Parallel()
		enhancer := &Enhancer{
			fs:          fs,
			model:       defaultModel,
			bruinPath:   "/usr/local/bin/bruin",
			useMCP:      true,
			repoRoot:    "/path/to/repo",
			environment: "production",
		}

		config := enhancer.buildMCPConfig()

		assert.Contains(t, config, "BRUIN_REPO_ROOT")
		assert.Contains(t, config, "/path/to/repo")
		assert.Contains(t, config, "BRUIN_ENVIRONMENT")
		assert.Contains(t, config, "production")
	})

	t.Run("omits env when not set", func(t *testing.T) {
		t.Parallel()
		enhancer := &Enhancer{
			fs:        fs,
			model:     defaultModel,
			bruinPath: "/usr/local/bin/bruin",
			useMCP:    true,
		}

		config := enhancer.buildMCPConfig()

		assert.NotContains(t, config, "BRUIN_REPO_ROOT")
		assert.NotContains(t, config, "BRUIN_ENVIRONMENT")
	})
}

func TestEnhancer_SetRepoRoot(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()
	enhancer := NewEnhancer(fs, "")

	enhancer.SetRepoRoot("/path/to/repo")

	assert.Equal(t, "/path/to/repo", enhancer.repoRoot)
}

func TestEnhancer_SetEnvironment(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()
	enhancer := NewEnhancer(fs, "")

	enhancer.SetEnvironment("production")

	assert.Equal(t, "production", enhancer.environment)
}

func TestEnhancer_UseMCP(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()

	t.Run("enables MCP when bruin path is set", func(t *testing.T) {
		t.Parallel()
		enhancer := &Enhancer{
			fs:        fs,
			model:     defaultModel,
			bruinPath: "/usr/local/bin/bruin",
			useMCP:    true,
		}

		assert.True(t, enhancer.useMCP)
	})

	t.Run("disables MCP when bruin path is empty", func(t *testing.T) {
		t.Parallel()
		enhancer := &Enhancer{
			fs:        fs,
			model:     defaultModel,
			bruinPath: "",
			useMCP:    false,
		}

		assert.False(t, enhancer.useMCP)
	})
}
