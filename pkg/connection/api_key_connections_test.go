package connection

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewManagerFromConfigRegistersAPIKeyConnections(t *testing.T) {
	t.Setenv("OPENAI_TEST_KEY", "resolved-key")
	connections := &config.Connections{
		OpenAI:     []config.APIKeyConnection{{ConnectionMetadata: config.ConnectionMetadata{Name: "openai-main"}, APIKey: "${OPENAI_TEST_KEY}"}},
		OpenCode:   []config.APIKeyConnection{{ConnectionMetadata: config.ConnectionMetadata{Name: "opencode-main"}, APIKey: "key"}},
		OpenRouter: []config.APIKeyConnection{{ConnectionMetadata: config.ConnectionMetadata{Name: "openrouter-main"}, APIKey: "key"}},
		Google:     []config.APIKeyConnection{{ConnectionMetadata: config.ConnectionMetadata{Name: "google-main"}, APIKey: "key"}},
		Typesafe:   []config.APIKeyConnection{{ConnectionMetadata: config.ConnectionMetadata{Name: "typesafe-main"}, APIKey: "key"}},
	}
	cfg := &config.Config{SelectedEnvironment: &config.Environment{Connections: connections}}

	getter, errs := NewManagerFromConfig(cfg)
	require.Empty(t, errs)

	for _, provider := range []string{"openai", "opencode", "openrouter", "google", "typesafe"} {
		name := provider + "-main"
		assert.Equal(t, provider, getter.GetConnectionType(name))
		connection, ok := getter.GetConnection(name).(*config.APIKeyConnection)
		require.True(t, ok)
		assert.Same(t, connection, getter.GetConnectionDetails(name))
	}
	assert.Equal(t, "resolved-key", getter.GetConnection("openai-main").(*config.APIKeyConnection).GetAPIKey())
}
