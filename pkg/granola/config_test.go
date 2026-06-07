package granola

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "basic config",
			config: Config{
				APIKey: "granola-api-key",
			},
			expected: "granola://?api_key=granola-api-key",
		},
		{
			name: "encodes api key",
			config: Config{
				APIKey: "key with spaces&symbols=ok",
			},
			expected: "granola://?api_key=key+with+spaces%26symbols%3Dok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.config.GetIngestrURI())
		})
	}
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{APIKey: "granola-api-key"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "granola://?api_key=granola-api-key", uri)
}
