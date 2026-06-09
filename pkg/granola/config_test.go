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

			got, err := tt.config.GetIngestrURI()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestConfig_GetIngestrURI_RequiresAPIKey(t *testing.T) {
	t.Parallel()

	_, err := (&Config{}).GetIngestrURI()
	require.EqualError(t, err, "granola: api_key must be provided")
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{APIKey: "granola-api-key"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "granola://?api_key=granola-api-key", uri)
}
