package balldontlie

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
			name:     "api key only",
			config:   Config{APIKey: "balldontlie-key"},
			expected: "balldontlie://?api_key=balldontlie-key",
		},
		{
			name: "season",
			config: Config{
				APIKey: "balldontlie-key",
				Season: "2026",
			},
			expected: "balldontlie://?api_key=balldontlie-key&season=2026",
		},
		{
			name: "all params",
			config: Config{
				APIKey:  "balldontlie-key",
				Season:  "2022",
				BaseURL: "https://example.test",
			},
			expected: "balldontlie://?api_key=balldontlie-key&base_url=https%3A%2F%2Fexample.test&season=2022",
		},
		{
			name:     "encodes api key",
			config:   Config{APIKey: "key with spaces&symbols=ok"},
			expected: "balldontlie://?api_key=key+with+spaces%26symbols%3Dok",
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
	require.EqualError(t, err, "balldontlie: api_key must be provided")
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{APIKey: "balldontlie-key", Season: "2026"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "balldontlie://?api_key=balldontlie-key&season=2026", uri)
}
