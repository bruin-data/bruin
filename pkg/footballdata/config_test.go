package footballdata

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
			config:   Config{APIKey: "footballdata-key"},
			expected: "footballdata://?api_key=footballdata-key",
		},
		{
			name: "competition and season",
			config: Config{
				APIKey:      "footballdata-key",
				Competition: "WC",
				Season:      "2026",
			},
			expected: "footballdata://?api_key=footballdata-key&competition=WC&season=2026",
		},
		{
			name: "all params",
			config: Config{
				APIKey:         "footballdata-key",
				Competition:    "WC",
				Season:         "2026",
				BaseURL:        "https://example.test/v4",
				Matchday:       "1",
				Status:         "FINISHED",
				Stage:          "GROUP_STAGE",
				Group:          "GROUP_A",
				UnfoldGoals:    true,
				UnfoldBookings: true,
				UnfoldSubs:     true,
				UnfoldLineups:  true,
			},
			expected: "footballdata://?api_key=footballdata-key&base_url=https%3A%2F%2Fexample.test%2Fv4&competition=WC&group=GROUP_A&matchday=1&season=2026&stage=GROUP_STAGE&status=FINISHED&unfold_bookings=true&unfold_goals=true&unfold_lineups=true&unfold_subs=true",
		},
		{
			name:     "encodes api key",
			config:   Config{APIKey: "key with spaces&symbols=ok"},
			expected: "footballdata://?api_key=key+with+spaces%26symbols%3Dok",
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
	require.EqualError(t, err, "footballdata: api_key must be provided")
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{APIKey: "footballdata-key", Competition: "WC", Season: "2026"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "footballdata://?api_key=footballdata-key&competition=WC&season=2026", uri)
}
