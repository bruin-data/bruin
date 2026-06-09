package kalshi

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
			name:     "empty config",
			config:   Config{},
			expected: "kalshi://",
		},
		{
			name: "encodes query params",
			config: Config{
				QueryParams: map[string]string{
					"status":        "open",
					"series_ticker": "KXHIGHNY",
				},
			},
			expected: "kalshi://?series_ticker=KXHIGHNY&status=open",
		},
		{
			name: "skips empty query params",
			config: Config{
				QueryParams: map[string]string{
					"":       "ignored",
					"status": "",
				},
			},
			expected: "kalshi://",
		},
		{
			name: "escapes query params",
			config: Config{
				QueryParams: map[string]string{
					"event_ticker": "event ticker/with symbols",
					"tickers":      "ticker-1,ticker-2",
				},
			},
			expected: "kalshi://?event_ticker=event+ticker%2Fwith+symbols&tickers=ticker-1%2Cticker-2",
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

	client, err := NewClient(Config{
		QueryParams: map[string]string{
			"ticker": "KXHIGHNY-26JUN0600-T50",
		},
	})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "kalshi://?ticker=KXHIGHNY-26JUN0600-T50", uri)
}
