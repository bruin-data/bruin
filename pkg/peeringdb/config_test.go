package peeringdb

import (
	"net/url"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/mask"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		want    string
		wantErr string
	}{
		{
			name:   "api key",
			config: Config{APIKey: "abc.123"},
			want:   "peeringdb://?api_key=abc.123",
		},
		{
			name:   "special characters are escaped",
			config: Config{APIKey: "a+b/c=d&e?f#g"},
			want:   "peeringdb://?api_key=a%2Bb%2Fc%3Dd%26e%3Ff%23g",
		},
		{
			name:    "missing api key",
			config:  Config{},
			wantErr: "peeringdb: api_key must be provided",
		},
		{
			name:    "blank api key",
			config:  Config{APIKey: "   "},
			wantErr: "peeringdb: api_key must be provided",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.config.GetIngestrURI()
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestConfig_GetIngestrURI_RoundTripAndMasking checks that ingestr's url.Parse(uri).Query() reads the key back
// unchanged, and that the masker built from the configured value redacts it.
func TestConfig_GetIngestrURI_RoundTripAndMasking(t *testing.T) {
	t.Parallel()

	for _, apiKey := range []string{"abc.123", " abc.123 ", "a+b/c=d&e?f#g %"} {
		uri, err := (&Config{APIKey: apiKey}).GetIngestrURI()
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(uri, "peeringdb://?"))

		parsed, err := url.Parse(uri)
		require.NoError(t, err)
		assert.Equal(t, apiKey, parsed.Query().Get("api_key"))
		assert.Empty(t, parsed.Host)

		masked := mask.New([]string{apiKey}).Mask(uri)
		assert.NotContains(t, masked, "abc.123", apiKey)
		assert.NotContains(t, masked, url.QueryEscape(apiKey), apiKey)
	}
}
