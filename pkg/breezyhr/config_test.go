package breezyhr

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
			name:   "all fields",
			config: Config{APIKey: "breezy_pat_123", CompanyID: "abc123"},
			want:   "breezyhr://?api_key=breezy_pat_123&company_id=abc123",
		},
		{
			name:   "special characters in query values are escaped",
			config: Config{APIKey: "a+b/c=d&e?f#g", CompanyID: "my company&x=1"},
			want:   "breezyhr://?api_key=a%2Bb%2Fc%3Dd%26e%3Ff%23g&company_id=my+company%26x%3D1",
		},
		{
			name:   "surrounding whitespace is trimmed except in the api key",
			config: Config{APIKey: " breezy_pat_123 ", CompanyID: "  abc123  "},
			want:   "breezyhr://?api_key=+breezy_pat_123+&company_id=abc123",
		},
		{
			name:    "missing api key",
			config:  Config{CompanyID: "abc123"},
			wantErr: "breezyhr: api_key must be provided",
		},
		{
			name:    "blank api key",
			config:  Config{APIKey: "   ", CompanyID: "abc123"},
			wantErr: "breezyhr: api_key must be provided",
		},
		{
			name:    "missing company id",
			config:  Config{APIKey: "breezy_pat_123"},
			wantErr: "breezyhr: company_id must be provided",
		},
		{
			name:    "blank company id",
			config:  Config{APIKey: "breezy_pat_123", CompanyID: "  "},
			wantErr: "breezyhr: company_id must be provided",
		},
		{
			name:    "api key is checked before company id",
			config:  Config{},
			wantErr: "breezyhr: api_key must be provided",
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

// TestConfig_GetIngestrURI_RoundTrip checks that the values come back unchanged when the URI is parsed
// the way ingestr parses it: strip the breezyhr:// prefix and the "?", then url.ParseQuery.
func TestConfig_GetIngestrURI_RoundTrip(t *testing.T) {
	t.Parallel()

	c := Config{APIKey: "a+b/c=d&e?f#g %", CompanyID: "abc 123"}

	uri, err := c.GetIngestrURI()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(uri, "breezyhr://"))

	values, err := url.ParseQuery(strings.TrimPrefix(strings.TrimPrefix(uri, "breezyhr://"), "?"))
	require.NoError(t, err)
	assert.Equal(t, c.APIKey, values.Get("api_key"))
	assert.Equal(t, c.CompanyID, values.Get("company_id"))
}

// TestConfig_GetIngestrURI_APIKeyIsMasked checks that the api_key in the URI is redacted by the masker
// that bruin builds from the configured value, including values with surrounding whitespace.
func TestConfig_GetIngestrURI_APIKeyIsMasked(t *testing.T) {
	t.Parallel()

	for _, apiKey := range []string{"breezy_pat_123", " breezy_pat_123 ", "a+b/c=d&e?f#g %"} {
		c := Config{APIKey: apiKey, CompanyID: "abc123"}

		uri, err := c.GetIngestrURI()
		require.NoError(t, err)

		masked := mask.New([]string{apiKey}).Mask(uri)
		assert.NotContains(t, masked, "breezy_pat_123", apiKey)
		assert.NotContains(t, masked, url.QueryEscape(apiKey), apiKey)
		assert.Contains(t, masked, "company_id=abc123", apiKey)
	}
}
