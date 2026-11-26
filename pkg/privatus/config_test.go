package privatus

import (
	"net/url"
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
			name:   "required fields only",
			config: Config{APIKey: "pat_123"},
			want:   "privatus://?api_key=pat_123",
		},
		{
			name: "all fields",
			config: Config{
				APIKey:  "pat_123",
				SiteIDs: []string{"pa_AB12CD34EF", "pa_XY98ZW76VU"},
			},
			want: "privatus://?api_key=pat_123&site_ids=pa_AB12CD34EF%2Cpa_XY98ZW76VU",
		},
		{
			name:   "special characters in the api key are escaped",
			config: Config{APIKey: "a+b/c=d&e?f#g"},
			want:   "privatus://?api_key=a%2Bb%2Fc%3Dd%26e%3Ff%23g",
		},
		{
			name:   "api key is not trimmed",
			config: Config{APIKey: " pat_123 "},
			want:   "privatus://?api_key=+pat_123+",
		},
		{
			name:   "site ids are trimmed and blank entries skipped",
			config: Config{APIKey: "pat_123", SiteIDs: []string{" pa_AB12CD34EF ", "", "  "}},
			want:   "privatus://?api_key=pat_123&site_ids=pa_AB12CD34EF",
		},
		{
			name:   "only blank site ids are left out",
			config: Config{APIKey: "pat_123", SiteIDs: []string{"", " "}},
			want:   "privatus://?api_key=pat_123",
		},
		{
			name:    "missing api key",
			config:  Config{SiteIDs: []string{"pa_AB12CD34EF"}},
			wantErr: "privatus: api_key must be provided",
		},
		{
			name:    "blank api key",
			config:  Config{APIKey: "   "},
			wantErr: "privatus: api_key must be provided",
		},
		{
			name:    "invalid site id",
			config:  Config{APIKey: "pat_123", SiteIDs: []string{"pa_AB12CD34EF", "AB12CD34EF"}},
			wantErr: `privatus: invalid site id "AB12CD34EF" in site_ids, expected a value like pa_AB12CD34EF`,
		},
		{
			name:    "site id with a comma",
			config:  Config{APIKey: "pat_123", SiteIDs: []string{"pa_AB,pa_CD"}},
			wantErr: `privatus: invalid site id "pa_AB,pa_CD" in site_ids, expected a value like pa_AB12CD34EF`,
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
// the way ingestr parses it.
func TestConfig_GetIngestrURI_RoundTrip(t *testing.T) {
	t.Parallel()

	c := Config{APIKey: "a+b/c=d&e?f#g %", SiteIDs: []string{"pa_AB12CD34EF", "pa_XY98ZW76VU"}}

	uri, err := c.GetIngestrURI()
	require.NoError(t, err)

	parsed, err := url.Parse(uri)
	require.NoError(t, err)
	assert.Equal(t, "privatus", parsed.Scheme)

	q := parsed.Query()
	assert.Equal(t, c.APIKey, q.Get("api_key"))
	assert.Equal(t, "pa_AB12CD34EF,pa_XY98ZW76VU", q.Get("site_ids"))
}

// TestConfig_GetIngestrURI_APIKeyIsMasked checks that the api_key in the URI is redacted by the masker
// that bruin builds from the configured value, including values with surrounding whitespace.
func TestConfig_GetIngestrURI_APIKeyIsMasked(t *testing.T) {
	t.Parallel()

	for _, apiKey := range []string{"pat_123", " pat_123 ", "a+b/c=d&e?f#g %"} {
		c := Config{APIKey: apiKey, SiteIDs: []string{"pa_AB12CD34EF"}}

		uri, err := c.GetIngestrURI()
		require.NoError(t, err)

		masked := mask.New([]string{apiKey}).Mask(uri)
		assert.NotContains(t, masked, "pat_123", apiKey)
		assert.NotContains(t, masked, url.QueryEscape(apiKey), apiKey)
		assert.Contains(t, masked, "site_ids=pa_AB12CD34EF", apiKey)
	}
}
