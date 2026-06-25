package redditads

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
				AccessToken: "token_123",
			},
			expected: "redditads://?access_token=token_123",
		},
		{
			name: "with oauth app credentials",
			config: Config{
				AccessToken:  "token_123",
				ClientID:     "cid",
				ClientSecret: "csec",
			},
			expected: "redditads://?access_token=token_123&client_id=cid&client_secret=csec",
		},
		{
			name: "with refresh credentials and no access token",
			config: Config{
				ClientID:     "cid",
				ClientSecret: "csec",
				RefreshToken: "rtok",
			},
			expected: "redditads://?client_id=cid&client_secret=csec&refresh_token=rtok",
		},
		{
			name: "encodes query parameters",
			config: Config{
				AccessToken: "token with spaces&symbols=ok",
			},
			expected: "redditads://?access_token=token+with+spaces%26symbols%3Dok",
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

func TestConfig_GetIngestrURI_RequiresCredentials(t *testing.T) {
	t.Parallel()

	_, err := (&Config{}).GetIngestrURI()
	require.EqualError(t, err, "reddit_ads: either access_token, or client_id + client_secret + refresh_token, must be provided")

	// Incomplete refresh credentials (missing refresh_token) are rejected too.
	_, err = (&Config{ClientID: "cid", ClientSecret: "csec"}).GetIngestrURI()
	require.Error(t, err)
}

func TestConfig_GetIngestrURI_AccessTokenOnly(t *testing.T) {
	t.Parallel()

	got, err := (&Config{AccessToken: "token_123"}).GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "redditads://?access_token=token_123", got)
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{
		AccessToken: "token_123",
	})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "redditads://?access_token=token_123", uri)
}
