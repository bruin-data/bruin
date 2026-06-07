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
				AccountIds:  "id_123,id_456",
			},
			expected: "redditads://?access_token=token_123&account_ids=id_123%2Cid_456",
		},
		{
			name: "encodes query parameters",
			config: Config{
				AccessToken: "token with spaces&symbols=ok",
				AccountIds:  "account 1,account&2",
			},
			expected: "redditads://?access_token=token+with+spaces%26symbols%3Dok&account_ids=account+1%2Caccount%262",
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
		AccessToken: "token_123",
		AccountIds:  "id_123",
	})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "redditads://?access_token=token_123&account_ids=id_123", uri)
}
