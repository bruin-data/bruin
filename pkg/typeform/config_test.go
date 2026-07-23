package typeform

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
			name:     "token only",
			config:   Config{Token: "my-token"},
			expected: "typeform://?token=my-token",
		},
		{
			name:     "token with region",
			config:   Config{Token: "my-token", Region: "eu"},
			expected: "typeform://?token=my-token&region=eu",
		},
		{
			name:     "escapes token",
			config:   Config{Token: "tok/with+symbols"},
			expected: "typeform://?token=tok%2Fwith%2Bsymbols",
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

	client, err := NewClient(Config{Token: "my-token", Region: "us"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "typeform://?token=my-token&region=us", uri)
}
