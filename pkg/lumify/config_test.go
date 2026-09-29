package lumify

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		want    string
		wantErr bool
	}{
		{
			name: "required fields only",
			config: Config{
				APIKey: "lmfy-secret",
			},
			want: "lumify://?api_key=lmfy-secret",
		},
		{
			name: "all optional parameters",
			config: Config{
				APIKey:  "lmfy-secret",
				Sport:   "nba",
				League:  "nba-league",
				BaseURL: "https://staging.lumify.ai",
			},
			want: "lumify://?api_key=lmfy-secret&base_url=https%3A%2F%2Fstaging.lumify.ai&league=nba-league&sport=nba",
		},
		{
			name:    "missing api key",
			config:  Config{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.config.GetIngestrURI()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{APIKey: "lmfy-secret", Sport: "nba"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "lumify://?api_key=lmfy-secret&sport=nba", uri)
}
