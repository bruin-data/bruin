package cloudflareradar

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
			name:   "valid api token",
			config: Config{APIToken: "test-token"},
			want:   "cloudflare-radar://?api_token=test-token",
		},
		{
			name:   "token with special characters is escaped",
			config: Config{APIToken: "a b&c"},
			want:   "cloudflare-radar://?api_token=a+b%26c",
		},
		{
			name:    "missing api token",
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

	client, err := NewClient(Config{APIToken: "test-token"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "cloudflare-radar://?api_token=test-token", uri)
}
