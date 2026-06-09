package wistia

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want map[string]string
	}{
		{
			name: "access token",
			cfg: Config{
				AccessToken: "wistia-token",
			},
			want: map[string]string{
				"access_token": "wistia-token",
			},
		},
		{
			name: "api key alias",
			cfg: Config{
				APIKey: "api-key-token",
			},
			want: map[string]string{
				"access_token": "api-key-token",
			},
		},
		{
			name: "token alias",
			cfg: Config{
				Token: "token-alias",
			},
			want: map[string]string{
				"access_token": "token-alias",
			},
		},
		{
			name: "optional parameters",
			cfg: Config{
				AccessToken: "tok en+/=",
				APIVersion:  "2026-03",
				BaseURL:     "https://api.example.com/modern?env=test",
			},
			want: map[string]string{
				"access_token": "tok en+/=",
				"api_version":  "2026-03",
				"base_url":     "https://api.example.com/modern?env=test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			requireQueryValues(t, tt.cfg.GetIngestrURI(), tt.want)
		})
	}
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{AccessToken: "wistia-token"})
	require.NoError(t, err)

	got, err := client.GetIngestrURI()

	require.NoError(t, err)
	requireQueryValues(t, got, map[string]string{
		"access_token": "wistia-token",
	})
}

func requireQueryValues(t *testing.T, rawURI string, want map[string]string) {
	t.Helper()

	uri, err := url.Parse(rawURI)
	require.NoError(t, err)
	require.Equal(t, "wistia", uri.Scheme)
	require.Empty(t, uri.Host)
	require.Empty(t, uri.Path)

	query := uri.Query()
	require.Len(t, query, len(want))
	for key, value := range want {
		require.Equal(t, value, query.Get(key), "query param %s", key)
	}
}
