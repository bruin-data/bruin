package bamboohr

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
			name: "api key auth",
			config: Config{
				CompanyDomain: "acme",
				APIKey:        "secret",
			},
			want: "bamboohr://acme?api_key=secret",
		},
		{
			name: "access token auth",
			config: Config{
				CompanyDomain: "acme",
				AccessToken:   "token",
			},
			want: "bamboohr://acme?access_token=token",
		},
		{
			name: "api key with timezone",
			config: Config{
				CompanyDomain: "acme",
				APIKey:        "secret",
				Timezone:      "America/Denver",
			},
			want: "bamboohr://acme?api_key=secret&timezone=America%2FDenver",
		},
		{
			name:    "missing company domain",
			config:  Config{APIKey: "secret"},
			wantErr: true,
		},
		{
			name:    "missing credentials",
			config:  Config{CompanyDomain: "acme"},
			wantErr: true,
		},
		{
			name: "mutually exclusive credentials",
			config: Config{
				CompanyDomain: "acme",
				APIKey:        "secret",
				AccessToken:   "token",
			},
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

	client, err := NewClient(Config{CompanyDomain: "acme", APIKey: "secret"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "bamboohr://acme?api_key=secret", uri)
}
