package fakturoid

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func floatPtr(v float64) *float64 { return &v }

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
				ClientID:     "cid",
				ClientSecret: "csecret",
				Slug:         "acme",
				UserAgent:    "MyCompany (billing@mycompany.com)",
			},
			want: "fakturoid://?client_id=cid&client_secret=csecret&slug=acme&user_agent=MyCompany+%28billing%40mycompany.com%29",
		},
		{
			name: "with rate limit",
			config: Config{
				ClientID:     "cid",
				ClientSecret: "csecret",
				Slug:         "acme",
				UserAgent:    "MyCompany (billing@mycompany.com)",
				RateLimit:    floatPtr(3),
			},
			want: "fakturoid://?client_id=cid&client_secret=csecret&rate_limit=3&slug=acme&user_agent=MyCompany+%28billing%40mycompany.com%29",
		},
		{
			name: "missing client_id",
			config: Config{
				ClientSecret: "csecret",
				Slug:         "acme",
				UserAgent:    "MyCompany (billing@mycompany.com)",
			},
			wantErr: true,
		},
		{
			name: "missing client_secret",
			config: Config{
				ClientID:  "cid",
				Slug:      "acme",
				UserAgent: "MyCompany (billing@mycompany.com)",
			},
			wantErr: true,
		},
		{
			name: "missing slug",
			config: Config{
				ClientID:     "cid",
				ClientSecret: "csecret",
				UserAgent:    "MyCompany (billing@mycompany.com)",
			},
			wantErr: true,
		},
		{
			name: "missing user_agent",
			config: Config{
				ClientID:     "cid",
				ClientSecret: "csecret",
				Slug:         "acme",
			},
			wantErr: true,
		},
		{
			name: "non-positive rate limit",
			config: Config{
				ClientID:     "cid",
				ClientSecret: "csecret",
				Slug:         "acme",
				UserAgent:    "MyCompany (billing@mycompany.com)",
				RateLimit:    floatPtr(0),
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

	client, err := NewClient(Config{
		ClientID:     "cid",
		ClientSecret: "csecret",
		Slug:         "acme",
		UserAgent:    "MyCompany (billing@mycompany.com)",
	})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Contains(t, uri, "fakturoid://?")
	assert.Contains(t, uri, "slug=acme")
}
