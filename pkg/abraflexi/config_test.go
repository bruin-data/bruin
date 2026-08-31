package abraflexi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intPtr(v int) *int           { return &v }
func floatPtr(v float64) *float64 { return &v }
func boolPtr(v bool) *bool        { return &v }

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
				Host:     "example.flexibee.eu",
				Username: "api-user",
				Password: "secret",
				Company:  "acme_s_r_o_",
			},
			want: "abra://example.flexibee.eu?company=acme_s_r_o_&password=secret&username=api-user",
		},
		{
			name: "all optional parameters",
			config: Config{
				Host:             "example.flexibee.eu",
				Username:         "api-user",
				Password:         "secret",
				Company:          "acme_s_r_o_",
				Scheme:           "https",
				PageSize:         intPtr(500),
				RateLimit:        floatPtr(2),
				IncludeExpensive: boolPtr(false),
			},
			want: "abra://example.flexibee.eu?company=acme_s_r_o_&include_expensive=false&page_size=500&password=secret&rate_limit=2&scheme=https&username=api-user",
		},
		{
			name: "self-hosted install with a url path prefix",
			config: Config{
				Host:     "erp.example.com",
				Path:     "flexi",
				Username: "api-user",
				Password: "secret",
				Company:  "acme_s_r_o_",
			},
			want: "abra://erp.example.com/flexi?company=acme_s_r_o_&password=secret&username=api-user",
		},
		{
			name: "url path prefix keeps its leading slash and drops a trailing slash",
			config: Config{
				Host:     "erp.example.com",
				Path:     "/flexi/",
				Username: "api-user",
				Password: "secret",
				Company:  "acme_s_r_o_",
			},
			want: "abra://erp.example.com/flexi?company=acme_s_r_o_&password=secret&username=api-user",
		},
		{
			name: "password with reserved characters is escaped",
			config: Config{
				Host:     "example.flexibee.eu",
				Username: "api-user",
				Password: "p@ss&word",
				Company:  "acme_s_r_o_",
			},
			want: "abra://example.flexibee.eu?company=acme_s_r_o_&password=p%40ss%26word&username=api-user",
		},
		{
			name: "values are trimmed",
			config: Config{
				Host:     " example.flexibee.eu ",
				Username: " api-user ",
				Password: " secret ",
				Company:  " acme_s_r_o_ ",
			},
			want: "abra://example.flexibee.eu?company=acme_s_r_o_&password=secret&username=api-user",
		},
		{
			name:    "missing host",
			config:  Config{Username: "api-user", Password: "secret", Company: "acme_s_r_o_"},
			wantErr: true,
		},
		{
			name:    "missing username",
			config:  Config{Host: "example.flexibee.eu", Password: "secret", Company: "acme_s_r_o_"},
			wantErr: true,
		},
		{
			name:    "missing password",
			config:  Config{Host: "example.flexibee.eu", Username: "api-user", Company: "acme_s_r_o_"},
			wantErr: true,
		},
		{
			name:    "missing company",
			config:  Config{Host: "example.flexibee.eu", Username: "api-user", Password: "secret"},
			wantErr: true,
		},
		{
			name: "non-positive page size",
			config: Config{
				Host: "example.flexibee.eu", Username: "api-user", Password: "secret",
				Company: "acme_s_r_o_", PageSize: intPtr(-1),
			},
			wantErr: true,
		},
		{
			name: "non-positive rate limit",
			config: Config{
				Host: "example.flexibee.eu", Username: "api-user", Password: "secret",
				Company: "acme_s_r_o_", RateLimit: floatPtr(0),
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

func TestNewClient(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{
		Host: "example.flexibee.eu", Username: "api-user", Password: "secret", Company: "acme_s_r_o_",
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "abra://example.flexibee.eu?company=acme_s_r_o_&password=secret&username=api-user", uri)
}
