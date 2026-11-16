package odoo

import (
	"net/url"
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
		wantErr string
	}{
		{
			name: "required fields only",
			config: Config{
				Host:   "mycompany.odoo.com",
				APIKey: "key_123",
			},
			want: "odoo://mycompany.odoo.com?api_key=key_123",
		},
		{
			name: "all fields",
			config: Config{
				Host:     "erp.example.com:8069",
				APIKey:   "key_123",
				Database: "mycompany",
				Username: "me@example.com",
				Scheme:   "http",
				API:      "jsonrpc",
				PageSize: 500,
			},
			want: "odoo://erp.example.com:8069?api=jsonrpc&api_key=key_123&database=mycompany&page_size=500&scheme=http&username=me%40example.com",
		},
		{
			name: "special characters in query values are escaped",
			config: Config{
				Host:     "mycompany.odoo.com",
				APIKey:   "a+b/c=d&e?f#g",
				Database: "my db",
				Username: "me+odoo@example.com",
			},
			want: "odoo://mycompany.odoo.com?api_key=a%2Bb%2Fc%3Dd%26e%3Ff%23g&database=my+db&username=me%2Bodoo%40example.com",
		},
		{
			name: "host with path prefix",
			config: Config{
				Host:   "example.com/odoo/",
				APIKey: "key_123",
				API:    "json2",
			},
			want: "odoo://example.com/odoo?api=json2&api_key=key_123",
		},
		{
			name: "surrounding whitespace is trimmed",
			config: Config{
				Host:     "  mycompany.odoo.com  ",
				APIKey:   " key_123 ",
				Database: " mycompany ",
				Scheme:   " https ",
			},
			want: "odoo://mycompany.odoo.com?api_key=key_123&database=mycompany&scheme=https",
		},
		{
			name: "auto api",
			config: Config{
				Host:   "mycompany.odoo.com",
				APIKey: "key_123",
				API:    "auto",
			},
			want: "odoo://mycompany.odoo.com?api=auto&api_key=key_123",
		},
		{
			name:    "missing host",
			config:  Config{APIKey: "key_123"},
			wantErr: "odoo: host must be provided",
		},
		{
			name:    "blank host",
			config:  Config{Host: "   ", APIKey: "key_123"},
			wantErr: "odoo: host must be provided",
		},
		{
			name:    "host with scheme",
			config:  Config{Host: "https://mycompany.odoo.com", APIKey: "key_123"},
			wantErr: `odoo: host must not include a scheme, got "https://mycompany.odoo.com"; set scheme to http or https instead`,
		},
		{
			name:    "missing api key",
			config:  Config{Host: "mycompany.odoo.com"},
			wantErr: "odoo: api_key must be provided",
		},
		{
			name:    "invalid scheme",
			config:  Config{Host: "mycompany.odoo.com", APIKey: "key_123", Scheme: "ftp"},
			wantErr: `odoo: scheme must be https or http, got "ftp"`,
		},
		{
			name:    "invalid api",
			config:  Config{Host: "mycompany.odoo.com", APIKey: "key_123", API: "xmlrpc"},
			wantErr: `odoo: api must be one of auto, json2, jsonrpc, got "xmlrpc"`,
		},
		{
			name:    "jsonrpc without username",
			config:  Config{Host: "mycompany.odoo.com", APIKey: "key_123", API: "jsonrpc", Database: "mycompany"},
			wantErr: "odoo: database and username are required for the jsonrpc API (Odoo 18 and older)",
		},
		{
			name:    "negative page size",
			config:  Config{Host: "mycompany.odoo.com", APIKey: "key_123", PageSize: -1},
			wantErr: "odoo: page_size must be between 1 and 5000, got -1",
		},
		{
			name:    "page size above the maximum",
			config:  Config{Host: "mycompany.odoo.com", APIKey: "key_123", PageSize: 5001},
			wantErr: "odoo: page_size must be between 1 and 5000, got 5001",
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

	c := Config{
		Host:     "erp.example.com:8069",
		APIKey:   "a+b/c=d&e?f#g %",
		Database: "my db",
		Username: "me+odoo@example.com",
	}

	uri, err := c.GetIngestrURI()
	require.NoError(t, err)

	parsed, err := url.Parse(uri)
	require.NoError(t, err)
	assert.Equal(t, "odoo", parsed.Scheme)
	assert.Equal(t, "erp.example.com:8069", parsed.Host)

	q := parsed.Query()
	assert.Equal(t, c.APIKey, q.Get("api_key"))
	assert.Equal(t, c.Database, q.Get("database"))
	assert.Equal(t, c.Username, q.Get("username"))
	assert.False(t, q.Has("scheme"))
	assert.False(t, q.Has("api"))
	assert.False(t, q.Has("page_size"))
}
