package odoo

import (
	"net/url"
	"testing"

	"github.com/bruin-data/bruin/pkg/mask"
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
			name: "surrounding whitespace is trimmed except in the api key",
			config: Config{
				Host:     "  mycompany.odoo.com  ",
				APIKey:   " key_123 ",
				Database: " mycompany ",
				Scheme:   " https ",
			},
			want: "odoo://mycompany.odoo.com?api_key=+key_123+&database=mycompany&scheme=https",
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
			name: "port",
			config: Config{
				Host:   "erp.example.com",
				Port:   8443,
				APIKey: "key_123",
			},
			want: "odoo://erp.example.com:8443?api_key=key_123",
		},
		{
			name: "port goes before the path prefix",
			config: Config{
				Host:   "erp.example.com/odoo/",
				Port:   8443,
				APIKey: "key_123",
			},
			want: "odoo://erp.example.com:8443/odoo?api_key=key_123",
		},
		{
			name: "port with an IPv6 host",
			config: Config{
				Host:   "::1",
				Port:   8069,
				APIKey: "key_123",
			},
			want: "odoo://[::1]:8069?api_key=key_123",
		},
		{
			name: "port with a bracketed IPv6 host and path prefix",
			config: Config{
				Host:   "[2001:db8::1]/odoo",
				Port:   8069,
				APIKey: "key_123",
			},
			want: "odoo://[2001:db8::1]:8069/odoo?api_key=key_123",
		},
		{
			name: "bracketed IPv6 host with its own port",
			config: Config{
				Host:   "[2001:db8::1]:8069",
				APIKey: "key_123",
			},
			want: "odoo://[2001:db8::1]:8069?api_key=key_123",
		},
		{
			name: "port in host with path prefix",
			config: Config{
				Host:   "erp.example.com:8069/odoo",
				APIKey: "key_123",
			},
			want: "odoo://erp.example.com:8069/odoo?api_key=key_123",
		},
		{
			name: "highest port",
			config: Config{
				Host:   "erp.example.com",
				Port:   65535,
				APIKey: "key_123",
			},
			want: "odoo://erp.example.com:65535?api_key=key_123",
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
			name:    "blank api key",
			config:  Config{Host: "mycompany.odoo.com", APIKey: "   "},
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
		{
			name:    "port in both host and port",
			config:  Config{Host: "erp.example.com:8069", Port: 8443, APIKey: "key_123"},
			wantErr: `odoo: host "erp.example.com:8069" already includes a port; set the port in either host or port, not both`,
		},
		{
			name:    "port in both host with path prefix and port",
			config:  Config{Host: "erp.example.com:8069/odoo", Port: 8443, APIKey: "key_123"},
			wantErr: `odoo: host "erp.example.com:8069/odoo" already includes a port; set the port in either host or port, not both`,
		},
		{
			name:    "port in both bracketed IPv6 host and port",
			config:  Config{Host: "[::1]:8069", Port: 8443, APIKey: "key_123"},
			wantErr: `odoo: host "[::1]:8069" already includes a port; set the port in either host or port, not both`,
		},
		{
			name:    "malformed bracketed IPv6 host with port",
			config:  Config{Host: "[::1", Port: 8443, APIKey: "key_123"},
			wantErr: `odoo: host "[::1" is not a valid bracketed IPv6 address`,
		},
		{
			name:    "negative port",
			config:  Config{Host: "erp.example.com", Port: -1, APIKey: "key_123"},
			wantErr: "odoo: port must be between 1 and 65535, got -1",
		},
		{
			name:    "port above the maximum",
			config:  Config{Host: "erp.example.com", Port: 65536, APIKey: "key_123"},
			wantErr: "odoo: port must be between 1 and 65535, got 65536",
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

// TestConfig_GetIngestrURI_PortRoundTrip checks that a separate port lands in the authority, where ingestr
// reads it as part of the server address.
func TestConfig_GetIngestrURI_PortRoundTrip(t *testing.T) {
	t.Parallel()

	c := Config{Host: "erp.example.com/odoo", Port: 8443, APIKey: "key_123"}

	uri, err := c.GetIngestrURI()
	require.NoError(t, err)

	parsed, err := url.Parse(uri)
	require.NoError(t, err)
	assert.Equal(t, "erp.example.com:8443", parsed.Host)
	assert.Equal(t, "8443", parsed.Port())
	assert.Equal(t, "/odoo", parsed.Path)
}

// TestConfig_GetIngestrURI_APIKeyIsMasked checks that the api_key in the URI is redacted by the masker
// that bruin builds from the configured value, including values with surrounding whitespace.
func TestConfig_GetIngestrURI_APIKeyIsMasked(t *testing.T) {
	t.Parallel()

	for _, apiKey := range []string{"key_123", " key_123 ", "a+b/c=d&e?f#g %"} {
		c := Config{Host: "mycompany.odoo.com", APIKey: apiKey, Database: "mycompany"}

		uri, err := c.GetIngestrURI()
		require.NoError(t, err)

		masked := mask.New([]string{apiKey}).Mask(uri)
		assert.NotContains(t, masked, "key_123", apiKey)
		assert.NotContains(t, masked, url.QueryEscape(apiKey), apiKey)
		assert.Contains(t, masked, "database=mycompany", apiKey)
	}
}
