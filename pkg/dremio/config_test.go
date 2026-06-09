package dremio

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_ToDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   Config
		expected string
		wantErr  bool
	}{
		{
			name: "host port and credentials",
			config: Config{
				Host:     "dremio.example.com",
				Port:     32010,
				Username: "admin",
				Password: "secret",
			},
			expected: "uri=grpc+tcp://dremio.example.com:32010;username=admin;password=secret",
		},
		{
			name: "no password",
			config: Config{
				Host:     "localhost",
				Port:     32010,
				Username: "admin",
			},
			expected: "uri=grpc+tcp://localhost:32010;username=admin",
		},
		{
			name: "uri only",
			config: Config{
				Host: "localhost",
				Port: 32010,
			},
			expected: "uri=grpc+tcp://localhost:32010",
		},
		{
			name: "password with '=' is kept verbatim",
			config: Config{
				Host:     "localhost",
				Port:     32010,
				Username: "admin",
				Password: "p@ss=word",
			},
			expected: "uri=grpc+tcp://localhost:32010;username=admin;password=p@ss=word",
		},
		{
			name: "semicolon in password is rejected",
			config: Config{
				Host:     "localhost",
				Port:     32010,
				Username: "admin",
				Password: "p@ss;word",
			},
			wantErr: true,
		},
		{
			name: "token auth over TLS (Dremio Cloud)",
			config: Config{
				Host:  "data.dremio.cloud",
				Port:  443,
				Token: "pat123",
				TLS:   true,
			},
			expected: "uri=grpc+tls://data.dremio.cloud:443;adbc.flight.sql.authorization_header=Bearer pat123",
		},
		{
			name: "tls with skip verify",
			config: Config{
				Host:          "localhost",
				Port:          32010,
				Username:      "admin",
				Password:      "secret",
				TLS:           true,
				TLSSkipVerify: true,
			},
			expected: "uri=grpc+tls://localhost:32010;username=admin;password=secret;adbc.flight.sql.client_option.tls_skip_verify=true",
		},
		{
			name: "token and password together is rejected",
			config: Config{
				Host:     "localhost",
				Port:     32010,
				Username: "admin",
				Password: "secret",
				Token:    "pat123",
			},
			wantErr: true,
		},
		{
			name: "semicolon in token is rejected",
			config: Config{
				Host:  "localhost",
				Port:  32010,
				Token: "pat;123",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dsn, err := tt.config.ToDSN()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, dsn)
		})
	}
}

func TestConfig_GetDatabase(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "analytics", Config{Database: "analytics"}.GetDatabase())
	assert.Empty(t, Config{}.GetDatabase())
}
