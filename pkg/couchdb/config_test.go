package couchdb

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
		wantErr string
	}{
		{
			name: "with credentials and port",
			config: Config{
				Username: "admin",
				Password: "password",
				Host:     "localhost",
				Port:     5984,
			},
			want: "couchdb://admin:password@localhost:5984",
		},
		{
			name: "without port",
			config: Config{
				Username: "admin",
				Password: "password",
				Host:     "localhost",
			},
			want: "couchdb://admin:password@localhost",
		},
		{
			name: "anonymous",
			config: Config{
				Host: "localhost",
				Port: 5984,
			},
			want: "couchdb://localhost:5984",
		},
		{
			name: "ssl",
			config: Config{
				Username: "admin",
				Password: "password",
				Host:     "couchdb.example.com",
				SSL:      true,
			},
			want: "couchdb+https://admin:password@couchdb.example.com",
		},
		{
			name: "special characters in credentials are escaped",
			config: Config{
				Username: "user@corp",
				Password: "p@ss/w#rd?",
				Host:     "localhost",
				Port:     5984,
			},
			want: "couchdb://user%40corp:p%40ss%2Fw%23rd%3F@localhost:5984",
		},
		{
			name: "ipv6 host",
			config: Config{
				Host: "::1",
				Port: 5984,
			},
			want: "couchdb://[::1]:5984",
		},
		{
			name: "ipv6 host without port",
			config: Config{
				Host: "::1",
			},
			want: "couchdb://[::1]",
		},
		{
			name: "bracketed ipv6 host with port",
			config: Config{
				Host: "[::1]",
				Port: 5984,
			},
			want: "couchdb://[::1]:5984",
		},
		{
			name: "bracketed ipv6 host without port",
			config: Config{
				Host: "[2001:db8::1]",
				SSL:  true,
			},
			want: "couchdb+https://[2001:db8::1]",
		},
		{
			name:    "missing host",
			config:  Config{Username: "admin", Password: "password"},
			wantErr: "couchdb: host must be provided",
		},
		{
			name:    "blank host",
			config:  Config{Host: "   "},
			wantErr: "couchdb: host must be provided",
		},
		{
			name:    "invalid port",
			config:  Config{Host: "localhost", Port: -1},
			wantErr: "couchdb: port must be between 1 and 65535, got -1",
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
