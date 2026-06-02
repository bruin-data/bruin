package flightsql

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
