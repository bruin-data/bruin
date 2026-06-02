package flightsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ToDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   Config
		expected string
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.config.ToDSN())
		})
	}
}

func TestConfig_GetDatabase(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "analytics", Config{Database: "analytics"}.GetDatabase())
	assert.Empty(t, Config{}.GetDatabase())
}
