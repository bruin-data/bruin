package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ToDBConnectionURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		readOnly bool
		expected string
	}{
		{
			name:     "read-write",
			readOnly: false,
			expected: "postgres://user:password@localhost:5432/database?sslmode=disable&pool_max_conns=10&search_path=schema",
		},
		{
			name:     "read-only",
			readOnly: true,
			expected: "postgres://user:password@localhost:5432/database?sslmode=disable&pool_max_conns=10&search_path=schema&options=-c+default_transaction_read_only%3Don",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := Config{
				Username:     "user",
				Password:     "password",
				Host:         "localhost",
				Port:         5432,
				Database:     "database",
				Schema:       "schema",
				PoolMaxConns: 10,
				SslMode:      "disable",
				ReadOnly:     tt.readOnly,
			}

			assert.Equal(t, tt.expected, c.ToDBConnectionURI())
		})
	}
}

func TestConfig_ToIngestr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		readOnly bool
		expected string
	}{
		{
			name:     "read-write",
			readOnly: false,
			expected: "postgresql://user:password@localhost:5432/database?sslmode=disable",
		},
		{
			name:     "read-only",
			readOnly: true,
			expected: "postgresql://user:password@localhost:5432/database?sslmode=disable&options=-c+default_transaction_read_only%3Don",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := Config{
				Username:     "user",
				Password:     "password",
				Host:         "localhost",
				Port:         5432,
				Database:     "database",
				Schema:       "schema",
				PoolMaxConns: 10,
				SslMode:      "disable",
				ReadOnly:     tt.readOnly,
			}

			assert.Equal(t, tt.expected, c.GetIngestrURI())
		})
	}
}
