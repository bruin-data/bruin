package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ToDBConnectionURI(t *testing.T) {
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
	}

	assert.Equal(t, "postgres://user:password@localhost:5432/database?sslmode=disable&pool_max_conns=10&search_path=schema", c.ToDBConnectionURI())
}

func TestConfig_ToIngestr(t *testing.T) {
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
	}

	assert.Equal(t, "postgresql://user:password@localhost:5432/database?sslmode=disable", c.ToIngestrUrl())
}
