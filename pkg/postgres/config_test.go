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

	assert.Equal(t, "postgresql://user:password@localhost:5432/database?sslmode=disable", c.GetIngestrURI())
}

func TestConfig_ToIngestrCDC(t *testing.T) {
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
		CDC:          true,
		Publication:  "my_publication",
		Slot:         "my_slot",
		CDCMode:      "stream",
	}

	uri := c.GetIngestrURI()
	assert.Contains(t, uri, "postgres+cdc://")
	assert.Contains(t, uri, "user:password@localhost:5432/database")
	assert.Contains(t, uri, "publication=my_publication")
	assert.Contains(t, uri, "slot=my_slot")
	assert.Contains(t, uri, "mode=stream")
	assert.Contains(t, uri, "sslmode=disable")
}

func TestConfig_ToIngestrCDC_BatchMode(t *testing.T) {
	t.Parallel()
	c := Config{
		Username:    "user",
		Password:    "password",
		Host:        "localhost",
		Port:        5432,
		Database:    "database",
		CDC:         true,
		Publication: "pub1",
		Slot:        "slot1",
		CDCMode:     "batch",
	}

	uri := c.GetIngestrURI()
	assert.Contains(t, uri, "postgres+cdc://")
	assert.Contains(t, uri, "mode=batch")
}

func TestConfig_ToIngestrCDC_MinimalParams(t *testing.T) {
	t.Parallel()
	c := Config{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     5432,
		Database: "database",
		CDC:      true,
	}

	uri := c.GetIngestrURI()
	assert.Equal(t, "postgres+cdc://user:password@localhost:5432/database", uri)
}
