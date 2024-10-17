package duckdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ToDBConnectionURI(t *testing.T) {
	t.Parallel()
	c := Config{
		Path: "/some/path/db.duckdb",
	}

	assert.Equal(t, "/some/path/db.duckdb", c.ToDBConnectionURI())
}

func TestConfig_ToIngestr(t *testing.T) {
	t.Parallel()
	c := Config{
		Path: "/some/path/db.duckdb",
	}

	assert.Equal(t, "duckdb:////some/path/db.duckdb", c.GetIngestrURI())
}
