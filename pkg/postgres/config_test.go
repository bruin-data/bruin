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

func TestConfig_ToDBConnectionURI_WithReadOnly(t *testing.T) {
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
		ReadOnly:     true,
	}

	expected := "postgres://user:password@localhost:5432/database?sslmode=disable&pool_max_conns=10&search_path=schema&options=-c+default_transaction_read_only%3Don"
	assert.Equal(t, expected, c.ToDBConnectionURI())
}

func TestConfig_ToDBConnectionURI_ReadOnlyWithoutSchema(t *testing.T) {
	t.Parallel()
	c := Config{
		Username:     "user",
		Password:     "password",
		Host:         "localhost",
		Port:         5432,
		Database:     "database",
		PoolMaxConns: 10,
		SslMode:      "disable",
		ReadOnly:     true,
	}

	expected := "postgres://user:password@localhost:5432/database?sslmode=disable&pool_max_conns=10&options=-c+default_transaction_read_only%3Don"
	assert.Equal(t, expected, c.ToDBConnectionURI())
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

func TestConfig_GetIngestrURI_WithReadOnly(t *testing.T) {
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
		ReadOnly:     true,
	}

	expected := "postgresql://user:password@localhost:5432/database?sslmode=disable&options=-c+default_transaction_read_only%3Don"
	assert.Equal(t, expected, c.GetIngestrURI())
}

func TestConfig_GetIngestrURI_ReadOnlyWithoutSSL(t *testing.T) {
	t.Parallel()
	c := Config{
		Username:     "user",
		Password:     "password",
		Host:         "localhost",
		Port:         5432,
		Database:     "database",
		PoolMaxConns: 10,
		ReadOnly:     true,
	}

	expected := "postgresql://user:password@localhost:5432/database?options=-c+default_transaction_read_only%3Don"
	assert.Equal(t, expected, c.GetIngestrURI())
}

func TestRedShiftConfig_ToDBConnectionURI_WithReadOnly(t *testing.T) {
	t.Parallel()
	c := RedShiftConfig{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     5439,
		Database: "database",
		Schema:   "schema",
		SslMode:  "require",
		ReadOnly: true,
	}

	expected := "postgres://user:password@localhost:5439/database?sslmode=require&search_path=schema&options=-c+default_transaction_read_only%3Don"
	assert.Equal(t, expected, c.ToDBConnectionURI())
}

func TestRedShiftConfig_GetIngestrURI_WithReadOnly(t *testing.T) {
	t.Parallel()
	c := RedShiftConfig{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     5439,
		Database: "database",
		SslMode:  "require",
		ReadOnly: true,
	}

	expected := "redshift://user:password@localhost:5439/database?sslmode=require&options=-c+default_transaction_read_only%3Don"
	assert.Equal(t, expected, c.GetIngestrURI())
}
