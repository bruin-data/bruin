package mssql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ToDBConnectionURI(t *testing.T) {
	t.Parallel()
	c := Config{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     1433,
		Instance: "instance",
		Database: "database",
	}

	assert.Equal(t, "sqlserver://user:password@localhost:1433/instance?app+name=Bruin&database=database", c.ToDBConnectionURI())
}
