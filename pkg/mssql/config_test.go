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
		Database: "database",
	}

	assert.Equal(t, "sqlserver://user:password@localhost:1433?app+name=Bruin+CLI&database=database", c.ToDBConnectionURI())
}

func TestConfig_ToIngestr(t *testing.T) {
	t.Parallel()
	c := Config{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     1433,
		Database: "database",
	}

	assert.Equal(t, "mssql://user:password@localhost:1433/database?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server", c.GetIngestrURI())

	c = Config{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Port:     1433,
		Database: "database",
		Query:    "TrustServerCertificate=yes&driver=ODBC+Driver+17+for+SQL+Server",
	}

	assert.Equal(t, "mssql://user:password@localhost:1433/database?TrustServerCertificate=yes&driver=ODBC+Driver+17+for+SQL+Server", c.GetIngestrURI())
}
