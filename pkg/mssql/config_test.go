package mssql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ToDBConnectionURI(t *testing.T) {
	t.Parallel()

	t.Run("default parameters without custom query", func(t *testing.T) {
		c := Config{
			Username: "user",
			Password: "password",
			Host:     "localhost",
			Port:     1433,
			Database: "database",
		}

		uri := c.ToDBConnectionURI()
		assert.Contains(t, uri, "sqlserver://user:password@localhost:1433")
		assert.Contains(t, uri, "TrustServerCertificate=true")
		assert.Contains(t, uri, "encrypt=disable")
		assert.Contains(t, uri, "app+name=Bruin+CLI")
		assert.Contains(t, uri, "database=database")
	})

	t.Run("custom query parameters", func(t *testing.T) {
		c := Config{
			Username: "user",
			Password: "password",
			Host:     "localhost",
			Port:     1433,
			Database: "database",
			Query:    "encrypt=true&TrustServerCertificate=false",
		}

		uri := c.ToDBConnectionURI()
		assert.Contains(t, uri, "sqlserver://user:password@localhost:1433")
		assert.Contains(t, uri, "encrypt=true")
		assert.Contains(t, uri, "TrustServerCertificate=false")
		assert.Contains(t, uri, "database=database")
	})

	t.Run("custom query without database parameter", func(t *testing.T) {
		c := Config{
			Username: "user",
			Password: "password",
			Host:     "localhost",
			Port:     1433,
			Database: "database",
			Query:    "connection+timeout=30",
		}

		uri := c.ToDBConnectionURI()
		assert.Contains(t, uri, "sqlserver://user:password@localhost:1433")
		assert.Contains(t, uri, "connection+timeout=30")
		assert.Contains(t, uri, "database=database")
	})

	t.Run("custom query with database parameter already included", func(t *testing.T) {
		c := Config{
			Username: "user",
			Password: "password",
			Host:     "localhost",
			Port:     1433,
			Database: "database",
			Query:    "database=customdb&encrypt=true",
		}

		uri := c.ToDBConnectionURI()
		assert.Contains(t, uri, "sqlserver://user:password@localhost:1433")
		assert.Contains(t, uri, "database=customdb")
		// Should not add database parameter again
		assert.NotContains(t, uri, "database=database")
	})
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
