package fabric_warehouse

import (
	"testing"

	"github.com/microsoft/go-mssqldb/azuread"
	"github.com/stretchr/testify/assert"
)

func TestConfig_ToDBConnectionURI(t *testing.T) {
	t.Parallel()

	t.Run("default parameters", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Username: "user",
			Password: "password",
			Host:     "localhost",
			Port:     1433,
			Database: "warehouse",
		}

		uri := c.ToDBConnectionURI()
		assert.Contains(t, uri, "sqlserver://user:password@localhost:1433")
		assert.Contains(t, uri, "encrypt=true")
		assert.Contains(t, uri, "TrustServerCertificate=false")
		assert.Contains(t, uri, "app+name=Bruin+CLI")
		assert.Contains(t, uri, "database=warehouse")
		assert.Equal(t, "sqlserver", c.DriverName())
	})

	t.Run("azure default credential", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Host:                      "fabric.example",
			Database:                  "warehouse",
			UseAzureDefaultCredential: true,
		}

		uri := c.ToDBConnectionURI()
		assert.Contains(t, uri, "sqlserver://fabric.example:1433")
		assert.Contains(t, uri, "fedauth=ActiveDirectoryDefault")
		assert.Equal(t, azuread.DriverName, c.DriverName())
	})

	t.Run("service principal", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Host:         "fabric.example",
			Database:     "warehouse",
			ClientID:     "client-id",
			ClientSecret: "secret",
			TenantID:     "tenant-id",
		}

		uri := c.ToDBConnectionURI()
		assert.Contains(t, uri, "sqlserver://client-id%40tenant-id:secret@fabric.example:1433")
		assert.Contains(t, uri, "fedauth=ActiveDirectoryServicePrincipal")
		assert.Equal(t, azuread.DriverName, c.DriverName())
	})
}

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()
	c := Config{
		Username: "user",
		Password: "password",
		Host:     "localhost",
		Database: "warehouse",
	}

	assert.Equal(t, "fabric://?server=localhost&database=warehouse&authentication=sql&username=user&password=password", c.GetIngestrURI())
}
