package fabric

import (
	"testing"

	"github.com/microsoft/go-mssqldb/azuread"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	// fabricSparkConfig mirrors the Warehouse path's existing precedence.
	t.Run("azure default credential wins over service principal", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Host:                      "fabric.example",
			Database:                  "warehouse",
			UseAzureDefaultCredential: true,
			ClientID:                  "client-id",
			ClientSecret:              "secret",
			TenantID:                  "tenant-id",
		}

		uri := c.ToDBConnectionURI()
		assert.Contains(t, uri, "fedauth=ActiveDirectoryDefault")
		assert.NotContains(t, uri, "fedauth=ActiveDirectoryServicePrincipal")
	})
}

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	t.Run("service principal", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Host:         "myworkspace.datawarehouse.fabric.microsoft.com",
			Database:     "MyWarehouse",
			ClientID:     "client-id",
			ClientSecret: "secret",
			TenantID:     "tenant-id",
		}

		uri, err := c.GetIngestrURI()
		require.NoError(t, err)
		assert.Equal(t, "fabric://client-id:secret@myworkspace.datawarehouse.fabric.microsoft.com:1433/MyWarehouse?tenant_id=tenant-id", uri)
	})

	t.Run("service principal without secret", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Host:     "fabric.example",
			Port:     1433,
			Database: "warehouse",
			ClientID: "client-id",
			TenantID: "tenant-id",
		}

		uri, err := c.GetIngestrURI()
		require.NoError(t, err)
		assert.Equal(t, "fabric://client-id@fabric.example:1433/warehouse?tenant_id=tenant-id", uri)
	})

	t.Run("azure default credential", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Host:                      "fabric.example",
			Database:                  "warehouse",
			UseAzureDefaultCredential: true,
		}

		uri, err := c.GetIngestrURI()
		require.NoError(t, err)
		assert.Equal(t, "fabric://fabric.example:1433/warehouse?fedauth=ActiveDirectoryDefault", uri)
	})

	// Keep the existing ingestr precedence for connections that already set both
	// modes: explicit service-principal credentials win over ambient credentials.
	t.Run("service principal wins over azure default credential", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Host:                      "fabric.example",
			Database:                  "warehouse",
			UseAzureDefaultCredential: true,
			ClientID:                  "client-id",
			ClientSecret:              "secret",
			TenantID:                  "tenant-id",
		}

		uri, err := c.GetIngestrURI()
		require.NoError(t, err)
		assert.Equal(t, "fabric://client-id:secret@fabric.example:1433/warehouse?tenant_id=tenant-id", uri)
	})

	t.Run("sql auth is rejected", func(t *testing.T) {
		t.Parallel()
		c := Config{
			Username: "user",
			Password: "password",
			Host:     "localhost",
			Database: "warehouse",
		}

		_, err := c.GetIngestrURI()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Microsoft Entra ID authentication")
	})
}
