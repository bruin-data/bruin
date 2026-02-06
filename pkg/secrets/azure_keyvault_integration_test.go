//go:build integration

package secrets

import (
	"os"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestAzureKeyVaultIntegration(t *testing.T) {
	// Skip if not in integration test mode
	if os.Getenv("BRUIN_INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration test; set BRUIN_INTEGRATION_TESTS=true to run")
	}

	// Verify required env vars
	vaultURL := os.Getenv("BRUIN_AZURE_KEYVAULT_URL")
	if vaultURL == "" {
		t.Skip("BRUIN_AZURE_KEYVAULT_URL not set")
	}

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	t.Run("can create client from env with default credential", func(t *testing.T) {
		_ = os.Setenv("BRUIN_AZURE_AUTH_METHOD", "default")

		client, err := NewAzureKeyVaultClientFromEnv(sugar)
		require.NoError(t, err)
		require.NotNil(t, client)
	})

	t.Run("can fetch generic secret", func(t *testing.T) {
		// Requires a secret named "test-generic" in the vault with:
		// {"type": "generic", "details": {"value": "test-value"}}
		_ = os.Setenv("BRUIN_AZURE_AUTH_METHOD", "default")

		client, err := NewAzureKeyVaultClientFromEnv(sugar)
		require.NoError(t, err)

		conn := client.GetConnection("test-generic")
		if conn == nil {
			t.Skip("test-generic secret not found in vault")
		}

		gc, ok := conn.(*config.GenericConnection)
		require.True(t, ok)
		require.Equal(t, "test-value", gc.Value)
	})

	t.Run("can fetch postgres connection", func(t *testing.T) {
		// Requires a secret named "test-postgres" in the vault with proper format:
		// {"type": "postgres", "details": {"username": "user", "password": "pass", "host": "host", "port": 5432, "database": "db", "schema": "public"}}
		_ = os.Setenv("BRUIN_AZURE_AUTH_METHOD", "default")

		client, err := NewAzureKeyVaultClientFromEnv(sugar)
		require.NoError(t, err)

		conn := client.GetConnection("test-postgres")
		if conn == nil {
			t.Skip("test-postgres secret not found in vault")
		}
		require.NotNil(t, conn)
	})

	t.Run("can fetch connection details", func(t *testing.T) {
		_ = os.Setenv("BRUIN_AZURE_AUTH_METHOD", "default")

		client, err := NewAzureKeyVaultClientFromEnv(sugar)
		require.NoError(t, err)

		deets := client.GetConnectionDetails("test-generic")
		if deets == nil {
			t.Skip("test-generic secret not found in vault")
		}

		gc, ok := deets.(*config.GenericConnection)
		require.True(t, ok)
		require.Equal(t, "test-generic", gc.Name)
		require.Equal(t, "test-value", gc.Value)
	})
}
