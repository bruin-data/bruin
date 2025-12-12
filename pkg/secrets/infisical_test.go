package secrets

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNewInfisicalClient(t *testing.T) {
	t.Parallel()
	log := &mockLogger{}

	t.Run("returns error if host is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewInfisicalClient(log, "", "clientID", "secret", "projectID", "dev", "/")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty infisical host")
	})

	t.Run("returns error if client ID is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewInfisicalClient(log, "https://app.infisical.com", "", "secret", "projectID", "dev", "/")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty infisical client ID")
	})

	t.Run("returns error if client secret is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewInfisicalClient(log, "https://app.infisical.com", "clientID", "", "projectID", "dev", "/")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty infisical client secret")
	})

	t.Run("returns error if project ID is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewInfisicalClient(log, "https://app.infisical.com", "clientID", "secret", "", "dev", "/")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty infisical project ID")
	})

	t.Run("returns error if environment is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewInfisicalClient(log, "https://app.infisical.com", "clientID", "secret", "projectID", "", "/")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty infisical environment")
	})
}

func TestInfisicalClient_GetConnection_ReturnsConnection(t *testing.T) {
	t.Parallel()

	// Note: This test would require proper mocking of the Infisical SDK
	// For now, we're testing the cache behavior only
	// Full integration tests would be done with actual Infisical instance

	// Skip for now - proper SDK mocking will be added in the future
	t.Skip("Requires proper Infisical SDK mocking")
}

func TestInfisicalClient_GetConnection_FromCache(t *testing.T) {
	t.Parallel()

	cachedConnection := []string{"some", "data", "not", "nil"}

	// Create a mock client (nil is fine since we're testing cache hit only)
	c := &InfisicalClient{
		client:           nil, // Won't be called due to cache hit
		projectID:        "test-project",
		environment:      "dev",
		secretPath:       "/",
		logger:           &mockLogger{},
		cacheConnections: map[string]any{"test-connection": cachedConnection},
	}

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
	require.Equal(t, cachedConnection, conn)
}

func TestInfisicalClient_GetConnectionDetails_FromCache(t *testing.T) {
	t.Parallel()

	cachedConnection := config.GenericConnection{
		Name:  "test-connection",
		Value: "cached-value",
	}
	c := &InfisicalClient{
		client:                  nil, // Won't be called due to cache hit
		projectID:               "test-project",
		environment:             "dev",
		secretPath:              "/",
		logger:                  &mockLogger{},
		cacheConnectionsDetails: map[string]any{"test-connection": &cachedConnection},
	}

	deets := c.GetConnectionDetails("test-connection")
	require.NotNil(t, deets)
	gc, ok := deets.(*config.GenericConnection)
	require.True(t, ok)
	require.Equal(t, "cached-value", gc.Value)
}

func TestInfisicalClient_SecretParsing(t *testing.T) {
	t.Parallel()

	t.Run("valid secret format parses correctly", func(t *testing.T) {
		t.Parallel()

		// Test secret format validation
		// This would normally test the parsing logic with proper mocking
		// Skipping for now - proper SDK mocking will be added in the future
		t.Skip("Requires proper Infisical SDK mocking")
	})
}
