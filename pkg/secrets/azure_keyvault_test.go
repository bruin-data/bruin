package secrets

import (
	"context"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/require"
)

// mockAzureKeyVaultClient implements azureKeyVaultSecretsClient for testing.
type mockAzureKeyVaultClient struct {
	response azsecrets.GetSecretResponse
	err      error
}

func (m *mockAzureKeyVaultClient) GetSecret(ctx context.Context, name string, version string, options *azsecrets.GetSecretOptions) (azsecrets.GetSecretResponse, error) {
	return m.response, m.err
}

func TestNewAzureKeyVaultClient(t *testing.T) {
	t.Parallel()
	log := &mockLogger{}

	t.Run("returns error if vault URL is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewAzureKeyVaultClient(log, "", "tenant", "client", "secret")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty Azure Key Vault URL")
	})

	t.Run("returns error if vault URL is invalid format", func(t *testing.T) {
		t.Parallel()
		client, err := NewAzureKeyVaultClient(log, "not-a-url", "tenant", "client", "secret")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "invalid Azure Key Vault URL")
	})

	t.Run("returns error if vault URL is not https", func(t *testing.T) {
		t.Parallel()
		client, err := NewAzureKeyVaultClient(log, "http://vault.vault.azure.net", "tenant", "client", "secret")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "invalid Azure Key Vault URL")
	})

	t.Run("returns error if vault URL is not azure.net domain", func(t *testing.T) {
		t.Parallel()
		client, err := NewAzureKeyVaultClient(log, "https://vault.example.com", "tenant", "client", "secret")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "invalid Azure Key Vault URL")
	})

	t.Run("returns error if tenant ID is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewAzureKeyVaultClient(log, "https://vault.vault.azure.net", "", "client", "secret")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "tenant ID required")
	})

	t.Run("returns error if client ID is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewAzureKeyVaultClient(log, "https://vault.vault.azure.net", "tenant", "", "secret")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "client ID required")
	})

	t.Run("returns error if client secret is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewAzureKeyVaultClient(log, "https://vault.vault.azure.net", "tenant", "client", "")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "client secret required")
	})
}

func TestAzureKeyVaultClient_GetConnection_ReturnsConnection(t *testing.T) {
	t.Parallel()
	secretValue := `{"details": {"username": "testuser", "password": "testpass", "host": "testhost", "port": 1337, "database": "testdb", "schema": "testschema"}, "type": "postgres"}`
	c := &AzureKeyVaultClient{
		client: &mockAzureKeyVaultClient{
			response: azsecrets.GetSecretResponse{
				Secret: azsecrets.Secret{
					Value: &secretValue,
				},
			},
			err: nil,
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
}

func TestAzureKeyVaultClient_GetConnection_ReturnsGenericConnection(t *testing.T) {
	t.Parallel()
	secretValue := `{"details": {"value": "somevalue"}, "type": "generic"}`
	c := &AzureKeyVaultClient{
		client: &mockAzureKeyVaultClient{
			response: azsecrets.GetSecretResponse{
				Secret: azsecrets.Secret{
					Value: &secretValue,
				},
			},
			err: nil,
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
	require.Equal(t, "somevalue", conn.(*config.GenericConnection).Value)
}

func TestAzureKeyVaultClient_GetConnection_ReturnsError(t *testing.T) {
	t.Parallel()
	c := &AzureKeyVaultClient{
		client: &mockAzureKeyVaultClient{
			response: azsecrets.GetSecretResponse{},
			err:      context.DeadlineExceeded,
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("missing-secret")
	require.Nil(t, conn)
}

func TestAzureKeyVaultClient_GetConnection_ReturnsFromCache(t *testing.T) {
	t.Parallel()
	c := &AzureKeyVaultClient{
		client: &mockAzureKeyVaultClient{
			response: azsecrets.GetSecretResponse{},
			err:      context.DeadlineExceeded,
		},
		logger:           &mockLogger{},
		cacheConnections: map[string]any{"test-connection": []string{"some", "data", "not", "nil"}},
	}

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
	require.Equal(t, []string{"some", "data", "not", "nil"}, conn)
}

func TestAzureKeyVaultClient_GetConnection_ThreadSafe(t *testing.T) {
	t.Parallel()
	secretValue := `{"details": {"value": "somevalue"}, "type": "generic"}`

	c := &AzureKeyVaultClient{
		client: &mockAzureKeyVaultClient{
			response: azsecrets.GetSecretResponse{
				Secret: azsecrets.Secret{
					Value: &secretValue,
				},
			},
			err: nil,
		},
		logger:                  &mockLogger{},
		cacheConnections:        make(map[string]any),
		cacheConnectionsDetails: make(map[string]any),
	}

	// Run concurrent access
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := c.GetConnection("test-connection")
			require.NotNil(t, conn)
		}()
	}
	wg.Wait()
}

func TestAzureKeyVaultClient_GetConnectionDetails_ReturnsDetails(t *testing.T) {
	t.Parallel()
	secretValue := `{"details": {"value": "somevalue"}, "type": "generic"}`
	c := &AzureKeyVaultClient{
		client: &mockAzureKeyVaultClient{
			response: azsecrets.GetSecretResponse{
				Secret: azsecrets.Secret{
					Value: &secretValue,
				},
			},
			err: nil,
		},
		logger:                  &mockLogger{},
		cacheConnectionsDetails: make(map[string]any),
	}

	deets := c.GetConnectionDetails("test-connection")
	require.NotNil(t, deets)
	gc, ok := deets.(*config.GenericConnection)
	require.True(t, ok)
	require.Equal(t, "test-connection", gc.Name)
	require.Equal(t, "somevalue", gc.Value)
}

func TestAzureKeyVaultClient_GetConnectionDetails_ReturnsFromCache(t *testing.T) {
	t.Parallel()
	c := &AzureKeyVaultClient{
		client: &mockAzureKeyVaultClient{
			err: nil,
		},
		logger: &mockLogger{},
		cacheConnectionsDetails: map[string]any{"test-connection": config.AthenaConnection{
			Name:      "test-connection",
			SecretKey: "test-secret-key",
		}},
	}

	deets := c.GetConnectionDetails("test-connection")
	require.NotNil(t, deets)
	require.Equal(
		t,
		config.AthenaConnection{
			Name:      "test-connection",
			SecretKey: "test-secret-key",
		},
		deets,
	)
}

func TestAzureKeyVaultClient_GetConnection_ReturnsErrorForNilSecretValue(t *testing.T) {
	t.Parallel()
	c := &AzureKeyVaultClient{
		client: &mockAzureKeyVaultClient{
			response: azsecrets.GetSecretResponse{
				Secret: azsecrets.Secret{
					Value: nil,
				},
			},
			err: nil,
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.Nil(t, conn)
}

func TestAzureKeyVaultClient_GetConnection_ReturnsErrorForInvalidJSON(t *testing.T) {
	t.Parallel()
	secretValue := "not valid json"
	c := &AzureKeyVaultClient{
		client: &mockAzureKeyVaultClient{
			response: azsecrets.GetSecretResponse{
				Secret: azsecrets.Secret{
					Value: &secretValue,
				},
			},
			err: nil,
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.Nil(t, conn)
}

func TestAzureKeyVaultClient_GetConnection_ReturnsErrorForMissingFields(t *testing.T) {
	t.Parallel()
	secretValue := `{"some": "data"}`
	c := &AzureKeyVaultClient{
		client: &mockAzureKeyVaultClient{
			response: azsecrets.GetSecretResponse{
				Secret: azsecrets.Secret{
					Value: &secretValue,
				},
			},
			err: nil,
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.Nil(t, conn)
}

func TestAzureKeyVaultClient_GetConnection_ReturnsErrorForDetailsNotMap(t *testing.T) {
	t.Parallel()
	secretValue := `{"details": "not-a-map", "type": "generic"}`
	c := &AzureKeyVaultClient{
		client: &mockAzureKeyVaultClient{
			response: azsecrets.GetSecretResponse{
				Secret: azsecrets.Secret{
					Value: &secretValue,
				},
			},
			err: nil,
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.Nil(t, conn)
}

func TestAzureKeyVaultClient_GetConnection_ReturnsErrorForEmptyType(t *testing.T) {
	t.Parallel()
	secretValue := `{"details": {"value": "test"}, "type": ""}`
	c := &AzureKeyVaultClient{
		client: &mockAzureKeyVaultClient{
			response: azsecrets.GetSecretResponse{
				Secret: azsecrets.Secret{
					Value: &secretValue,
				},
			},
			err: nil,
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.Nil(t, conn)
}
