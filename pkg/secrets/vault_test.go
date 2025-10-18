package secrets

import (
	"context"
	"testing"

	"github.com/hashicorp/vault-client-go"
	"github.com/hashicorp/vault-client-go/schema"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/logger"
)

type mockLogger struct{}

func (m *mockLogger) Error(args ...any)                       {}
func (m *mockLogger) Info(args ...any)                        {}
func (m *mockLogger) Debug(args ...any)                       {}
func (m *mockLogger) Warn(args ...any)                        {}
func (m *mockLogger) With(args ...any) logger.Logger          { return m }
func (m *mockLogger) Debugf(format string, args ...any)       {}
func (m *mockLogger) Debugw(msg string, keysAndValues ...any) {}
func (m *mockLogger) Warnf(format string, args ...any)        {}

func TestNewVaultClient(t *testing.T) {
	t.Parallel()
	log := &mockLogger{}

	t.Run("returns error if host is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewVaultClient(log, "", "token", "role", "path", "mount")
		require.Error(t, err)
		require.Nil(t, client)
	})

	t.Run("returns error if path is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewVaultClient(log, "host", "token", "role", "", "mount")
		require.Error(t, err)
		require.Nil(t, client)
	})

	t.Run("returns error if mountPath is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewVaultClient(log, "host", "token", "role", "path", "")
		require.Error(t, err)
		require.Nil(t, client)
	})

	t.Run("returns error if no credentials provided", func(t *testing.T) {
		t.Parallel()
		client, err := NewVaultClient(log, "host", "", "", "path", "mount")
		require.Error(t, err)
		require.Nil(t, client)
	})
}

// Create a mock vault client that implements kvV2Reader
// and returns a mock *vault.Response[schema.KvV2ReadResponse].
type mockVaultClient struct {
	response *vault.Response[schema.KvV2ReadResponse]
	err      error
}

func (m *mockVaultClient) KvV2Read(ctx context.Context, path string, opts ...vault.RequestOption) (*vault.Response[schema.KvV2ReadResponse], error) {
	return m.response, m.err
}

// Additional tests for newVaultClientWithToken and newVaultClientWithKubernetesAuth would require
// interface abstraction or more advanced mocking, which is not shown here.
func TestClient_GetConnection_ReturnsConnection(t *testing.T) {
	t.Parallel()
	c := &Client{
		client: &mockVaultClient{
			response: &vault.Response[schema.KvV2ReadResponse]{
				Data: schema.KvV2ReadResponse{
					Data: map[string]any{
						"details": map[string]any{
							"username": "testuser",
							"password": "testpass",
							"host":     "testhost",
							"port":     1337,
							"database": "testdb",
							"schema":   "testschema",
						},
						"type": "postgres",
					},
				},
			},
			err: nil,
		},
		mountPath:        "mount",
		path:             "path",
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
}

func TestClient_GetConnection_ReturnsGenericConnection(t *testing.T) {
	t.Parallel()
	c := &Client{
		client: &mockVaultClient{
			response: &vault.Response[schema.KvV2ReadResponse]{
				Data: schema.KvV2ReadResponse{
					Data: map[string]any{
						"details": map[string]any{
							"value": "somevalue",
						},
						"type": "generic",
					},
				},
			},
			err: nil,
		},
		mountPath:        "mount",
		path:             "path",
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
	require.Equal(t, "somevalue", conn.(*config.GenericConnection).Value)
}

// Additional tests for newVaultClientWithToken and newVaultClientWithKubernetesAuth would require
// interface abstraction or more advanced mocking, which is not shown here.
func TestClient_GetConnection_ReturnsConnection_FromCache(t *testing.T) {
	t.Parallel()
	c := &Client{
		client: &mockVaultClient{
			response: nil,
			err:      errors.New("test error"), // This error should not be returned
		},
		mountPath:        "mount",
		path:             "path",
		logger:           &mockLogger{},
		cacheConnections: map[string]any{"test-connection": []string{"some", "data", "not", "nil"}},
	}

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
	require.Equal(t, []string{"some", "data", "not", "nil"}, conn)
}

func TestClient_GetConnectionDetails_ReturnsDetails(t *testing.T) {
	t.Parallel()
	c := &Client{
		client: &mockVaultClient{
			response: &vault.Response[schema.KvV2ReadResponse]{
				Data: schema.KvV2ReadResponse{
					Data: map[string]any{
						"details": map[string]any{
							"value": "somevalue",
						},
						"type": "generic",
					},
				},
			},
			err: nil,
		},
		mountPath:               "mount",
		path:                    "path",
		logger:                  &mockLogger{},
		cacheConnectionsDetails: make(map[string]any),
	}

	// First call should fetch and cache the details
	deets := c.GetConnectionDetails("test-connection")
	require.NotNil(t, deets)
	gc, ok := deets.(*config.GenericConnection)
	require.True(t, ok)
	require.Equal(t, "test-connection", gc.Name)
	require.Equal(t, "somevalue", gc.Value)
}

func TestClient_GetConnectionDetails_ReturnsDetails_FromCache(t *testing.T) {
	t.Parallel()
	c := &Client{
		client: &mockVaultClient{
			err: nil,
		},
		mountPath: "mount",
		path:      "path",
		logger:    &mockLogger{},
		cacheConnectionsDetails: map[string]any{"test-connection": config.AthenaConnection{
			Name:      "test-connection",
			SecretKey: "test-secret-key",
		}},
	}

	// First call should fetch and cache the details
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
