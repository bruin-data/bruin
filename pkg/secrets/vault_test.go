package secrets

import (
	"testing"

	"context"

	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/hashicorp/vault-client-go"
	"github.com/hashicorp/vault-client-go/schema"
	"github.com/stretchr/testify/assert"
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
	log := &mockLogger{}

	t.Run("returns nil if host is empty", func(t *testing.T) {
		client, err := NewVaultClient(log, "", "token", "role", "path", "mount")
		assert.NoError(t, err)
		assert.Nil(t, client)
	})

	t.Run("returns error if path is empty", func(t *testing.T) {
		client, err := NewVaultClient(log, "host", "token", "role", "", "mount")
		assert.Error(t, err)
		assert.Nil(t, client)
	})

	t.Run("returns error if mountPath is empty", func(t *testing.T) {
		client, err := NewVaultClient(log, "host", "token", "role", "path", "")
		assert.Error(t, err)
		assert.Nil(t, client)
	})

	t.Run("returns error if no credentials provided", func(t *testing.T) {
		client, err := NewVaultClient(log, "host", "", "", "path", "mount")
		assert.Error(t, err)
		assert.Nil(t, client)
	})
}

// Example stub for GetConnection, as full test would require heavy mocking of vault.Client
func TestClient_GetConnection_NilClient(t *testing.T) {
	c := &Client{client: nil, logger: &mockLogger{}}
	conn := c.GetConnection("test")
	assert.Nil(t, conn)
}

// Create a mock vault client that implements kvV2Reader
// and returns a mock *vault.Response[schema.KvV2ReadResponse]
type mockVaultClient struct{}

func (m *mockVaultClient) KvV2Read(ctx context.Context, path string, opts ...vault.RequestOption) (*vault.Response[schema.KvV2ReadResponse], error) {
	return &vault.Response[schema.KvV2ReadResponse]{
		Data: schema.KvV2ReadResponse{
			Data: map[string]any{
				"details": map[string]any{
					"username": "testuser",
					"password": "testpass",
				},
				"type": "test_type",
			},
		},
	}, nil
}

// Additional tests for newVaultClientWithToken and newVaultClientWithKubernetesAuth would require
// interface abstraction or more advanced mocking, which is not shown here.
func TestClient_GetConnection_ReturnsConnection(t *testing.T) {
	c := &Client{
		client:    &mockVaultClient{},
		mountPath: "mount",
		path:      "path",
		logger:    &mockLogger{},
	}

	conn := c.GetConnection("test-connection")
	assert.NotNil(t, conn)
}
