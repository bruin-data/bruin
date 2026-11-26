package secrets

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestNewDopplerClient(t *testing.T) {
	t.Parallel()
	log := &mockLogger{}

	t.Run("returns error if token is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewDopplerClient(log, "", "project", "config")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty doppler token")
	})

	t.Run("returns error if project is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewDopplerClient(log, "token", "", "config")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty doppler project")
	})

	t.Run("returns error if config is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewDopplerClient(log, "token", "project", "")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty doppler config")
	})

	t.Run("creates client successfully with valid parameters", func(t *testing.T) {
		t.Parallel()
		client, err := NewDopplerClient(log, "token", "project", "config")
		require.NoError(t, err)
		require.NotNil(t, client)
	})
}

// mockDopplerHTTPClient implements the dopplerHTTPClient interface for testing.
type mockDopplerHTTPClient struct {
	response map[string]any
	err      error
}

func (m *mockDopplerHTTPClient) GetSecret(ctx context.Context, secretName string) (map[string]any, error) {
	return m.response, m.err
}

func TestDopplerClient_GetConnection_ReturnsConnection(t *testing.T) {
	t.Parallel()
	c := &DopplerClient{
		client: &mockDopplerHTTPClient{
			response: map[string]any{
				"details": map[string]any{
					"username": "testuser",
					"password": "testpass",
					"host":     "testhost",
					"port":     float64(5432),
					"database": "testdb",
					"schema":   "testschema",
				},
				"type": "postgres",
			},
			err: nil,
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
}

func TestDopplerClient_GetConnection_ReturnsGenericConnection(t *testing.T) {
	t.Parallel()
	c := &DopplerClient{
		client: &mockDopplerHTTPClient{
			response: map[string]any{
				"details": map[string]any{
					"value": "somevalue",
				},
				"type": "generic",
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

func TestDopplerClient_GetConnection_HandlesError(t *testing.T) {
	t.Parallel()
	c := &DopplerClient{
		client: &mockDopplerHTTPClient{
			response: nil,
			err:      errors.New("doppler API error"),
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.Nil(t, conn)
}

func TestDopplerClient_GetConnectionDetails_ReturnsDetails(t *testing.T) {
	t.Parallel()
	c := &DopplerClient{
		client: &mockDopplerHTTPClient{
			response: map[string]any{
				"details": map[string]any{
					"value": "somevalue",
				},
				"type": "generic",
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

func TestDopplerClient_GetConnectionDetails_FromCache(t *testing.T) {
	t.Parallel()
	cachedConnection := config.GenericConnection{
		ConnectionMetadata: config.ConnectionMetadata{Name: "test-connection"},
		Value:              "cached-value",
	}
	c := &DopplerClient{
		client: &mockDopplerHTTPClient{
			response: nil,
			err:      errors.New("should not be called"),
		},
		logger:                  &mockLogger{},
		cacheConnectionsDetails: map[string]any{"test-connection": &cachedConnection},
	}

	deets := c.GetConnectionDetails("test-connection")
	require.NotNil(t, deets)
	gc, ok := deets.(*config.GenericConnection)
	require.True(t, ok)
	require.Equal(t, "cached-value", gc.Value)
}

func TestDopplerClient_GetSecret_MissingDetailsAndType(t *testing.T) {
	t.Parallel()
	c := &DopplerClient{
		client: &mockDopplerHTTPClient{
			response: map[string]any{
				"some_field": "value",
			},
			err: nil,
		},
		logger:                  &mockLogger{},
		cacheConnectionsDetails: make(map[string]any),
	}

	deets := c.GetConnectionDetails("test-connection")
	require.Nil(t, deets)
}

func TestDopplerClient_GetSecret_DetailsNotAMap(t *testing.T) {
	t.Parallel()
	c := &DopplerClient{
		client: &mockDopplerHTTPClient{
			response: map[string]any{
				"details": "not-a-map",
				"type":    "generic",
			},
			err: nil,
		},
		logger:                  &mockLogger{},
		cacheConnectionsDetails: make(map[string]any),
	}

	deets := c.GetConnectionDetails("test-connection")
	require.Nil(t, deets)
}

// A Doppler outage mustn't look like a missing secret: Python and R assets report a secret that
// GetConnectionDetails didn't return as missing unless the client says why.
func TestDopplerClient_LookupError_TellsAnOutageFromAMissingSecret(t *testing.T) {
	t.Parallel()

	mock := &mockDopplerHTTPClient{err: &config.SecretsBackendUnavailableError{Err: errors.New("doppler API returned status 503")}}
	c := &DopplerClient{client: mock, logger: &mockLogger{}, cacheConnections: map[string]any{}, cacheConnectionsDetails: map[string]any{}}

	require.Nil(t, c.GetConnectionDetails("pg"))
	var unavailable *config.SecretsBackendUnavailableError
	require.ErrorAs(t, c.LookupError("pg"), &unavailable)

	mock.err = newSecretNotFoundError("secret '%s' not found in Doppler", "other")
	require.Nil(t, c.GetConnection("other"))
	require.NoError(t, c.LookupError("other"), "the secret doesn't exist")
}
