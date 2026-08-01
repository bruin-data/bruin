package secrets

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/hashicorp/vault-client-go"
	"github.com/hashicorp/vault-client-go/schema"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type mockLogger struct{}

func (m *mockLogger) Error(args ...any)                       {}
func (m *mockLogger) Errorf(format string, args ...any)       {}
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
		client, err := NewVaultClient(log, "", "token", "role", "path", "mount", "kubernetes")
		require.Error(t, err)
		require.Nil(t, client)
	})

	t.Run("returns error if path is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewVaultClient(log, "https://vault.example.com", "token", "role", "", "mount", "kubernetes")
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty vault path")
		require.Nil(t, client)
	})

	t.Run("returns error if mountPath is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewVaultClient(log, "https://vault.example.com", "token", "role", "path", "", "kubernetes")
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty vault mountpath")
		require.Nil(t, client)
	})

	t.Run("returns error if no credentials provided", func(t *testing.T) {
		t.Parallel()
		client, err := NewVaultClient(log, "https://vault.example.com", "", "", "path", "mount", "kubernetes")
		require.Error(t, err)
		require.Contains(t, err.Error(), "no vault credentials")
		require.Nil(t, client)
	})
}

func TestValidateVaultAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		address string
		wantErr string
	}{
		{name: "https", address: "https://vault.example.com:8200"},
		{name: "http", address: "http://127.0.0.1:8200"},
		{name: "unix socket", address: "unix:///var/run/vault.sock"},
		{name: "missing scheme", address: "vault.example.com", wantErr: "URL scheme"},
		{name: "missing host", address: "https:///vault", wantErr: "must include a host"},
		{name: "URL credentials", address: "https://user:password@vault.example.com", wantErr: "URL credentials"},
		{name: "query string", address: "https://vault.example.com?token=secret", wantErr: "query parameters"},
		{name: "unsupported scheme", address: "ftp://vault.example.com", wantErr: "URL scheme"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateVaultAddress(tt.address)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestVaultClientConfigFromEnv(t *testing.T) {
	environmentVariables := []string{
		"BRUIN_VAULT_TIMEOUT",
		"BRUIN_VAULT_RETRY_WAIT_MIN",
		"BRUIN_VAULT_RETRY_WAIT_MAX",
		"BRUIN_VAULT_MAX_RETRIES",
	}
	for _, name := range environmentVariables {
		t.Setenv(name, "")
	}

	t.Run("uses defaults", func(t *testing.T) {
		clientConfig, err := vaultClientConfigFromEnv()
		require.NoError(t, err)
		require.Equal(t, defaultVaultClientConfig(), clientConfig)
	})

	t.Run("reads overrides", func(t *testing.T) {
		t.Setenv("BRUIN_VAULT_TIMEOUT", "12s")
		t.Setenv("BRUIN_VAULT_RETRY_WAIT_MIN", "50ms")
		t.Setenv("BRUIN_VAULT_RETRY_WAIT_MAX", "3s")
		t.Setenv("BRUIN_VAULT_MAX_RETRIES", "5")

		clientConfig, err := vaultClientConfigFromEnv()
		require.NoError(t, err)
		require.Equal(t, vaultClientConfig{
			requestTimeout: 12 * time.Second,
			retryWaitMin:   50 * time.Millisecond,
			retryWaitMax:   3 * time.Second,
			retryMax:       5,
		}, clientConfig)
	})

	for _, tt := range []struct {
		name  string
		value string
	}{
		{name: "BRUIN_VAULT_TIMEOUT", value: "invalid"},
		{name: "BRUIN_VAULT_RETRY_WAIT_MIN", value: "0s"},
		{name: "BRUIN_VAULT_RETRY_WAIT_MAX", value: "-1s"},
		{name: "BRUIN_VAULT_MAX_RETRIES", value: "-1"},
	} {
		t.Run("rejects invalid "+tt.name, func(t *testing.T) {
			t.Setenv(tt.name, tt.value)
			_, err := vaultClientConfigFromEnv()
			require.ErrorContains(t, err, tt.name)
		})
	}

	t.Run("rejects a maximum wait below the minimum", func(t *testing.T) {
		t.Setenv("BRUIN_VAULT_RETRY_WAIT_MIN", "2s")
		t.Setenv("BRUIN_VAULT_RETRY_WAIT_MAX", "1s")
		_, err := vaultClientConfigFromEnv()
		require.ErrorContains(t, err, "BRUIN_VAULT_RETRY_WAIT_MAX")
	})
}

// Create a mock vault client that implements kvV2Reader
// and returns a mock *vault.Response[schema.KvV2ReadResponse].
type mockVaultClient struct {
	response *vault.Response[schema.KvV2ReadResponse]
	err      error
	handler  func(context.Context, string) (*vault.Response[schema.KvV2ReadResponse], error)
	calls    atomic.Int32
}

func (m *mockVaultClient) KvV2Read(ctx context.Context, path string, opts ...vault.RequestOption) (*vault.Response[schema.KvV2ReadResponse], error) {
	m.calls.Add(1)
	if m.handler != nil {
		return m.handler(ctx, path)
	}
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

func TestClient_GetConnection_Returns404Error(t *testing.T) {
	t.Parallel()
	c := &Client{
		client: &mockVaultClient{
			response: nil,
			err:      &vault.ResponseError{StatusCode: 404, Errors: []string{}},
		},
		mountPath:        "mount",
		path:             "path",
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("missing-secret")
	require.Nil(t, conn)
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
			ConnectionMetadata: config.ConnectionMetadata{Name: "test-connection"},
			SecretKey:          "test-secret-key",
		}},
	}

	// First call should fetch and cache the details
	deets := c.GetConnectionDetails("test-connection")
	require.NotNil(t, deets)
	require.Equal(
		t,
		config.AthenaConnection{
			ConnectionMetadata: config.ConnectionMetadata{Name: "test-connection"},
			SecretKey:          "test-secret-key",
		},
		deets,
	)
}

func TestLoginToVaultWithKubernetesValidatesAuthResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		statusCode  int
		body        string
		wantToken   string
		wantErr     string
		redactedErr string
	}{
		{
			name:       "returns client token",
			statusCode: http.StatusOK,
			body:       `{"data":null,"auth":{"client_token":"vault-client-token"}}`,
			wantToken:  "vault-client-token",
		},
		{
			name:       "rejects missing auth data",
			statusCode: http.StatusOK,
			body:       `{"data":null}`,
			wantErr:    "Vault returned no client token",
		},
		{
			name:        "redacts Vault error response",
			statusCode:  http.StatusForbidden,
			body:        `{"errors":["sensitive authentication diagnostic"]}`,
			wantErr:     "HTTP status 403",
			redactedErr: "sensitive authentication diagnostic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(tt.statusCode)
				_, _ = writer.Write([]byte(tt.body))
			}))
			defer server.Close()

			clientConfig := vaultClientConfig{
				requestTimeout: time.Second,
				retryWaitMin:   time.Millisecond,
				retryWaitMax:   2 * time.Millisecond,
				retryMax:       0,
			}
			client, err := newVaultAPIClient(server.URL, clientConfig)
			require.NoError(t, err)

			token, err := loginToVaultWithKubernetes(t.Context(), client, "service-account-token", "role", "kubernetes")
			if tt.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, tt.wantToken, token)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			if tt.redactedErr != "" {
				require.NotContains(t, err.Error(), tt.redactedErr)
			}
		})
	}
}

func TestClient_RetriesTransientVaultFailures(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		call := calls.Add(1)
		if call <= 2 {
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = writer.Write([]byte(`{"errors":["temporary failure"]}`))
			return
		}

		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"data":{"data":{"details":{"value":"somevalue"},"type":"generic"}}}`))
	}))
	defer server.Close()

	clientConfig := vaultClientConfig{
		requestTimeout: time.Second,
		retryWaitMin:   time.Millisecond,
		retryWaitMax:   2 * time.Millisecond,
		retryMax:       2,
	}
	client, err := newVaultClientWithToken(t.Context(), server.URL, "token", "mount", &mockLogger{}, "path", clientConfig)
	require.NoError(t, err)

	connection, err := client.ResolveConnection("test-connection")
	require.NoError(t, err)
	require.NotNil(t, connection)
	require.EqualValues(t, 3, calls.Load())
}

func TestClient_DoesNotRetryPermanentVaultFailures(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusNotFound)
		_, _ = writer.Write([]byte(`{"errors":["not found"]}`))
	}))
	defer server.Close()

	clientConfig := vaultClientConfig{
		requestTimeout: time.Second,
		retryWaitMin:   time.Millisecond,
		retryWaitMax:   2 * time.Millisecond,
		retryMax:       3,
	}
	client, err := newVaultClientWithToken(t.Context(), server.URL, "token", "mount", &mockLogger{}, "path", clientConfig)
	require.NoError(t, err)

	_, err = client.ResolveConnection("missing")
	require.ErrorContains(t, err, "not found in Vault")
	require.EqualValues(t, 1, calls.Load())
}

func TestClient_VaultReadHasAnOverallTimeout(t *testing.T) {
	t.Parallel()

	mockClient := &mockVaultClient{
		handler: func(ctx context.Context, _ string) (*vault.Response[schema.KvV2ReadResponse], error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.LessOrEqual(t, time.Until(deadline), 25*time.Millisecond)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	client := newClient(t.Context(), mockClient, "mount", &mockLogger{}, "path", 20*time.Millisecond)

	started := time.Now()
	_, err := client.ResolveConnection("test-connection")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), time.Second)
}

func TestClient_ConcurrentLookupsShareOneVaultRequest(t *testing.T) {
	t.Parallel()

	mockClient := &mockVaultClient{
		handler: func(_ context.Context, _ string) (*vault.Response[schema.KvV2ReadResponse], error) {
			time.Sleep(20 * time.Millisecond)
			return genericVaultResponse(), nil
		},
	}
	client := newClient(t.Context(), mockClient, "mount", &mockLogger{}, "path", time.Second)

	const goroutineCount = 10
	connections := make([]any, goroutineCount)
	var waitGroup sync.WaitGroup
	for index := range goroutineCount {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			connections[index] = client.GetConnection("test-connection")
		}()
	}
	waitGroup.Wait()

	for index, connection := range connections {
		require.NotNil(t, connection, "connection %d", index)
	}
	require.EqualValues(t, 1, mockClient.calls.Load())
}

func TestClient_RejectsMalformedVaultResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		response *vault.Response[schema.KvV2ReadResponse]
		wantErr  string
	}{
		{name: "empty response", response: nil, wantErr: "empty response"},
		{
			name: "missing type",
			response: &vault.Response[schema.KvV2ReadResponse]{Data: schema.KvV2ReadResponse{Data: map[string]any{
				"details": map[string]any{"value": "secret"},
			}}},
			wantErr: "must contain both 'type'",
		},
		{
			name: "empty type",
			response: &vault.Response[schema.KvV2ReadResponse]{Data: schema.KvV2ReadResponse{Data: map[string]any{
				"type":    " ",
				"details": map[string]any{"value": "secret"},
			}}},
			wantErr: "must contain both 'type'",
		},
		{
			name: "invalid details",
			response: &vault.Response[schema.KvV2ReadResponse]{Data: schema.KvV2ReadResponse{Data: map[string]any{
				"type":    "generic",
				"details": "secret",
			}}},
			wantErr: "must contain both 'type'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := newClient(t.Context(), &mockVaultClient{response: tt.response}, "mount", &mockLogger{}, "path", time.Second)
			_, err := client.ResolveConnection("test-connection")
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestClient_RedactsVaultErrorResponseBodies(t *testing.T) {
	t.Parallel()

	client := newClient(t.Context(), &mockVaultClient{err: &vault.ResponseError{
		StatusCode: http.StatusInternalServerError,
		Errors:     []string{"sensitive backend diagnostic"},
	}}, "mount", &mockLogger{}, "path", time.Second)

	_, err := client.ResolveConnection("test-connection")
	require.Error(t, err)
	require.ErrorContains(t, err, fmt.Sprintf("HTTP status %d", http.StatusInternalServerError))
	require.NotContains(t, err.Error(), "sensitive backend diagnostic")
}

func TestVaultRetryBackoffHonorsRetryAfter(t *testing.T) {
	t.Parallel()

	response := &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     http.Header{"Retry-After": []string{"3"}},
	}
	require.Equal(t, 3*time.Second, vaultRetryBackoff(time.Millisecond, time.Second, 0, response))
}

func genericVaultResponse() *vault.Response[schema.KvV2ReadResponse] {
	return &vault.Response[schema.KvV2ReadResponse]{
		Data: schema.KvV2ReadResponse{
			Data: map[string]any{
				"details": map[string]any{
					"value": "somevalue",
				},
				"type": "generic",
			},
		},
	}
}
