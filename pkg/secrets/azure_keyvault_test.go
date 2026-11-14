package secrets

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testGenericSecretJSONAzure = `{"details": {"value": "somevalue"}, "type": "generic"}`

// mockAzureKeyVaultClient implements azureKeyVaultSecretsClient for testing. It serves
// secrets from a map of secret name to value, or a fixed response/error.
type mockAzureKeyVaultClient struct {
	response azsecrets.GetSecretResponse
	err      error
	secrets  map[string]string

	// listPages are the secrets returned by successive list pages.
	listPages [][]*azsecrets.SecretProperties
	listErr   error

	mu        sync.Mutex
	requested []string
}

func (m *mockAzureKeyVaultClient) GetSecret(ctx context.Context, name string, version string, options *azsecrets.GetSecretOptions) (azsecrets.GetSecretResponse, error) {
	m.mu.Lock()
	m.requested = append(m.requested, name)
	m.mu.Unlock()

	if m.secrets != nil {
		// Key Vault secret names are case-insensitive.
		for secretName, value := range m.secrets {
			if strings.EqualFold(secretName, name) {
				return azsecrets.GetSecretResponse{Secret: azsecrets.Secret{Value: &value}}, nil
			}
		}
		return azsecrets.GetSecretResponse{}, &azcore.ResponseError{StatusCode: http.StatusNotFound, ErrorCode: "SecretNotFound"}
	}

	return m.response, m.err
}

func (m *mockAzureKeyVaultClient) NewListSecretPropertiesPager(options *azsecrets.ListSecretPropertiesOptions) *runtime.Pager[azsecrets.ListSecretPropertiesResponse] {
	return runtime.NewPager(runtime.PagingHandler[azsecrets.ListSecretPropertiesResponse]{
		More: func(page azsecrets.ListSecretPropertiesResponse) bool {
			return page.NextLink != nil
		},
		Fetcher: func(ctx context.Context, page *azsecrets.ListSecretPropertiesResponse) (azsecrets.ListSecretPropertiesResponse, error) {
			if m.listErr != nil {
				return azsecrets.ListSecretPropertiesResponse{}, m.listErr
			}
			index := 0
			if page != nil {
				index, _ = strconv.Atoi(*page.NextLink)
			}
			resp := azsecrets.ListSecretPropertiesResponse{}
			if index < len(m.listPages) {
				resp.Value = m.listPages[index]
			}
			if index+1 < len(m.listPages) {
				next := strconv.Itoa(index + 1)
				resp.NextLink = &next
			}
			return resp, nil
		},
	})
}

func newTestAzureClient(mock *mockAzureKeyVaultClient, prefix string) *AzureKeyVaultClient {
	return newAzureKeyVaultClient(&mockLogger{}, mock, prefix)
}

func azureSecretProperties(name string, enabled, managed bool) *azsecrets.SecretProperties {
	id := azsecrets.ID("https://myvault.vault.azure.net/secrets/" + name + "/0123456789abcdef")
	return &azsecrets.SecretProperties{
		ID:         &id,
		Attributes: &azsecrets.SecretAttributes{Enabled: &enabled},
		Managed:    &managed,
	}
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

	t.Run("creates client with valid credentials", func(t *testing.T) {
		t.Parallel()
		client, err := NewAzureKeyVaultClient(log, "https://vault.vault.azure.net", "tenant", "client", "secret")
		require.NoError(t, err)
		require.NotNil(t, client)
	})
}

func TestValidateVaultURL(t *testing.T) {
	t.Parallel()

	for _, vaultURL := range []string{
		"https://myvault.vault.azure.net",
		"https://myvault.vault.azure.net/",
		"https://myvault.vault.azure.net:443",
		"https://MyVault.Vault.Azure.Net",
		"https://myvault.vault.azure.cn",
		"https://myvault.vault.usgovcloudapi.net",
		"https://myvault.vault.microsoftazure.de",
	} {
		require.NoError(t, validateVaultURL(vaultURL), vaultURL)
	}

	for _, vaultURL := range []string{
		"not-a-url",
		"http://myvault.vault.azure.net",
		"http://myvault.vault.azure.cn",
		"https://vault.example.com",
		"https://vault.azure.net",
		"https://myvault.vault.azure.net.example.com",
		"https://myvaultvault.azure.net",
	} {
		err := validateVaultURL(vaultURL)
		require.Error(t, err, vaultURL)
		require.Contains(t, err.Error(), "invalid Azure Key Vault URL", vaultURL)
	}
}

func TestAzureClientOptionsForVault(t *testing.T) {
	//nolint:paralleltest // mutates process env vars, which parallel subtests would race on.
	t.Run("picks the vault's cloud", func(t *testing.T) {
		unsetenv(t, "AZURE_AUTHORITY_HOST")

		require.Equal(t, cloud.AzureChina, azureClientOptionsForVault("https://myvault.vault.azure.cn").Cloud)
		require.Equal(t, cloud.AzureGovernment, azureClientOptionsForVault("https://myvault.vault.usgovcloudapi.net").Cloud)
		require.Equal(t, cloud.Configuration{}, azureClientOptionsForVault("https://myvault.vault.azure.net").Cloud)
	})

	t.Run("AZURE_AUTHORITY_HOST wins", func(t *testing.T) {
		t.Setenv("AZURE_AUTHORITY_HOST", "https://login.example.com/")

		require.Equal(t, cloud.Configuration{}, azureClientOptionsForVault("https://myvault.vault.azure.cn").Cloud)
	})
}

func TestNewAzureKeyVaultClientFromEnv(t *testing.T) {
	log := &mockLogger{}

	t.Run("rejects an unsupported auth method", func(t *testing.T) {
		clearAzureKeyVaultEnv(t)
		t.Setenv("BRUIN_AZURE_KEYVAULT_URL", "https://myvault.vault.azure.net")
		t.Setenv("BRUIN_AZURE_AUTH_METHOD", "password")

		_, err := NewAzureKeyVaultClientFromEnv(log)
		require.ErrorContains(t, err, "unsupported Azure auth method: password")
		require.ErrorContains(t, err, "workload_identity")
	})

	t.Run("rejects a prefix Key Vault can't store", func(t *testing.T) {
		clearAzureKeyVaultEnv(t)
		t.Setenv("BRUIN_AZURE_KEYVAULT_URL", "https://myvault.vault.azure.net")
		t.Setenv("BRUIN_AZURE_SECRET_PREFIX", "bruin/prod/")

		_, err := NewAzureKeyVaultClientFromEnv(log)
		require.ErrorContains(t, err, "BRUIN_AZURE_SECRET_PREFIX 'bruin/prod/' is not valid")
	})

	t.Run("reads the secret prefix", func(t *testing.T) {
		clearAzureKeyVaultEnv(t)
		t.Setenv("BRUIN_AZURE_KEYVAULT_URL", "https://myvault.vault.azure.net")
		t.Setenv("BRUIN_AZURE_SECRET_PREFIX", "bruin-prod-")

		client, err := NewAzureKeyVaultClientFromEnv(log)
		require.NoError(t, err)
		require.Equal(t, "bruin-prod-", client.prefix)
	})

	t.Run("workload_identity explains missing webhook env vars", func(t *testing.T) {
		clearAzureKeyVaultEnv(t)
		t.Setenv("BRUIN_AZURE_KEYVAULT_URL", "https://myvault.vault.azure.net")
		t.Setenv("BRUIN_AZURE_AUTH_METHOD", "workload_identity")

		_, err := NewAzureKeyVaultClientFromEnv(log)
		require.ErrorContains(t, err, "failed to create Azure workload identity credential")
		require.ErrorContains(t, err, "AZURE_FEDERATED_TOKEN_FILE")
		require.ErrorContains(t, err, "azure.workload.identity/use")
	})

	t.Run("workload_identity works with the AKS webhook env vars", func(t *testing.T) {
		clearAzureKeyVaultEnv(t)
		setAKSWorkloadIdentityEnv(t)
		t.Setenv("BRUIN_AZURE_KEYVAULT_URL", "https://myvault.vault.azure.net")
		t.Setenv("BRUIN_AZURE_AUTH_METHOD", "workload_identity")

		client, err := NewAzureKeyVaultClientFromEnv(log)
		require.NoError(t, err)
		require.NotNil(t, client)
	})
}

func TestAzureCredentials_WorkloadAndManagedIdentity(t *testing.T) {
	const vaultScope = "https://vault.azure.net/.default"

	//nolint:paralleltest // mutates process env vars, which parallel subtests would race on.
	t.Run("workload_identity exchanges the federated token", func(t *testing.T) {
		clearAzureKeyVaultEnv(t)
		setAKSWorkloadIdentityEnv(t)
		entra := &fakeEntra{}

		cred, err := azureCredentialFromEnv("workload_identity", azcore.ClientOptions{Transport: entra})
		require.NoError(t, err)

		token, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{vaultScope}})
		require.NoError(t, err)
		require.Equal(t, "entra-token", token.Token)
		require.Equal(t, "federated-sa-token", entra.lastTokenForm("client_assertion"))
		require.Equal(t, "aks-client-id", entra.lastTokenForm("client_id"))
		require.Equal(t, "login.microsoftonline.com", entra.lastTokenHost())
	})

	t.Run("BRUIN_AZURE_CLIENT_ID overrides the webhook's client ID", func(t *testing.T) {
		clearAzureKeyVaultEnv(t)
		setAKSWorkloadIdentityEnv(t)
		t.Setenv("BRUIN_AZURE_CLIENT_ID", "bruin-client-id")
		entra := &fakeEntra{}

		cred, err := azureCredentialFromEnv("workload_identity", azcore.ClientOptions{Transport: entra})
		require.NoError(t, err)

		_, err = cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{vaultScope}})
		require.NoError(t, err)
		require.Equal(t, "bruin-client-id", entra.lastTokenForm("client_id"))
	})

	//nolint:paralleltest // mutates process env vars, which parallel subtests would race on.
	t.Run("workload_identity authenticates against a sovereign cloud vault's authority", func(t *testing.T) {
		clearAzureKeyVaultEnv(t)
		setAKSWorkloadIdentityEnv(t)
		entra := &fakeEntra{}
		clientOptions := azureClientOptionsForVault("https://myvault.vault.azure.cn")
		clientOptions.Transport = entra

		cred, err := azureCredentialFromEnv("workload_identity", clientOptions)
		require.NoError(t, err)

		_, err = cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{"https://vault.azure.cn/.default"}})
		require.NoError(t, err)
		require.Equal(t, "login.chinacloudapi.cn", entra.lastTokenHost())
	})

	//nolint:paralleltest // mutates process env vars, which parallel subtests would race on.
	t.Run("default picks up AKS workload identity", func(t *testing.T) {
		clearAzureKeyVaultEnv(t)
		setAKSWorkloadIdentityEnv(t)
		entra := &fakeEntra{}

		cred, err := azureCredentialFromEnv("default", azcore.ClientOptions{Transport: entra})
		require.NoError(t, err)

		token, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{vaultScope}})
		require.NoError(t, err)
		require.Equal(t, "entra-token", token.Token)
		require.Equal(t, "federated-sa-token", entra.lastTokenForm("client_assertion"))
		require.Zero(t, entra.imdsCalls())
	})

	//nolint:paralleltest // mutates process env vars, which parallel subtests would race on.
	t.Run("default falls back to managed identity", func(t *testing.T) {
		clearAzureKeyVaultEnv(t)
		entra := &fakeEntra{}

		cred, err := azureCredentialFromEnv("default", azcore.ClientOptions{Transport: entra})
		require.NoError(t, err)

		token, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{vaultScope}})
		require.NoError(t, err)
		require.Equal(t, "imds-token", token.Token)
		require.NotZero(t, entra.imdsCalls())
	})

	t.Run("managed_identity selects a user-assigned identity", func(t *testing.T) {
		clearAzureKeyVaultEnv(t)
		t.Setenv("BRUIN_AZURE_CLIENT_ID", "user-assigned-id")
		entra := &fakeEntra{}

		cred, err := azureCredentialFromEnv("managed_identity", azcore.ClientOptions{Transport: entra})
		require.NoError(t, err)

		token, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{vaultScope}})
		require.NoError(t, err)
		require.Equal(t, "imds-token", token.Token)
		require.Equal(t, "user-assigned-id", entra.lastIMDSQuery("client_id"))
	})
}

func clearAzureKeyVaultEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"BRUIN_AZURE_KEYVAULT_URL",
		"BRUIN_AZURE_AUTH_METHOD",
		"BRUIN_AZURE_TENANT_ID",
		"BRUIN_AZURE_CLIENT_ID",
		"BRUIN_AZURE_CLIENT_SECRET",
		"BRUIN_AZURE_SECRET_PREFIX",
		"AZURE_CLIENT_ID",
		"AZURE_TENANT_ID",
		"AZURE_CLIENT_SECRET",
		"AZURE_CLIENT_CERTIFICATE_PATH",
		"AZURE_CLIENT_CERTIFICATE_PASSWORD",
		"AZURE_USERNAME",
		"AZURE_PASSWORD",
		"AZURE_FEDERATED_TOKEN_FILE",
		"AZURE_AUTHORITY_HOST",
		"AZURE_ADDITIONALLY_ALLOWED_TENANTS",
		"IDENTITY_ENDPOINT",
		"IDENTITY_HEADER",
		"IDENTITY_SERVER_THUMBPRINT",
		"IMDS_ENDPOINT",
		"MSI_ENDPOINT",
		"MSI_SECRET",
	} {
		unsetenv(t, key)
	}
}

// setAKSWorkloadIdentityEnv sets what the AKS workload identity webhook injects into a pod.
func setAKSWorkloadIdentityEnv(t *testing.T) {
	t.Helper()

	t.Setenv("AZURE_CLIENT_ID", "aks-client-id")
	t.Setenv("AZURE_TENANT_ID", "aks-tenant-id")
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", writeTempFile(t, "federated-sa-token"))
}

// fakeEntra is an azcore transport standing in for Microsoft Entra ID and the
// Azure instance metadata service (IMDS), recording what it was asked.
type fakeEntra struct {
	mu         sync.Mutex
	tokenForms []map[string][]string
	tokenHosts []string
	imdsReqs   []*http.Request
}

func (f *fakeEntra) Do(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	host := req.URL.Host
	if host == "169.254.169.254" {
		f.imdsReqs = append(f.imdsReqs, req)
		if req.Header.Get("Metadata") == "" {
			// DefaultAzureCredential's availability probe.
			return fakeResponse(req, http.StatusBadRequest, `{"error":"invalid_request"}`), nil
		}
		return fakeResponse(req, http.StatusOK, `{"access_token":"imds-token","expires_in":"3600","expires_on":"4102444800","resource":"https://vault.azure.net","token_type":"Bearer"}`), nil
	}

	segments := strings.Split(req.URL.Path, "/")
	switch segments[len(segments)-1] {
	case "instance":
		// Instance discovery goes to the public cloud; like Entra, answer for the authority it asks about.
		authority, err := url.Parse(req.URL.Query().Get("authorization_endpoint"))
		if err != nil {
			return nil, err
		}
		tenant := strings.Split(strings.TrimPrefix(authority.Path, "/"), "/")[0]
		return fakeResponse(req, http.StatusOK, fmt.Sprintf(`{"tenant_discovery_endpoint":"https://%[1]s/%[2]s/v2.0/.well-known/openid-configuration",`+
			`"api-version":"1.1","metadata":[{"preferred_network":"%[1]s","preferred_cache":"%[1]s","aliases":["%[1]s"]}]}`, authority.Host, tenant)), nil
	case "openid-configuration":
		tenant := segments[1]
		return fakeResponse(req, http.StatusOK, fmt.Sprintf(`{"token_endpoint":"https://%[1]s/%[2]s/oauth2/v2.0/token",`+
			`"authorization_endpoint":"https://%[1]s/%[2]s/oauth2/v2.0/authorize","issuer":"https://%[1]s/%[2]s/v2.0"}`, host, tenant)), nil
	case "token":
		if err := req.ParseForm(); err != nil {
			return nil, err
		}
		f.tokenForms = append(f.tokenForms, req.PostForm)
		f.tokenHosts = append(f.tokenHosts, host)
		return fakeResponse(req, http.StatusOK, `{"access_token":"entra-token","expires_in":3600,"token_type":"Bearer"}`), nil
	}

	return nil, fmt.Errorf("fakeEntra: unexpected request %s", req.URL)
}

func fakeResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func (f *fakeEntra) lastTokenForm(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.tokenForms) == 0 {
		return ""
	}
	if values := f.tokenForms[len(f.tokenForms)-1][key]; len(values) > 0 {
		return values[0]
	}

	return ""
}

func (f *fakeEntra) lastTokenHost() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.tokenHosts) == 0 {
		return ""
	}

	return f.tokenHosts[len(f.tokenHosts)-1]
}

func (f *fakeEntra) imdsCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.imdsReqs)
}

func (f *fakeEntra) lastIMDSQuery(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.imdsReqs) == 0 {
		return ""
	}

	return f.imdsReqs[len(f.imdsReqs)-1].URL.Query().Get(key)
}

func TestAzureKeyVaultClient_GetConnection_ReturnsConnection(t *testing.T) {
	t.Parallel()
	secretValue := `{"details": {"username": "testuser", "password": "testpass", "host": "testhost", "port": 1337, "database": "testdb", "schema": "testschema"}, "type": "postgres"}`
	c := newTestAzureClient(&mockAzureKeyVaultClient{
		response: azsecrets.GetSecretResponse{Secret: azsecrets.Secret{Value: &secretValue}},
	}, "")

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
	require.Equal(t, "postgres", c.GetConnectionType("test-connection"))
}

func TestAzureKeyVaultClient_GetConnection_ReturnsGenericConnection(t *testing.T) {
	t.Parallel()
	secretValue := testGenericSecretJSONAzure
	c := newTestAzureClient(&mockAzureKeyVaultClient{
		response: azsecrets.GetSecretResponse{Secret: azsecrets.Secret{Value: &secretValue}},
	}, "")

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
	require.Equal(t, "somevalue", conn.(*config.GenericConnection).Value)
}

func TestAzureKeyVaultClient_GetConnection_ReturnsError(t *testing.T) {
	t.Parallel()
	c := newTestAzureClient(&mockAzureKeyVaultClient{err: context.DeadlineExceeded}, "")

	conn := c.GetConnection("missing-secret")
	require.Nil(t, conn)

	_, err := c.ResolveConnection("missing-secret")
	require.ErrorContains(t, err, "failed to read secret 'missing-secret' from Azure Key Vault")
	require.False(t, isSecretNotFound(err))
}

func TestAzureKeyVaultClient_GetConnection_NotFound(t *testing.T) {
	t.Parallel()
	c := newTestAzureClient(&mockAzureKeyVaultClient{secrets: map[string]string{}}, "")

	_, err := c.ResolveConnection("missing-secret")
	require.True(t, isSecretNotFound(err))
	require.ErrorContains(t, err, "secret 'missing-secret' not found in Azure Key Vault")
}

func TestAzureKeyVaultClient_SharesOneFetchPerName(t *testing.T) {
	t.Parallel()
	mock := &mockAzureKeyVaultClient{secrets: map[string]string{"test-connection": testGenericSecretJSONAzure}}
	c := newTestAzureClient(mock, "")

	require.Equal(t, "generic", c.GetConnectionType("test-connection"))
	require.NotNil(t, c.GetConnection("test-connection"))
	require.NotNil(t, c.GetConnectionDetails("test-connection"))
	require.Equal(t, "generic", c.GetConnectionType("test-connection"))

	require.Equal(t, []string{"test-connection"}, mock.requested)
}

func TestAzureKeyVaultClient_GetConnection_ThreadSafe(t *testing.T) {
	t.Parallel()
	mock := &mockAzureKeyVaultClient{secrets: map[string]string{"test-connection": testGenericSecretJSONAzure}}
	c := newTestAzureClient(mock, "")

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := c.GetConnection("test-connection")
			assert.NotNil(t, conn)
		}()
	}
	wg.Wait()
}

func TestAzureKeyVaultClient_GetConnectionDetails_ReturnsDetails(t *testing.T) {
	t.Parallel()
	secretValue := testGenericSecretJSONAzure
	c := newTestAzureClient(&mockAzureKeyVaultClient{
		response: azsecrets.GetSecretResponse{Secret: azsecrets.Secret{Value: &secretValue}},
	}, "")

	deets := c.GetConnectionDetails("test-connection")
	require.NotNil(t, deets)
	gc, ok := deets.(*config.GenericConnection)
	require.True(t, ok)
	require.Equal(t, "test-connection", gc.Name)
	require.Equal(t, "somevalue", gc.Value)
}

func TestAzureKeyVaultClient_GetConnection_RejectsMalformedSecrets(t *testing.T) {
	t.Parallel()

	for name, value := range map[string]*string{
		"nil secret value":  nil,
		"invalid JSON":      ptr("not valid json"),
		"missing fields":    ptr(`{"some": "data"}`),
		"details not a map": ptr(`{"type": "generic", "details": "not a map"}`),
		"empty type":        ptr(`{"type": "", "details": {"value": "test"}}`),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := newTestAzureClient(&mockAzureKeyVaultClient{
				response: azsecrets.GetSecretResponse{Secret: azsecrets.Secret{Value: value}},
			}, "")
			require.Nil(t, c.GetConnection("test-connection"))
			require.Empty(t, c.GetConnectionType("test-connection"))
		})
	}
}

func ptr(s string) *string { return &s }

func TestAzureKeyVaultClient_RejectsInvalidSecretNames(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"my_connection", "my.connection", "bruin/prod/conn", "köln", strings.Repeat("a", 128)} {
		mock := &mockAzureKeyVaultClient{secrets: map[string]string{}}
		c := newTestAzureClient(mock, "")

		_, err := c.ResolveConnection(name)
		require.Error(t, err, name)
		require.False(t, isSecretNotFound(err), name)
		require.Contains(t, err.Error(), "not a valid Key Vault secret name", name)
		require.Contains(t, err.Error(), "letters, digits and hyphens", name)
		require.Empty(t, mock.requested, "no request should be sent for %q", name)
	}

	mock := &mockAzureKeyVaultClient{secrets: map[string]string{strings.Repeat("a", 127): testGenericSecretJSONAzure}}
	require.NotNil(t, newTestAzureClient(mock, "").GetConnection(strings.Repeat("a", 127)))

	// The limit applies to the full secret name, prefix included.
	mock = &mockAzureKeyVaultClient{secrets: map[string]string{}}
	_, err := newTestAzureClient(mock, "bruin-prod-").ResolveConnection(strings.Repeat("a", 120))
	require.ErrorContains(t, err, "at most 127 characters")
}

func TestAzureKeyVaultClient_Prefix(t *testing.T) {
	t.Parallel()
	mock := &mockAzureKeyVaultClient{secrets: map[string]string{"bruin-prod-test-connection": testGenericSecretJSONAzure}}
	c := newTestAzureClient(mock, "bruin-prod-")

	deets := c.GetConnectionDetails("test-connection")
	require.NotNil(t, deets)
	require.Equal(t, "test-connection", deets.(*config.GenericConnection).Name)
	require.Equal(t, []string{"bruin-prod-test-connection"}, mock.requested)
}

func TestAzureKeyVaultClient_GetConnectionsConfig(t *testing.T) {
	t.Parallel()

	t.Run("lists every page and skips disabled, managed and non-connection secrets", func(t *testing.T) {
		t.Parallel()
		mock := &mockAzureKeyVaultClient{
			secrets: map[string]string{
				"conn-a":    testGenericSecretJSONAzure,
				"conn-b":    testGenericSecretJSONAzure,
				"unrelated": "plain text",
			},
			listPages: [][]*azsecrets.SecretProperties{
				{azureSecretProperties("conn-a", true, false), azureSecretProperties("disabled", false, false)},
				{azureSecretProperties("unrelated", true, false), azureSecretProperties("cert-backing", true, true), azureSecretProperties("conn-b", true, false)},
			},
		}
		c := newTestAzureClient(mock, "")

		var lister config.ConnectionsConfigGetter = c
		connections, err := lister.GetConnectionsConfig()
		require.NoError(t, err)
		require.Len(t, connections.Generic, 2)
		require.Equal(t, "conn-a", connections.Generic[0].Name)
		require.Equal(t, "conn-b", connections.Generic[1].Name)
		require.ElementsMatch(t, []string{"conn-a", "conn-b", "unrelated"}, mock.requested)
	})

	t.Run("filters by prefix case-insensitively and strips it", func(t *testing.T) {
		t.Parallel()
		mock := &mockAzureKeyVaultClient{
			secrets: map[string]string{"Bruin-Prod-conn-a": testGenericSecretJSONAzure},
			listPages: [][]*azsecrets.SecretProperties{{
				azureSecretProperties("Bruin-Prod-conn-a", true, false),
				azureSecretProperties("bruin-dev-conn-b", true, false),
				azureSecretProperties("bruin-prod-", true, false),
			}},
		}
		c := newTestAzureClient(mock, "bruin-prod-")

		connections, err := c.GetConnectionsConfig()
		require.NoError(t, err)
		require.Len(t, connections.Generic, 1)
		require.Equal(t, "conn-a", connections.Generic[0].Name)
	})

	t.Run("returns list errors", func(t *testing.T) {
		t.Parallel()
		c := newTestAzureClient(&mockAzureKeyVaultClient{listErr: &azcore.ResponseError{StatusCode: http.StatusForbidden}}, "")

		_, err := c.GetConnectionsConfig()
		require.ErrorContains(t, err, "failed to list secrets in Azure Key Vault")
	})
}
