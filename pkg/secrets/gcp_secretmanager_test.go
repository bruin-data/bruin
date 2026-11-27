package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testGCPProject = "my-project"

// mockGCPSecretManagerClient serves secrets from a map of secret ID to value, or a fixed error.
type mockGCPSecretManagerClient struct {
	secrets map[string]string
	err     error
	// checksum, when set, replaces the CRC32C sent with every payload.
	checksum *int64

	// listIDs are the secret IDs ListSecrets returns.
	listIDs []string
	listErr error

	mu        sync.Mutex
	requested []string
	listed    []string
}

func (m *mockGCPSecretManagerClient) AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	m.mu.Lock()
	m.requested = append(m.requested, req.GetName())
	m.mu.Unlock()

	if m.err != nil {
		return nil, m.err
	}

	// projects/<project>/secrets/<id>/versions/<version>
	parts := strings.Split(req.GetName(), "/")
	value, ok := m.secrets[parts[3]]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Secret [%s] not found or has no versions.", req.GetName())
	}

	checksum := int64(crc32.Checksum([]byte(value), crc32.MakeTable(crc32.Castagnoli)))
	if m.checksum != nil {
		checksum = *m.checksum
	}

	return &secretmanagerpb.AccessSecretVersionResponse{
		Name:    req.GetName(),
		Payload: &secretmanagerpb.SecretPayload{Data: []byte(value), DataCrc32C: &checksum},
	}, nil
}

//nolint:ireturn // fakes the SDK iterator.
func (m *mockGCPSecretManagerClient) ListSecrets(ctx context.Context, req *secretmanagerpb.ListSecretsRequest) gcpSecretIterator {
	m.mu.Lock()
	m.listed = append(m.listed, req.GetParent())
	m.mu.Unlock()

	it := &fakeGCPSecretIterator{err: m.listErr}
	for _, id := range m.listIDs {
		// The API names secrets by project number, not project ID.
		it.secrets = append(it.secrets, &secretmanagerpb.Secret{Name: "projects/123456789/secrets/" + id})
	}

	return it
}

type fakeGCPSecretIterator struct {
	secrets []*secretmanagerpb.Secret
	err     error
}

func (it *fakeGCPSecretIterator) Next() (*secretmanagerpb.Secret, error) {
	if it.err != nil {
		return nil, it.err
	}
	if len(it.secrets) == 0 {
		return nil, iterator.Done
	}
	secret := it.secrets[0]
	it.secrets = it.secrets[1:]

	return secret, nil
}

func newTestGCPClient(mock *mockGCPSecretManagerClient, prefix string) *GCPSecretManagerClient {
	return newGCPSecretManagerClient(&mockLogger{}, mock, testGCPProject, prefix, "")
}

func gcpVersionName(secretID, version string) string {
	return "projects/" + testGCPProject + "/secrets/" + secretID + "/versions/" + version
}

func TestGCPSecretManagerClient_GetConnection_ReturnsConnection(t *testing.T) {
	t.Parallel()
	secretString := `{"details": {"username": "testuser", "password": "testpass", "host": "testhost", "port": 1337, "database": "testdb", "schema": "testschema"}, "type": "postgres"}`
	c := newTestGCPClient(&mockGCPSecretManagerClient{secrets: map[string]string{"test-connection": secretString}}, "")

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
	require.Equal(t, "postgres", c.GetConnectionType("test-connection"))
}

func TestGCPSecretManagerClient_GetConnection_ReturnsGenericConnection(t *testing.T) {
	t.Parallel()
	c := newTestGCPClient(&mockGCPSecretManagerClient{secrets: map[string]string{"test-connection": testGenericSecretJSON}}, "")

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
	require.Equal(t, "somevalue", conn.(*config.GenericConnection).Value)
}

func TestGCPSecretManagerClient_GetConnectionDetails_ReturnsDetails(t *testing.T) {
	t.Parallel()
	c := newTestGCPClient(&mockGCPSecretManagerClient{secrets: map[string]string{"test-connection": testGenericSecretJSON}}, "")

	deets := c.GetConnectionDetails("test-connection")
	require.NotNil(t, deets)
	gc, ok := deets.(*config.GenericConnection)
	require.True(t, ok)
	require.Equal(t, "test-connection", gc.Name)
	require.Equal(t, "somevalue", gc.Value)
}

func TestGCPSecretManagerClient_ReadsTheConfiguredVersion(t *testing.T) {
	t.Parallel()
	mock := &mockGCPSecretManagerClient{secrets: map[string]string{"conn": testGenericSecretJSON}}

	require.NotNil(t, newTestGCPClient(mock, "").GetConnection("conn"))
	require.NotNil(t, newGCPSecretManagerClient(&mockLogger{}, mock, testGCPProject, "", "prod").GetConnection("conn"))

	require.Equal(t, []string{gcpVersionName("conn", "latest"), gcpVersionName("conn", "prod")}, mock.requested)
}

func TestGCPSecretManagerClient_GetConnection_NotFound(t *testing.T) {
	t.Parallel()
	c := newTestGCPClient(&mockGCPSecretManagerClient{secrets: map[string]string{}}, "")

	require.Nil(t, c.GetConnection("missing-secret"))
	_, err := c.ResolveConnection("missing-secret")
	require.True(t, isSecretNotFound(err))
	require.ErrorContains(t, err, "secret 'missing-secret' (version 'latest') in GCP project 'my-project' not found in GCP Secret Manager")
	require.NoError(t, c.LookupError("missing-secret"), "a missing secret is just a missing connection")
}

func TestGCPSecretManagerClient_GetConnection_PermissionDenied(t *testing.T) {
	t.Parallel()
	c := newTestGCPClient(&mockGCPSecretManagerClient{err: status.Error(codes.PermissionDenied, "Permission 'secretmanager.versions.access' denied")}, "")

	require.Nil(t, c.GetConnection("conn"))
	_, err := c.ResolveConnection("conn")
	require.False(t, isSecretNotFound(err))
	require.ErrorContains(t, err, "permission denied reading secret 'conn' (version 'latest') in GCP project 'my-project'")
	require.ErrorContains(t, err, "secretmanager.versions.access")
	require.ErrorContains(t, err, "roles/secretmanager.secretAccessor")

	var unavailable *config.SecretsBackendUnavailableError
	require.ErrorAs(t, c.LookupError("conn"), &unavailable)
}

func serviceDisabledError(t *testing.T, consumer string) error {
	t.Helper()
	st, err := status.New(codes.PermissionDenied, "Secret Manager API has not been used in project before or it is disabled.").
		WithDetails(&errdetails.ErrorInfo{
			Reason:   "SERVICE_DISABLED",
			Domain:   "googleapis.com",
			Metadata: map[string]string{"service": "secretmanager.googleapis.com", "consumer": consumer},
		})
	require.NoError(t, err)
	return st.Err()
}

func TestGCPSecretManagerClient_ServiceDisabled(t *testing.T) {
	t.Parallel()

	t.Run("read names the API, not an IAM role", func(t *testing.T) {
		t.Parallel()
		c := newTestGCPClient(&mockGCPSecretManagerClient{err: serviceDisabledError(t, "projects/quota-project")}, "")

		require.Nil(t, c.GetConnection("conn"))
		_, err := c.ResolveConnection("conn")
		require.ErrorContains(t, err, "the Secret Manager API is not enabled in GCP project 'quota-project'")
		require.ErrorContains(t, err, "gcloud services enable secretmanager.googleapis.com --project quota-project")
		require.NotContains(t, err.Error(), "roles/secretmanager.secretAccessor")

		var unavailable *config.SecretsBackendUnavailableError
		require.ErrorAs(t, c.LookupError("conn"), &unavailable)
	})

	t.Run("list names the API, not an IAM role", func(t *testing.T) {
		t.Parallel()
		c := newTestGCPClient(&mockGCPSecretManagerClient{listErr: serviceDisabledError(t, "projects/quota-project")}, "")

		_, err := c.GetConnectionsConfig()
		require.ErrorContains(t, err, "the Secret Manager API is not enabled in GCP project 'quota-project'")
		require.NotContains(t, err.Error(), "secretmanager.secrets.list")
	})

	t.Run("falls back to the configured project without a consumer", func(t *testing.T) {
		t.Parallel()
		c := newTestGCPClient(&mockGCPSecretManagerClient{listErr: serviceDisabledError(t, "")}, "")

		_, err := c.GetConnectionsConfig()
		require.ErrorContains(t, err, "the Secret Manager API is not enabled in GCP project 'my-project'")
	})
}

func TestGCPSecretManagerClient_GetConnection_ReturnsError(t *testing.T) {
	t.Parallel()
	c := newTestGCPClient(&mockGCPSecretManagerClient{err: status.Error(codes.Unavailable, "connection reset")}, "")

	require.Nil(t, c.GetConnection("conn"))
	_, err := c.ResolveConnection("conn")
	require.ErrorContains(t, err, "failed to read secret 'conn' (version 'latest') in GCP project 'my-project' from GCP Secret Manager")
	require.False(t, isSecretNotFound(err))

	var unavailable *config.SecretsBackendUnavailableError
	require.ErrorAs(t, err, &unavailable)
}

func TestGCPSecretManagerClient_GetConnection_DisabledVersionIsInvalid(t *testing.T) {
	t.Parallel()
	c := newTestGCPClient(&mockGCPSecretManagerClient{err: status.Error(codes.FailedPrecondition, "SecretVersion is in DISABLED state")}, "")

	_, err := c.ResolveConnection("conn")
	require.ErrorContains(t, err, "can't be read from GCP Secret Manager")
	require.False(t, isSecretNotFound(err))

	var unavailable *config.SecretsBackendUnavailableError
	require.False(t, errors.As(err, &unavailable), "a disabled version isn't an outage")
	var invalid *invalidConnectionSecretError
	require.ErrorAs(t, err, &invalid)
}

func TestGCPSecretManagerClient_GetConnection_ChecksumMismatch(t *testing.T) {
	t.Parallel()
	wrong := int64(42)
	c := newTestGCPClient(&mockGCPSecretManagerClient{secrets: map[string]string{"conn": testGenericSecretJSON}, checksum: &wrong}, "")

	_, err := c.ResolveConnection("conn")
	require.ErrorContains(t, err, "failed its checksum")

	var unavailable *config.SecretsBackendUnavailableError
	require.ErrorAs(t, err, &unavailable)
}

func TestGCPSecretManagerClient_GetConnection_RejectsMalformedSecrets(t *testing.T) {
	t.Parallel()

	for name, value := range map[string]string{
		"invalid JSON":   "not valid json",
		"missing fields": `{"some": "data"}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := newTestGCPClient(&mockGCPSecretManagerClient{secrets: map[string]string{"conn": value}}, "")
			require.Nil(t, c.GetConnection("conn"))
			require.Empty(t, c.GetConnectionType("conn"))

			var unavailable *config.SecretsBackendUnavailableError
			_, err := c.ResolveConnection("conn")
			require.Error(t, err)
			require.False(t, errors.As(err, &unavailable), "an invalid secret isn't an outage")
		})
	}
}

func TestGCPSecretManagerClient_RejectsInvalidSecretIDs(t *testing.T) {
	t.Parallel()
	mock := &mockGCPSecretManagerClient{secrets: map[string]string{}}
	c := newTestGCPClient(mock, "")

	for _, name := range []string{"my.postgres", "my/postgres", strings.Repeat("a", 256)} {
		_, err := c.ResolveConnection(name)
		require.ErrorContains(t, err, "is not a valid secret ID")
		require.ErrorContains(t, err, "rename the connection")
		require.False(t, isSecretNotFound(err))
	}
	require.Empty(t, mock.requested, "invalid names must not reach the API")

	_, err := newTestGCPClient(mock, "bruin_prod_").ResolveConnection(strings.Repeat("a", 250))
	require.ErrorContains(t, err, "is not a valid secret ID", "the prefix counts towards the length limit")
}

func TestGCPSecretManagerClient_SharesOneFetchPerName(t *testing.T) {
	t.Parallel()
	mock := &mockGCPSecretManagerClient{secrets: map[string]string{"test-connection": testGenericSecretJSON}}
	c := newTestGCPClient(mock, "")

	require.Equal(t, "generic", c.GetConnectionType("test-connection"))
	require.NotNil(t, c.GetConnection("test-connection"))
	require.NotNil(t, c.GetConnectionDetails("test-connection"))
	require.Equal(t, "generic", c.GetConnectionType("test-connection"))

	require.Equal(t, []string{gcpVersionName("test-connection", "latest")}, mock.requested)
}

func TestGCPSecretManagerClient_ConcurrentLookupsShareOneFetch(t *testing.T) {
	t.Parallel()
	mock := &mockGCPSecretManagerClient{secrets: map[string]string{"conn": testGenericSecretJSON}}
	c := newTestGCPClient(mock, "")

	var wg sync.WaitGroup
	var found atomic.Int32
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c.GetConnection("conn") != nil {
				found.Add(1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int32(20), found.Load())
	require.Len(t, mock.requested, 1)
}

func TestGCPSecretManagerClient_Prefix(t *testing.T) {
	t.Parallel()
	mock := &mockGCPSecretManagerClient{secrets: map[string]string{"bruin-prod-test-connection": testGenericSecretJSON}}
	c := newTestGCPClient(mock, "bruin-prod-")

	deets := c.GetConnectionDetails("test-connection")
	require.NotNil(t, deets)
	require.Equal(t, "test-connection", deets.(*config.GenericConnection).Name)
	require.Equal(t, []string{gcpVersionName("bruin-prod-test-connection", "latest")}, mock.requested)

	_, err := c.ResolveConnection("other")
	require.True(t, isSecretNotFound(err))
	require.ErrorContains(t, err, "secret 'bruin-prod-other'")
}

func TestGCPSecretManagerClient_GetConnectionsConfig(t *testing.T) {
	t.Parallel()

	t.Run("lists every secret without a prefix", func(t *testing.T) {
		t.Parallel()
		mock := &mockGCPSecretManagerClient{
			secrets: map[string]string{
				"conn-a":    testGenericSecretJSON,
				"conn-b":    testGenericSecretJSON,
				"unrelated": "plain text",
			},
			// no-versions has no secret versions, so reading it returns NotFound.
			listIDs: []string{"conn-b", "unrelated", "no-versions", "conn-a"},
		}
		c := newTestGCPClient(mock, "")

		var lister config.ConnectionsConfigGetter = c
		connections, err := lister.GetConnectionsConfig()
		require.NoError(t, err)
		require.Len(t, connections.Generic, 2)
		require.Equal(t, "conn-a", connections.Generic[0].Name)
		require.Equal(t, "conn-b", connections.Generic[1].Name)
		require.Equal(t, []string{"projects/" + testGCPProject}, mock.listed)
	})

	t.Run("filters by prefix and strips it", func(t *testing.T) {
		t.Parallel()
		mock := &mockGCPSecretManagerClient{
			secrets: map[string]string{"bruin-prod-conn-a": testGenericSecretJSON},
			// Only IDs that start with the exact prefix, and have something after it, are connections.
			listIDs: []string{"bruin-prod-conn-a", "Bruin-prod-x", "bruin-prod-", "other-bruin-prod-y"},
		}
		c := newTestGCPClient(mock, "bruin-prod-")

		connections, err := c.GetConnectionsConfig()
		require.NoError(t, err)
		require.Len(t, connections.Generic, 1)
		require.Equal(t, "conn-a", connections.Generic[0].Name)
		require.Equal(t, []string{gcpVersionName("bruin-prod-conn-a", "latest")}, mock.requested)
	})

	t.Run("skips secrets whose version is disabled", func(t *testing.T) {
		t.Parallel()
		mock := &mockGCPSecretManagerClient{
			err:     status.Error(codes.FailedPrecondition, "SecretVersion is in DISABLED state"),
			listIDs: []string{"conn-a"},
		}

		connections, err := newTestGCPClient(mock, "").GetConnectionsConfig()
		require.NoError(t, err)
		require.Empty(t, connections.Generic)
	})

	t.Run("returns read errors", func(t *testing.T) {
		t.Parallel()
		mock := &mockGCPSecretManagerClient{
			err:     status.Error(codes.PermissionDenied, "denied"),
			listIDs: []string{"conn-a"},
		}

		_, err := newTestGCPClient(mock, "").GetConnectionsConfig()
		require.ErrorContains(t, err, "secretmanager.versions.access")
	})

	t.Run("names the missing list permission", func(t *testing.T) {
		t.Parallel()
		c := newTestGCPClient(&mockGCPSecretManagerClient{listErr: status.Error(codes.PermissionDenied, "Permission 'secretmanager.secrets.list' denied")}, "")

		_, err := c.GetConnectionsConfig()
		require.ErrorContains(t, err, "permission denied listing secrets in GCP project 'my-project'")
		require.ErrorContains(t, err, "secretmanager.secrets.list")
	})

	t.Run("returns other list errors", func(t *testing.T) {
		t.Parallel()
		c := newTestGCPClient(&mockGCPSecretManagerClient{listErr: status.Error(codes.Unavailable, "connection reset")}, "")

		_, err := c.GetConnectionsConfig()
		require.ErrorContains(t, err, "failed to list secrets in GCP project 'my-project'")
		require.ErrorContains(t, err, "connection reset")
	})
}

func TestNewGCPSecretManagerClientFromEnv(t *testing.T) {
	t.Run("reads the project, prefix and version", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("BRUIN_GCP_CREDENTIALS_JSON", testGCPServiceAccountJSON("key-project"))
		t.Setenv("BRUIN_GCP_PROJECT", "secrets-project")
		t.Setenv("BRUIN_GCP_SECRET_PREFIX", "bruin_prod-")
		t.Setenv("BRUIN_GCP_SECRET_VERSION", "3")

		client, err := NewGCPSecretManagerClientFromEnv(context.Background(), &mockLogger{})
		require.NoError(t, err)
		require.Equal(t, "secrets-project", client.project)
		require.Equal(t, "bruin_prod-", client.prefix)
		require.Equal(t, "3", client.version)
	})

	t.Run("defaults to the latest version", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("BRUIN_GCP_CREDENTIALS_JSON", testGCPServiceAccountJSON("key-project"))

		client, err := NewGCPSecretManagerClientFromEnv(context.Background(), &mockLogger{})
		require.NoError(t, err)
		require.Equal(t, "latest", client.version)
		require.Empty(t, client.prefix)
	})

	t.Run("rejects an invalid prefix", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("BRUIN_GCP_SECRET_PREFIX", "bruin/prod/")

		_, err := NewGCPSecretManagerClientFromEnv(context.Background(), &mockLogger{})
		require.ErrorContains(t, err, "BRUIN_GCP_SECRET_PREFIX 'bruin/prod/' is not valid")
	})

	t.Run("rejects an invalid version", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("BRUIN_GCP_SECRET_VERSION", "1/../2")

		_, err := NewGCPSecretManagerClientFromEnv(context.Background(), &mockLogger{})
		require.ErrorContains(t, err, "BRUIN_GCP_SECRET_VERSION '1/../2' is not valid")
	})
}

func TestGCPCredentials_FromBruinEnv(t *testing.T) {
	t.Run("loads a service account key from BRUIN_GCP_CREDENTIALS_JSON", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("BRUIN_GCP_CREDENTIALS_JSON", testGCPServiceAccountJSON("key-project"))
		log := &messageLogger{}

		creds, project, err := loadGCPCredentialsFromEnv(context.Background(), log)
		require.NoError(t, err)
		require.NotNil(t, creds)
		require.Equal(t, "key-project", project)
		require.Contains(t, log.debugs, "GCP Secret Manager: using service_account credentials from BRUIN_GCP_CREDENTIALS_JSON")
		requireNoGCPSecretValues(t, log.debugs)
	})

	t.Run("loads an external account config from BRUIN_GCP_CREDENTIALS_FILE", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("BRUIN_GCP_CREDENTIALS_FILE", writeGCPCredentialsFile(t, testGCPExternalAccountJSON(t, "https://sts.googleapis.com/v1/token", "", "quota-project")))
		log := &messageLogger{}

		creds, project, err := loadGCPCredentialsFromEnv(context.Background(), log)
		require.NoError(t, err)
		require.NotNil(t, creds)
		require.Equal(t, "quota-project", project, "falls back to the quota project")
		require.Contains(t, log.debugs, "GCP Secret Manager: using external_account credentials from BRUIN_GCP_CREDENTIALS_FILE")
	})

	t.Run("loads authorized user credentials", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("BRUIN_GCP_CREDENTIALS_JSON", `{"type": "authorized_user", "client_id": "id", "client_secret": "secret", "refresh_token": "refresh", "quota_project_id": "user-project"}`)

		_, project, err := loadGCPCredentialsFromEnv(context.Background(), &mockLogger{})
		require.NoError(t, err)
		require.Equal(t, "user-project", project)
	})

	t.Run("loads impersonated service account configs", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("BRUIN_GCP_PROJECT", "secrets-project")
		t.Setenv("BRUIN_GCP_CREDENTIALS_JSON", `{
			"type": "impersonated_service_account",
			"service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/reader@secrets-project.iam.gserviceaccount.com:generateAccessToken",
			"source_credentials": {"type": "authorized_user", "client_id": "id", "client_secret": "secret", "refresh_token": "refresh"}
		}`)

		creds, project, err := loadGCPCredentialsFromEnv(context.Background(), &mockLogger{})
		require.NoError(t, err)
		require.NotNil(t, creds)
		require.Equal(t, "secrets-project", project)
	})

	t.Run("BRUIN credentials win over GOOGLE_APPLICATION_CREDENTIALS", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", writeGCPCredentialsFile(t, testGCPServiceAccountJSON("adc-project")))
		t.Setenv("BRUIN_GCP_CREDENTIALS_JSON", testGCPServiceAccountJSON("bruin-project"))

		_, project, err := loadGCPCredentialsFromEnv(context.Background(), &mockLogger{})
		require.NoError(t, err)
		require.Equal(t, "bruin-project", project)
	})

	t.Run("rejects both JSON and file", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("BRUIN_GCP_CREDENTIALS_JSON", testGCPServiceAccountJSON("p"))
		t.Setenv("BRUIN_GCP_CREDENTIALS_FILE", writeGCPCredentialsFile(t, testGCPServiceAccountJSON("p")))

		_, _, err := loadGCPCredentialsFromEnv(context.Background(), &mockLogger{})
		require.ErrorContains(t, err, "set only one of BRUIN_GCP_CREDENTIALS_JSON and BRUIN_GCP_CREDENTIALS_FILE")
	})

	t.Run("rejects unknown credential types", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("BRUIN_GCP_CREDENTIALS_JSON", `{"type": "api_key", "key": "abc"}`)

		_, _, err := loadGCPCredentialsFromEnv(context.Background(), &mockLogger{})
		require.ErrorContains(t, err, "invalid GCP credentials in BRUIN_GCP_CREDENTIALS_JSON: unsupported credential type 'api_key'")
		require.NotContains(t, err.Error(), "abc")
	})

	t.Run("rejects JSON that isn't JSON", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("BRUIN_GCP_CREDENTIALS_JSON", "not-json-secret")

		_, _, err := loadGCPCredentialsFromEnv(context.Background(), &mockLogger{})
		require.ErrorContains(t, err, "credentials are not valid JSON")
		require.NotContains(t, err.Error(), "not-json-secret")
	})

	t.Run("reports an unreadable credentials file", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("BRUIN_GCP_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "missing.json"))

		_, _, err := loadGCPCredentialsFromEnv(context.Background(), &mockLogger{})
		require.ErrorContains(t, err, "failed to read BRUIN_GCP_CREDENTIALS_FILE")
	})
}

func TestGCPCredentials_ApplicationDefault(t *testing.T) {
	t.Run("workload identity federation via GOOGLE_APPLICATION_CREDENTIALS", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", writeGCPCredentialsFile(t, testGCPExternalAccountJSON(t, "https://sts.googleapis.com/v1/token", "", "")))
		t.Setenv("BRUIN_GCP_PROJECT", "secrets-project")
		log := &messageLogger{}

		creds, project, err := loadGCPCredentialsFromEnv(context.Background(), log)
		require.NoError(t, err)
		require.NotNil(t, creds)
		require.Equal(t, "secrets-project", project)
		require.Contains(t, log.debugs, "GCP Secret Manager: using external_account credentials from GOOGLE_APPLICATION_CREDENTIALS")
	})

	t.Run("workload identity federation exchanges the pod token without a key", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		google := newFakeGoogleTokenServer(t)
		// What `gcloud iam workload-identity-pools create-cred-config --service-account` writes, pointed at a local STS.
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", writeGCPCredentialsFile(t, testGCPExternalAccountJSON(
			t,
			google.URL+"/v1/token",
			google.URL+"/v1/projects/-/serviceAccounts/reader@secrets-project.iam.gserviceaccount.com:generateAccessToken",
			"",
		)))
		t.Setenv("GOOGLE_CLOUD_PROJECT", "secrets-project")

		creds, project, err := loadGCPCredentialsFromEnv(context.Background(), &mockLogger{})
		require.NoError(t, err)
		require.Equal(t, "secrets-project", project)

		token, err := creds.Token(context.Background())
		require.NoError(t, err)
		require.Equal(t, "impersonated-token", token.Value)

		exchange := google.request("/v1/token")
		require.Equal(t, "eks-pod-token", exchange.Get("subject_token"))
		require.Equal(t, "urn:ietf:params:oauth:token-type:jwt", exchange.Get("subject_token_type"))
		require.Equal(t, testGCPWorkloadIdentityAudience, exchange.Get("audience"))
		require.Equal(t, "Bearer federated-token", google.header("/v1/projects/-/serviceAccounts/reader@secrets-project.iam.gserviceaccount.com:generateAccessToken"))
	})

	t.Run("GKE workload identity uses the metadata server", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		metadata := newFakeGCEMetadataServer(t, "gke-project")
		t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(metadata.URL, "http://"))
		log := &messageLogger{}

		creds, project, err := loadGCPCredentialsFromEnv(context.Background(), log)
		require.NoError(t, err)
		require.Equal(t, "gke-project", project, "falls back to the metadata server's project")
		require.Contains(t, log.debugs, "GCP Secret Manager: using credentials from the metadata server (GKE Workload Identity, GCE or Cloud Run)")

		token, err := creds.Token(context.Background())
		require.NoError(t, err)
		require.Equal(t, "metadata-token", token.Value)
	})

	t.Run("BRUIN_GCP_PROJECT wins over the credentials' project", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", writeGCPCredentialsFile(t, testGCPServiceAccountJSON("key-project")))
		t.Setenv("GOOGLE_CLOUD_PROJECT", "env-project")
		t.Setenv("BRUIN_GCP_PROJECT", "bruin-project")

		_, project, err := loadGCPCredentialsFromEnv(context.Background(), &mockLogger{})
		require.NoError(t, err)
		require.Equal(t, "bruin-project", project)
	})

	t.Run("errors clearly if no project can be resolved", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", writeGCPCredentialsFile(t, testGCPExternalAccountJSON(t, "https://sts.googleapis.com/v1/token", "", "")))

		_, _, err := loadGCPCredentialsFromEnv(context.Background(), &mockLogger{})
		require.ErrorContains(t, err, "no GCP project configured for GCP Secret Manager: set BRUIN_GCP_PROJECT")
	})
}

func TestGCPCredentials_Impersonation(t *testing.T) {
	t.Run("impersonates on top of the resolved credentials", func(t *testing.T) {
		clearGCPSecretManagerEnv(t)
		t.Setenv("BRUIN_GCP_CREDENTIALS_JSON", testGCPServiceAccountJSON("key-project"))
		t.Setenv("BRUIN_GCP_IMPERSONATE_SERVICE_ACCOUNT", "reader@secrets-project.iam.gserviceaccount.com")
		log := &messageLogger{}

		creds, project, err := loadGCPCredentialsFromEnv(context.Background(), log)
		require.NoError(t, err)
		require.Nil(t, creds.JSON(), "impersonated credentials replace the key file credentials")
		require.Equal(t, "key-project", project, "the project comes from the base credentials")
		require.Contains(t, log.debugs, "GCP Secret Manager: impersonating service account reader@secrets-project.iam.gserviceaccount.com")
	})
}

const testGCPWorkloadIdentityAudience = "//iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/bruin/providers/eks"

func testGCPServiceAccountJSON(project string) string {
	return fmt.Sprintf(`{
		"type": "service_account",
		"project_id": %q,
		"private_key_id": "key-id",
		"private_key": "-----BEGIN PRIVATE KEY-----\nTEST-PRIVATE-KEY\n-----END PRIVATE KEY-----\n",
		"client_email": "bruin@%s.iam.gserviceaccount.com",
		"client_id": "1234567890",
		"token_uri": "https://oauth2.googleapis.com/token"
	}`, project, project)
}

// testGCPExternalAccountJSON builds a workload identity federation config that reads the
// subject token from a file, the way an EKS or AKS projected ServiceAccount token is mounted.
func testGCPExternalAccountJSON(t *testing.T, tokenURL, impersonationURL, quotaProject string) string {
	t.Helper()

	config := map[string]any{
		"type":               "external_account",
		"audience":           testGCPWorkloadIdentityAudience,
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
		"token_url":          tokenURL,
		"credential_source": map[string]any{
			"file":   writeTempFile(t, "eks-pod-token"),
			"format": map[string]string{"type": "text"},
		},
	}
	if impersonationURL != "" {
		config["service_account_impersonation_url"] = impersonationURL
	}
	if quotaProject != "" {
		config["quota_project_id"] = quotaProject
	}

	payload, err := json.Marshal(config)
	require.NoError(t, err)

	return string(payload)
}

func writeGCPCredentialsFile(t *testing.T, contents string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "credentials.json")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))

	return path
}

func requireNoGCPSecretValues(t *testing.T, messages []string) {
	t.Helper()

	for _, message := range messages {
		require.NotContains(t, message, "PRIVATE KEY")
		require.NotContains(t, message, "key-id")
	}
}

// fakeGoogleTokenServer answers the STS token exchange and the IAM Credentials
// generateAccessToken call, and records each request.
type fakeGoogleTokenServer struct {
	*httptest.Server
	mu       sync.Mutex
	forms    map[string]url.Values
	authHdrs map[string]string
}

func newFakeGoogleTokenServer(t *testing.T) *fakeGoogleTokenServer {
	t.Helper()

	f := &fakeGoogleTokenServer{forms: map[string]url.Values{}, authHdrs: map[string]string{}}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.forms[r.URL.Path] = r.PostForm
		f.authHdrs[r.URL.Path] = r.Header.Get("Authorization")
		f.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/token":
			_, _ = w.Write([]byte(`{"access_token": "federated-token", "issued_token_type": "urn:ietf:params:oauth:token-type:access_token", "token_type": "Bearer", "expires_in": 3600}`))
		case strings.HasSuffix(r.URL.Path, ":generateAccessToken"):
			_, _ = w.Write([]byte(`{"accessToken": "impersonated-token", "expireTime": "2099-01-01T00:00:00Z"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.Close)

	return f
}

func (f *fakeGoogleTokenServer) request(path string) url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.forms[path]
}

func (f *fakeGoogleTokenServer) header(path string) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.authHdrs[path]
}

// newFakeGCEMetadataServer answers the metadata server calls GKE Workload Identity and GCE credentials make.
func newFakeGCEMetadataServer(t *testing.T, project string) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "missing Metadata-Flavor header", http.StatusForbidden)
			return
		}
		w.Header().Set("Metadata-Flavor", "Google")
		switch r.URL.Path {
		case "/computeMetadata/v1/project/project-id":
			_, _ = w.Write([]byte(project))
		case "/computeMetadata/v1/instance/service-accounts/default/token":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token": "metadata-token", "expires_in": 3600, "token_type": "Bearer"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	return server
}

func clearGCPSecretManagerEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"BRUIN_GCP_CREDENTIALS_FILE",
		"BRUIN_GCP_CREDENTIALS_JSON",
		"BRUIN_GCP_IMPERSONATE_SERVICE_ACCOUNT",
		"BRUIN_GCP_PROJECT",
		"BRUIN_GCP_SECRET_PREFIX",
		"BRUIN_GCP_SECRET_VERSION",
		"GOOGLE_APPLICATION_CREDENTIALS",
		"GOOGLE_CLOUD_PROJECT",
		"GOOGLE_CLOUD_QUOTA_PROJECT",
		"GOOGLE_API_USE_CLIENT_CERTIFICATE",
		"GOOGLE_AUTH_TRUST_BOUNDARY_ENABLED",
		"GCE_METADATA_HOST",
	} {
		unsetenv(t, key)
	}
	// Keep the developer's gcloud application default credentials out of the tests.
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("APPDATA", home)
	t.Setenv("CLOUDSDK_CONFIG", home)
}
