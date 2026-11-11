package secrets

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go-v2/credentials/endpointcreds"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

const testGenericSecretJSON = `{"details": {"value": "somevalue"}, "type": "generic"}`

func TestNewAWSSecretsManagerClient(t *testing.T) {
	t.Parallel()
	log := &mockLogger{}

	t.Run("returns error if access key ID is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewAWSSecretsManagerClient(log, "", "secret", "us-east-1", "")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty AWS access key ID")
	})

	t.Run("returns error if secret access key is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewAWSSecretsManagerClient(log, "key", "", "us-east-1", "")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty AWS secret access key")
	})

	t.Run("returns error if region is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewAWSSecretsManagerClient(log, "key", "secret", "", "")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty AWS region")
	})

	t.Run("creates client with valid credentials", func(t *testing.T) {
		t.Parallel()
		client, err := NewAWSSecretsManagerClient(log, "key", "secret", "us-east-1", "")
		require.NoError(t, err)
		require.NotNil(t, client)
	})
}

func TestNewAWSSecretsManagerClientFromEnv(t *testing.T) {
	log := &mockLogger{}

	t.Run("falls back to default AWS config chain", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "default-key")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "default-secret")
		t.Setenv("AWS_SESSION_TOKEN", "default-token")
		t.Setenv("AWS_REGION", "eu-west-1")

		client, err := NewAWSSecretsManagerClientFromEnv(context.Background(), log)
		require.NoError(t, err)
		require.NotNil(t, client)

		opts := requireSecretsManagerClientOptions(t, client)
		require.Equal(t, "eu-west-1", opts.Region)

		creds, err := opts.Credentials.Retrieve(context.Background())
		require.NoError(t, err)
		require.Equal(t, "default-key", creds.AccessKeyID)
		require.Equal(t, "default-secret", creds.SecretAccessKey)
		require.Equal(t, "default-token", creds.SessionToken)
	})

	t.Run("uses BRUIN static credentials as override", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "default-key")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "default-secret")
		t.Setenv("AWS_SESSION_TOKEN", "default-token")
		t.Setenv("AWS_REGION", "eu-west-1")
		t.Setenv("BRUIN_AWS_ACCESS_KEY_ID", "bruin-key")
		t.Setenv("BRUIN_AWS_SECRET_ACCESS_KEY", "bruin-secret")
		t.Setenv("BRUIN_AWS_SESSION_TOKEN", "bruin-token")
		t.Setenv("BRUIN_AWS_REGION", "us-east-2")

		client, err := NewAWSSecretsManagerClientFromEnv(context.Background(), log)
		require.NoError(t, err)
		require.NotNil(t, client)

		opts := requireSecretsManagerClientOptions(t, client)
		require.Equal(t, "us-east-2", opts.Region)

		creds, err := opts.Credentials.Retrieve(context.Background())
		require.NoError(t, err)
		require.Equal(t, "bruin-key", creds.AccessKeyID)
		require.Equal(t, "bruin-secret", creds.SecretAccessKey)
		require.Equal(t, "bruin-token", creds.SessionToken)
	})

	t.Run("errors if BRUIN access key is missing for static credentials", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "default-key")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "default-secret")
		t.Setenv("AWS_REGION", "eu-west-1")
		t.Setenv("BRUIN_AWS_SECRET_ACCESS_KEY", "bruin-secret")

		client, err := NewAWSSecretsManagerClientFromEnv(context.Background(), log)
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "BRUIN_AWS_ACCESS_KEY_ID env variable not set")
	})

	t.Run("errors if BRUIN secret key is missing for static credentials", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "default-key")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "default-secret")
		t.Setenv("AWS_REGION", "eu-west-1")
		t.Setenv("BRUIN_AWS_ACCESS_KEY_ID", "bruin-key")

		client, err := NewAWSSecretsManagerClientFromEnv(context.Background(), log)
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "BRUIN_AWS_SECRET_ACCESS_KEY env variable not set")
	})

	t.Run("errors clearly if no region can be resolved", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		t.Setenv("BRUIN_AWS_ACCESS_KEY_ID", "bruin-key")
		t.Setenv("BRUIN_AWS_SECRET_ACCESS_KEY", "bruin-secret")

		client, err := NewAWSSecretsManagerClientFromEnv(context.Background(), log)
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "no AWS region configured")
		require.Contains(t, err.Error(), "BRUIN_AWS_REGION")
	})

	t.Run("reads the secret prefix", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "default-key")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "default-secret")
		t.Setenv("AWS_REGION", "eu-west-1")
		t.Setenv("BRUIN_AWS_SECRET_PREFIX", "bruin/prod/")

		client, err := NewAWSSecretsManagerClientFromEnv(context.Background(), log)
		require.NoError(t, err)
		require.Equal(t, "bruin/prod/", client.prefix)
	})
}

func TestAWSCredentialChain_WorkloadIdentity(t *testing.T) {
	t.Run("IRSA uses the web identity token file", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		t.Setenv("AWS_REGION", "eu-west-1")
		t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::111111111111:role/pod-role")
		t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", writeTempFile(t, "irsa-token"))

		cfg, err := loadAWSConfigFromEnv(context.Background(), &mockLogger{})
		require.NoError(t, err)
		require.True(t, aws.IsCredentialsProvider(cfg.Credentials, &stscreds.WebIdentityRoleProvider{}))
		require.Contains(t, providerSources(cfg.Credentials), aws.CredentialSourceEnvVarsSTSWebIDToken)
	})

	t.Run("IRSA credentials are exchanged with STS", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		sts := newFakeSTS(t)
		t.Setenv("AWS_REGION", "eu-west-1")
		t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
		t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::111111111111:role/pod-role")
		t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", writeTempFile(t, "irsa-token"))
		log := &messageLogger{}

		cfg, err := loadAWSConfigFromEnv(context.Background(), log)
		require.NoError(t, err)

		creds, err := cfg.Credentials.Retrieve(context.Background())
		require.NoError(t, err)
		require.Equal(t, "web-identity-key", creds.AccessKeyID)
		require.Equal(t, stscreds.WebIdentityProviderName, creds.Source)
		require.Equal(t, []string{"AssumeRoleWithWebIdentity"}, sts.actions())
		require.Equal(t, "irsa-token", sts.request(0).Get("WebIdentityToken"))
		require.Equal(t, "arn:aws:iam::111111111111:role/pod-role", sts.request(0).Get("RoleArn"))

		require.Contains(t, log.debugs, "AWS Secrets Manager: using credentials from "+stscreds.WebIdentityProviderName)
		requireNoCredentialValues(t, log.debugs, creds)
	})

	t.Run("EKS Pod Identity uses the container credentials endpoint and token file", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		var authHeader atomic.Value
		agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader.Store(r.Header.Get("Authorization"))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"AccessKeyId":"pod-identity-key","SecretAccessKey":"pod-identity-secret","Token":"pod-identity-token","Expiration":"2099-01-01T00:00:00Z"}`))
		}))
		t.Cleanup(agent.Close)

		t.Setenv("AWS_REGION", "eu-west-1")
		// EKS injects http://169.254.170.23/v1/credentials; the SDK allows loopback hosts too, which keeps this test local.
		t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", agent.URL+"/v1/credentials")
		t.Setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", writeTempFile(t, "pod-identity-token"))
		log := &messageLogger{}

		cfg, err := loadAWSConfigFromEnv(context.Background(), log)
		require.NoError(t, err)
		require.True(t, aws.IsCredentialsProvider(cfg.Credentials, &endpointcreds.Provider{}))
		require.Contains(t, providerSources(cfg.Credentials), aws.CredentialSourceHTTP)

		creds, err := cfg.Credentials.Retrieve(context.Background())
		require.NoError(t, err)
		require.Equal(t, "pod-identity-key", creds.AccessKeyID)
		require.Equal(t, endpointcreds.ProviderName, creds.Source)
		require.Equal(t, "pod-identity-token", authHeader.Load())

		require.Contains(t, log.debugs, "AWS Secrets Manager: using credentials from "+endpointcreds.ProviderName)
		requireNoCredentialValues(t, log.debugs, creds)
	})

	t.Run("ECS task role uses the container credentials endpoint", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		t.Setenv("AWS_REGION", "eu-west-1")
		t.Setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/task")

		cfg, err := loadAWSConfigFromEnv(context.Background(), &mockLogger{})
		require.NoError(t, err)
		require.True(t, aws.IsCredentialsProvider(cfg.Credentials, &endpointcreds.Provider{}))
	})

	t.Run("EC2 instance profile is the last resort", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		t.Setenv("AWS_REGION", "eu-west-1")

		cfg, err := loadAWSConfigFromEnv(context.Background(), &mockLogger{})
		require.NoError(t, err)
		require.True(t, aws.IsCredentialsProvider(cfg.Credentials, &ec2rolecreds.Provider{}))
		require.Contains(t, providerSources(cfg.Credentials), aws.CredentialSourceIMDS)
	})

	t.Run("BRUIN static credentials win over IRSA", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		t.Setenv("AWS_REGION", "eu-west-1")
		t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::111111111111:role/pod-role")
		t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", writeTempFile(t, "irsa-token"))
		t.Setenv("BRUIN_AWS_ACCESS_KEY_ID", "bruin-key")
		t.Setenv("BRUIN_AWS_SECRET_ACCESS_KEY", "bruin-secret")

		cfg, err := loadAWSConfigFromEnv(context.Background(), &mockLogger{})
		require.NoError(t, err)
		require.True(t, aws.IsCredentialsProvider(cfg.Credentials, credentials.StaticCredentialsProvider{}))

		creds, err := cfg.Credentials.Retrieve(context.Background())
		require.NoError(t, err)
		require.Equal(t, "bruin-key", creds.AccessKeyID)
	})
}

func TestAWSCredentialChain_AssumeRole(t *testing.T) {
	t.Run("assumes BRUIN_AWS_ROLE_ARN with static credentials", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		sts := newFakeSTS(t)
		t.Setenv("AWS_REGION", "eu-west-1")
		t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
		t.Setenv("BRUIN_AWS_ACCESS_KEY_ID", "bruin-key")
		t.Setenv("BRUIN_AWS_SECRET_ACCESS_KEY", "bruin-secret")
		t.Setenv("BRUIN_AWS_ROLE_ARN", "arn:aws:iam::222222222222:role/secrets-reader")
		t.Setenv("BRUIN_AWS_EXTERNAL_ID", "ext-123")
		t.Setenv("BRUIN_AWS_ROLE_SESSION_NAME", "bruin-prod")

		cfg, err := loadAWSConfigFromEnv(context.Background(), &mockLogger{})
		require.NoError(t, err)
		require.True(t, aws.IsCredentialsProvider(cfg.Credentials, &stscreds.AssumeRoleProvider{}))

		creds, err := cfg.Credentials.Retrieve(context.Background())
		require.NoError(t, err)
		require.Equal(t, "assumed-role-key", creds.AccessKeyID)
		require.Equal(t, []string{"AssumeRole"}, sts.actions())
		form := sts.request(0)
		require.Equal(t, "arn:aws:iam::222222222222:role/secrets-reader", form.Get("RoleArn"))
		require.Equal(t, "ext-123", form.Get("ExternalId"))
		require.Equal(t, "bruin-prod", form.Get("RoleSessionName"))
		require.Contains(t, sts.authorization(0), "Credential=bruin-key/")
	})

	t.Run("chains BRUIN_AWS_ROLE_ARN onto IRSA", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		sts := newFakeSTS(t)
		t.Setenv("AWS_REGION", "eu-west-1")
		t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
		t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::111111111111:role/pod-role")
		t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", writeTempFile(t, "irsa-token"))
		t.Setenv("BRUIN_AWS_ROLE_ARN", "arn:aws:iam::222222222222:role/secrets-reader")

		cfg, err := loadAWSConfigFromEnv(context.Background(), &mockLogger{})
		require.NoError(t, err)

		creds, err := cfg.Credentials.Retrieve(context.Background())
		require.NoError(t, err)
		require.Equal(t, "assumed-role-key", creds.AccessKeyID)
		require.Equal(t, []string{"AssumeRoleWithWebIdentity", "AssumeRole"}, sts.actions())
		require.Equal(t, "arn:aws:iam::111111111111:role/pod-role", sts.request(0).Get("RoleArn"))
		require.Equal(t, "arn:aws:iam::222222222222:role/secrets-reader", sts.request(1).Get("RoleArn"))
		require.Equal(t, defaultAWSRoleSessionName, sts.request(1).Get("RoleSessionName"))
		require.Empty(t, sts.request(1).Get("ExternalId"))
		require.Contains(t, sts.authorization(1), "Credential=web-identity-key/")
	})

	t.Run("rejects a role ARN that isn't an IAM role", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		t.Setenv("AWS_REGION", "eu-west-1")
		t.Setenv("BRUIN_AWS_ROLE_ARN", "arn:aws:iam::222222222222:user/someone")

		_, err := loadAWSConfigFromEnv(context.Background(), &mockLogger{})
		require.ErrorContains(t, err, "BRUIN_AWS_ROLE_ARN must be an IAM role ARN")
	})

	t.Run("rejects role options without a role", func(t *testing.T) {
		clearAWSSecretsManagerEnv(t)
		t.Setenv("AWS_REGION", "eu-west-1")
		t.Setenv("BRUIN_AWS_EXTERNAL_ID", "ext-123")

		_, err := loadAWSConfigFromEnv(context.Background(), &mockLogger{})
		require.ErrorContains(t, err, "require BRUIN_AWS_ROLE_ARN")
	})
}

func clearAWSSecretsManagerEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"BRUIN_AWS_ACCESS_KEY_ID",
		"BRUIN_AWS_SECRET_ACCESS_KEY",
		"BRUIN_AWS_SESSION_TOKEN",
		"BRUIN_AWS_REGION",
		"BRUIN_AWS_SECRET_PREFIX",
		"BRUIN_AWS_ROLE_ARN",
		"BRUIN_AWS_EXTERNAL_ID",
		"BRUIN_AWS_ROLE_SESSION_NAME",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SESSION_TOKEN",
		"AWS_REGION",
		"AWS_DEFAULT_REGION",
		"AWS_PROFILE",
		"AWS_ROLE_ARN",
		"AWS_ROLE_SESSION_NAME",
		"AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
		"AWS_ENDPOINT_URL",
		"AWS_ENDPOINT_URL_STS",
	} {
		unsetenv(t, key)
	}
	// Keep the developer's ~/.aws files and the EC2 metadata service out of the tests.
	missing := filepath.Join(t.TempDir(), "missing")
	t.Setenv("AWS_CONFIG_FILE", missing)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", missing)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

//nolint:usetesting // testing.T has Setenv but no Unsetenv; this helper must restore after os.Unsetenv.
func unsetenv(t *testing.T, key string) {
	t.Helper()

	value, ok := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("failed to unset %s: %v", key, err)
	}

	t.Cleanup(func() {
		if ok {
			if err := os.Setenv(key, value); err != nil {
				t.Fatalf("failed to restore %s: %v", key, err)
			}
			return
		}
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("failed to restore unset %s: %v", key, err)
		}
	})
}

func writeTempFile(t *testing.T, contents string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))

	return path
}

func providerSources(provider aws.CredentialsProvider) []aws.CredentialSource {
	if sources, ok := provider.(aws.CredentialProviderSource); ok {
		return sources.ProviderSources()
	}

	return nil
}

func requireNoCredentialValues(t *testing.T, messages []string, creds aws.Credentials) {
	t.Helper()

	for _, message := range messages {
		require.NotContains(t, message, creds.AccessKeyID)
		require.NotContains(t, message, creds.SecretAccessKey)
		require.NotContains(t, message, creds.SessionToken)
	}
}

// fakeSTS answers AssumeRoleWithWebIdentity and AssumeRole and records each request.
type fakeSTS struct {
	*httptest.Server
	mu       sync.Mutex
	forms    []url.Values
	authHdrs []string
}

func newFakeSTS(t *testing.T) *fakeSTS {
	t.Helper()

	f := &fakeSTS{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.forms = append(f.forms, r.PostForm)
		f.authHdrs = append(f.authHdrs, r.Header.Get("Authorization"))
		f.mu.Unlock()

		action := r.PostForm.Get("Action")
		key := map[string]string{"AssumeRoleWithWebIdentity": "web-identity-key", "AssumeRole": "assumed-role-key"}[action]
		if key == "" {
			http.Error(w, "unexpected action "+action, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/xml")
		_, _ = fmt.Fprintf(w, `<%[1]sResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><%[1]sResult><Credentials>`+
			`<AccessKeyId>%[2]s</AccessKeyId><SecretAccessKey>%[2]s-secret</SecretAccessKey><SessionToken>%[2]s-token</SessionToken>`+
			`<Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></%[1]sResult></%[1]sResponse>`, action, key)
	}))
	t.Cleanup(f.Close)

	return f
}

func (f *fakeSTS) actions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	actions := make([]string, 0, len(f.forms))
	for _, form := range f.forms {
		actions = append(actions, strings.Join(form["Action"], ","))
	}

	return actions
}

func (f *fakeSTS) request(i int) url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.forms[i]
}

func (f *fakeSTS) authorization(i int) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.authHdrs[i]
}

func requireSecretsManagerClientOptions(t *testing.T, client *AWSSecretsManagerClient) secretsmanager.Options {
	t.Helper()

	secretsManagerClient, ok := client.client.(*secretsmanager.Client)
	require.True(t, ok)

	return secretsManagerClient.Options()
}

// mockAWSSecretsManagerClient serves secrets from a map of secret name to value, or a fixed response/error.
type mockAWSSecretsManagerClient struct {
	response *secretsmanager.GetSecretValueOutput
	err      error
	secrets  map[string]string

	// listPages are the secret names returned by successive ListSecrets calls.
	listPages [][]string
	listErr   error

	mu          sync.Mutex
	requested   []string
	listFilters []types.Filter
}

func (m *mockAWSSecretsManagerClient) GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	m.mu.Lock()
	m.requested = append(m.requested, aws.ToString(params.SecretId))
	m.mu.Unlock()

	if m.secrets != nil {
		value, ok := m.secrets[aws.ToString(params.SecretId)]
		if !ok {
			return nil, &types.ResourceNotFoundException{Message: aws.String("Secrets Manager can't find the specified secret.")}
		}
		return &secretsmanager.GetSecretValueOutput{SecretString: aws.String(value)}, nil
	}

	return m.response, m.err
}

func (m *mockAWSSecretsManagerClient) ListSecrets(ctx context.Context, params *secretsmanager.ListSecretsInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.ListSecretsOutput, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}

	m.mu.Lock()
	m.listFilters = params.Filters
	m.mu.Unlock()

	page := 0
	if params.NextToken != nil {
		page, _ = strconv.Atoi(*params.NextToken)
	}
	out := &secretsmanager.ListSecretsOutput{}
	if page < len(m.listPages) {
		for _, name := range m.listPages[page] {
			out.SecretList = append(out.SecretList, types.SecretListEntry{Name: aws.String(name)})
		}
	}
	if page+1 < len(m.listPages) {
		out.NextToken = aws.String(strconv.Itoa(page + 1))
	}

	return out, nil
}

func newTestAWSClient(mock *mockAWSSecretsManagerClient, prefix string) *AWSSecretsManagerClient {
	return newAWSSecretsManagerClient(context.Background(), &mockLogger{}, mock, prefix)
}

func TestAWSSecretsManagerClient_GetConnection_ReturnsConnection(t *testing.T) {
	t.Parallel()
	secretString := `{"details": {"username": "testuser", "password": "testpass", "host": "testhost", "port": 1337, "database": "testdb", "schema": "testschema"}, "type": "postgres"}`
	c := newTestAWSClient(&mockAWSSecretsManagerClient{
		response: &secretsmanager.GetSecretValueOutput{SecretString: aws.String(secretString)},
	}, "")

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
	require.Equal(t, "postgres", c.GetConnectionType("test-connection"))
}

func TestAWSSecretsManagerClient_GetConnection_ReturnsGenericConnection(t *testing.T) {
	t.Parallel()
	c := newTestAWSClient(&mockAWSSecretsManagerClient{
		response: &secretsmanager.GetSecretValueOutput{SecretString: aws.String(testGenericSecretJSON)},
	}, "")

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
	require.Equal(t, "somevalue", conn.(*config.GenericConnection).Value)
}

func TestAWSSecretsManagerClient_GetConnection_ReturnsError(t *testing.T) {
	t.Parallel()
	c := newTestAWSClient(&mockAWSSecretsManagerClient{err: errors.New("throttled")}, "")

	conn := c.GetConnection("missing-secret")
	require.Nil(t, conn)

	_, err := c.ResolveConnection("missing-secret")
	require.ErrorContains(t, err, "failed to read secret 'missing-secret' from AWS Secrets Manager")
	require.False(t, isSecretNotFound(err))
}

func TestAWSSecretsManagerClient_GetConnection_NotFound(t *testing.T) {
	t.Parallel()
	c := newTestAWSClient(&mockAWSSecretsManagerClient{secrets: map[string]string{}}, "")

	_, err := c.ResolveConnection("missing-secret")
	require.True(t, isSecretNotFound(err))
	require.ErrorContains(t, err, "secret 'missing-secret' not found in AWS Secrets Manager")
}

func TestAWSSecretsManagerClient_SharesOneFetchPerName(t *testing.T) {
	t.Parallel()
	mock := &mockAWSSecretsManagerClient{secrets: map[string]string{"test-connection": testGenericSecretJSON}}
	c := newTestAWSClient(mock, "")

	require.Equal(t, "generic", c.GetConnectionType("test-connection"))
	require.NotNil(t, c.GetConnection("test-connection"))
	require.NotNil(t, c.GetConnectionDetails("test-connection"))
	require.Equal(t, "generic", c.GetConnectionType("test-connection"))

	require.Equal(t, []string{"test-connection"}, mock.requested)
}

func TestAWSSecretsManagerClient_GetConnectionDetails_ReturnsDetails(t *testing.T) {
	t.Parallel()
	c := newTestAWSClient(&mockAWSSecretsManagerClient{
		response: &secretsmanager.GetSecretValueOutput{SecretString: aws.String(testGenericSecretJSON)},
	}, "")

	deets := c.GetConnectionDetails("test-connection")
	require.NotNil(t, deets)
	gc, ok := deets.(*config.GenericConnection)
	require.True(t, ok)
	require.Equal(t, "test-connection", gc.Name)
	require.Equal(t, "somevalue", gc.Value)
}

func TestAWSSecretsManagerClient_GetConnection_RejectsMalformedSecrets(t *testing.T) {
	t.Parallel()

	for name, output := range map[string]*secretsmanager.GetSecretValueOutput{
		"nil secret string": {SecretString: nil},
		"invalid JSON":      {SecretString: aws.String("not valid json")},
		"missing fields":    {SecretString: aws.String(`{"some": "data"}`)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := newTestAWSClient(&mockAWSSecretsManagerClient{response: output}, "")
			require.Nil(t, c.GetConnection("test-connection"))
			require.Empty(t, c.GetConnectionType("test-connection"))
		})
	}
}

func TestAWSSecretsManagerClient_Prefix(t *testing.T) {
	t.Parallel()
	mock := &mockAWSSecretsManagerClient{secrets: map[string]string{"bruin/prod/test-connection": testGenericSecretJSON}}
	c := newTestAWSClient(mock, "bruin/prod/")

	deets := c.GetConnectionDetails("test-connection")
	require.NotNil(t, deets)
	require.Equal(t, "test-connection", deets.(*config.GenericConnection).Name)
	require.Equal(t, []string{"bruin/prod/test-connection"}, mock.requested)

	_, err := c.ResolveConnection("other")
	require.True(t, isSecretNotFound(err))
	require.ErrorContains(t, err, "secret 'bruin/prod/other' not found")
}

func TestAWSSecretsManagerClient_GetConnectionsConfig(t *testing.T) {
	t.Parallel()

	t.Run("lists every page without a prefix", func(t *testing.T) {
		t.Parallel()
		mock := &mockAWSSecretsManagerClient{
			secrets: map[string]string{
				"conn-a":    testGenericSecretJSON,
				"conn-b":    testGenericSecretJSON,
				"unrelated": "plain text",
			},
			listPages: [][]string{{"conn-a"}, {"unrelated", "conn-b"}},
		}
		c := newTestAWSClient(mock, "")

		var lister config.ConnectionsConfigGetter = c
		connections, err := lister.GetConnectionsConfig()
		require.NoError(t, err)
		require.Len(t, connections.Generic, 2)
		require.Equal(t, "conn-a", connections.Generic[0].Name)
		require.Equal(t, "conn-b", connections.Generic[1].Name)
		require.Empty(t, mock.listFilters)
	})

	t.Run("filters by prefix and strips it", func(t *testing.T) {
		t.Parallel()
		mock := &mockAWSSecretsManagerClient{
			secrets: map[string]string{
				"bruin/prod/conn-a": testGenericSecretJSON,
			},
			// Only names that start with the exact prefix, and have something after it, are connections.
			listPages: [][]string{{"bruin/prod/conn-a", "Bruin/prod/x", "bruin/prod/"}},
		}
		c := newTestAWSClient(mock, "bruin/prod/")

		connections, err := c.GetConnectionsConfig()
		require.NoError(t, err)
		require.Len(t, connections.Generic, 1)
		require.Equal(t, "conn-a", connections.Generic[0].Name)
		require.Equal(t, []types.Filter{{Key: types.FilterNameStringTypeName, Values: []string{"bruin/prod/"}}}, mock.listFilters)
		require.Equal(t, []string{"bruin/prod/conn-a"}, mock.requested)
	})

	t.Run("returns list errors", func(t *testing.T) {
		t.Parallel()
		c := newTestAWSClient(&mockAWSSecretsManagerClient{listErr: errors.New("AccessDeniedException")}, "")

		_, err := c.GetConnectionsConfig()
		require.ErrorContains(t, err, "failed to list secrets in AWS Secrets Manager")
		require.ErrorContains(t, err, "AccessDeniedException")
	})
}
