package secrets

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestNewAWSSecretsManagerClient(t *testing.T) {
	t.Parallel()
	log := &mockLogger{}

	t.Run("returns error if access key ID is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewAWSSecretsManagerClient(log, "", "secret", "us-east-1")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty AWS access key ID")
	})

	t.Run("returns error if secret access key is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewAWSSecretsManagerClient(log, "key", "", "us-east-1")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty AWS secret access key")
	})

	t.Run("returns error if region is empty", func(t *testing.T) {
		t.Parallel()
		client, err := NewAWSSecretsManagerClient(log, "key", "secret", "")
		require.Error(t, err)
		require.Nil(t, client)
		require.Contains(t, err.Error(), "empty AWS region")
	})

	t.Run("creates client with valid credentials", func(t *testing.T) {
		t.Parallel()
		client, err := NewAWSSecretsManagerClient(log, "key", "secret", "us-east-1")
		require.NoError(t, err)
		require.NotNil(t, client)
	})
}

type mockAWSSecretsManagerClient struct {
	response *secretsmanager.GetSecretValueOutput
	err      error
}

func (m *mockAWSSecretsManagerClient) GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	return m.response, m.err
}

func TestAWSSecretsManagerClient_GetConnection_ReturnsConnection(t *testing.T) {
	t.Parallel()
	secretString := `{"details": {"username": "testuser", "password": "testpass", "host": "testhost", "port": 1337, "database": "testdb", "schema": "testschema"}, "type": "postgres"}`
	c := &AWSSecretsManagerClient{
		client: &mockAWSSecretsManagerClient{
			response: &secretsmanager.GetSecretValueOutput{
				SecretString: aws.String(secretString),
			},
			err: nil,
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
}

func TestAWSSecretsManagerClient_GetConnection_ReturnsGenericConnection(t *testing.T) {
	t.Parallel()
	secretString := `{"details": {"value": "somevalue"}, "type": "generic"}`
	c := &AWSSecretsManagerClient{
		client: &mockAWSSecretsManagerClient{
			response: &secretsmanager.GetSecretValueOutput{
				SecretString: aws.String(secretString),
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

func TestAWSSecretsManagerClient_GetConnection_ReturnsError(t *testing.T) {
	t.Parallel()
	c := &AWSSecretsManagerClient{
		client: &mockAWSSecretsManagerClient{
			response: nil,
			err:      errors.New("secret not found"),
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("missing-secret")
	require.Nil(t, conn)
}

func TestAWSSecretsManagerClient_GetConnection_ReturnsFromCache(t *testing.T) {
	t.Parallel()
	c := &AWSSecretsManagerClient{
		client: &mockAWSSecretsManagerClient{
			response: nil,
			err:      errors.New("test error"),
		},
		logger:           &mockLogger{},
		cacheConnections: map[string]any{"test-connection": []string{"some", "data", "not", "nil"}},
	}

	conn := c.GetConnection("test-connection")
	require.NotNil(t, conn)
	require.Equal(t, []string{"some", "data", "not", "nil"}, conn)
}

func TestAWSSecretsManagerClient_GetConnectionDetails_ReturnsDetails(t *testing.T) {
	t.Parallel()
	secretString := `{"details": {"value": "somevalue"}, "type": "generic"}`
	c := &AWSSecretsManagerClient{
		client: &mockAWSSecretsManagerClient{
			response: &secretsmanager.GetSecretValueOutput{
				SecretString: aws.String(secretString),
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

func TestAWSSecretsManagerClient_GetConnectionDetails_ReturnsFromCache(t *testing.T) {
	t.Parallel()
	c := &AWSSecretsManagerClient{
		client: &mockAWSSecretsManagerClient{
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

func TestAWSSecretsManagerClient_GetConnection_ReturnsErrorForNilSecretString(t *testing.T) {
	t.Parallel()
	c := &AWSSecretsManagerClient{
		client: &mockAWSSecretsManagerClient{
			response: &secretsmanager.GetSecretValueOutput{
				SecretString: nil,
			},
			err: nil,
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.Nil(t, conn)
}

func TestAWSSecretsManagerClient_GetConnection_ReturnsErrorForInvalidJSON(t *testing.T) {
	t.Parallel()
	c := &AWSSecretsManagerClient{
		client: &mockAWSSecretsManagerClient{
			response: &secretsmanager.GetSecretValueOutput{
				SecretString: aws.String("not valid json"),
			},
			err: nil,
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.Nil(t, conn)
}

func TestAWSSecretsManagerClient_GetConnection_ReturnsErrorForMissingFields(t *testing.T) {
	t.Parallel()
	c := &AWSSecretsManagerClient{
		client: &mockAWSSecretsManagerClient{
			response: &secretsmanager.GetSecretValueOutput{
				SecretString: aws.String(`{"some": "data"}`),
			},
			err: nil,
		},
		logger:           &mockLogger{},
		cacheConnections: make(map[string]any),
	}

	conn := c.GetConnection("test-connection")
	require.Nil(t, conn)
}
