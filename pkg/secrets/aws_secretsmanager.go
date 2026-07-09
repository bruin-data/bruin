package secrets

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/pkg/errors"
)

type awsSecretsManagerClient interface {
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

// AWSSecretsManagerClient manages secrets from AWS Secrets Manager.
type AWSSecretsManagerClient struct {
	client                  awsSecretsManagerClient
	logger                  logger.Logger
	cacheMu                 sync.RWMutex
	cacheConnections        map[string]any
	cacheConnectionsDetails map[string]any
}

// NewAWSSecretsManagerClientFromEnv creates a new AWS Secrets Manager client from environment variables.
func NewAWSSecretsManagerClientFromEnv(ctx context.Context, logger logger.Logger) (*AWSSecretsManagerClient, error) {
	accessKeyID := os.Getenv("BRUIN_AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("BRUIN_AWS_SECRET_ACCESS_KEY")
	if accessKeyID != "" && secretAccessKey == "" {
		return nil, errors.New("BRUIN_AWS_SECRET_ACCESS_KEY env variable not set")
	}
	if accessKeyID == "" && secretAccessKey != "" {
		return nil, errors.New("BRUIN_AWS_ACCESS_KEY_ID env variable not set")
	}

	region := os.Getenv("BRUIN_AWS_REGION")
	opts := []func(*awsconfig.LoadOptions) error{}
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}

	if accessKeyID != "" {
		sessionToken := os.Getenv("BRUIN_AWS_SESSION_TOKEN")
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken),
		))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load AWS config")
	}

	return newAWSSecretsManagerClientFromConfig(logger, cfg), nil
}

// NewAWSSecretsManagerClient creates a new AWS Secrets Manager client.
func NewAWSSecretsManagerClient(logger logger.Logger, accessKeyID, secretAccessKey, region, sessionToken string) (*AWSSecretsManagerClient, error) {
	if accessKeyID == "" {
		return nil, errors.New("empty AWS access key ID provided")
	}
	if secretAccessKey == "" {
		return nil, errors.New("empty AWS secret access key provided")
	}
	if region == "" {
		return nil, errors.New("empty AWS region provided")
	}

	cfg := aws.Config{
		Region: region,
		Credentials: credentials.NewStaticCredentialsProvider(
			accessKeyID,
			secretAccessKey,
			sessionToken,
		),
	}

	return newAWSSecretsManagerClientFromConfig(logger, cfg), nil
}

func newAWSSecretsManagerClientFromConfig(logger logger.Logger, cfg aws.Config) *AWSSecretsManagerClient {
	client := secretsmanager.NewFromConfig(cfg)

	return &AWSSecretsManagerClient{
		client:                  client,
		logger:                  logger,
		cacheConnections:        make(map[string]any),
		cacheConnectionsDetails: make(map[string]any),
	}
}

// GetConnection retrieves a connection by name from AWS Secrets Manager.
func (c *AWSSecretsManagerClient) GetConnection(name string) any {
	conn, err := c.ResolveConnection(name)
	if err != nil {
		c.logger.Errorf("%v", err)
		return nil
	}

	return conn
}

// ResolveConnection implements config.ConnectionResolver.
func (c *AWSSecretsManagerClient) ResolveConnection(name string) (any, error) {
	c.cacheMu.RLock()
	if conn, ok := c.cacheConnections[name]; ok {
		c.cacheMu.RUnlock()
		return conn, nil
	}
	c.cacheMu.RUnlock()

	manager, err := c.getAWSSecretsManager(name)
	if err != nil {
		return nil, err
	}

	conn := manager.GetConnection(name)

	c.cacheMu.Lock()
	c.cacheConnections[name] = conn
	c.cacheMu.Unlock()

	return conn, nil
}

// GetConnectionDetails retrieves connection details by name from AWS Secrets Manager.
func (c *AWSSecretsManagerClient) GetConnectionDetails(name string) any {
	c.cacheMu.RLock()
	if deets, ok := c.cacheConnectionsDetails[name]; ok {
		c.cacheMu.RUnlock()
		return deets
	}
	c.cacheMu.RUnlock()

	manager, err := c.getAWSSecretsManager(name)
	if err != nil {
		c.logger.Errorf("%v", err)
		return nil
	}

	deets := manager.GetConnectionDetails(name)

	c.cacheMu.Lock()
	c.cacheConnectionsDetails[name] = deets
	c.cacheMu.Unlock()

	return deets
}

func (c *AWSSecretsManagerClient) GetConnectionType(name string) string {
	manager, err := c.getAWSSecretsManager(name)
	if err != nil {
		return ""
	}
	return manager.GetConnectionType(name)
}

func (c *AWSSecretsManagerClient) getAWSSecretsManager(name string) (config.ConnectionAndDetailsGetter, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()

	result, err := c.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(name),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read secret '%s' from AWS Secrets Manager", name)
	}

	if result.SecretString == nil {
		return nil, errors.Errorf("secret '%s' has no string value", name)
	}

	var secretData map[string]any
	if err := json.Unmarshal([]byte(*result.SecretString), &secretData); err != nil {
		return nil, errors.Wrap(err, "failed to parse secret as JSON")
	}

	detailsRaw, okDetails := secretData["details"]
	secretType, okType := secretData["type"].(string)

	details, detailsIsMap := detailsRaw.(map[string]any)
	if !okDetails || !detailsIsMap || !okType || secretType == "" {
		return nil, errors.Errorf("secret '%s' must contain both 'type' (non-empty string) and 'details' (object)", name)
	}

	details["name"] = name

	connectionsMap := map[string][]map[string]any{
		secretType: {
			details,
		},
	}

	serialized, err := json.Marshal(connectionsMap)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to process secret '%s'", name)
	}

	var connections config.Connections

	if err := json.Unmarshal(serialized, &connections); err != nil {
		return nil, errors.Wrapf(err, "failed to parse secret '%s' configuration", name)
	}

	environment := config.Environment{
		Connections: &connections,
	}

	cfg := config.Config{
		Environments: map[string]config.Environment{
			"default": environment,
		},
		SelectedEnvironmentName: "default",
		SelectedEnvironment:     &environment,
		DefaultEnvironmentName:  "default",
	}

	manager, errs := connection.NewManagerFromConfig(&cfg)
	if len(errs) > 0 {
		return nil, errors.Wrapf(errs[0], "failed to configure connection '%s'", name)
	}

	return manager, nil
}
