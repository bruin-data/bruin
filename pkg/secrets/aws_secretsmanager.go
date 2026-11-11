package secrets

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/pkg/errors"
)

const (
	defaultAWSRequestTimeout  = 30 * time.Second
	defaultAWSRoleSessionName = "bruin-secrets"
)

type awsSecretsManagerClient interface {
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
	ListSecrets(ctx context.Context, params *secretsmanager.ListSecretsInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.ListSecretsOutput, error)
}

// AWSSecretsManagerClient manages secrets from AWS Secrets Manager.
type AWSSecretsManagerClient struct {
	*cachedSecretBackend

	client awsSecretsManagerClient
	// prefix is prepended to connection names to form secret names, e.g. "bruin/prod/".
	prefix string
	ctx    context.Context
}

// awsAssumeRole holds the optional BRUIN_AWS_ROLE_ARN settings. These are deliberately
// separate from the SDK's AWS_ROLE_ARN, which EKS IRSA uses for the pod's own role.
type awsAssumeRole struct {
	roleARN     string
	externalID  string
	sessionName string
}

// NewAWSSecretsManagerClientFromEnv creates a new AWS Secrets Manager client from environment variables.
//
// Credentials are resolved in this order:
//  1. BRUIN_AWS_ACCESS_KEY_ID / BRUIN_AWS_SECRET_ACCESS_KEY (and BRUIN_AWS_SESSION_TOKEN), if set.
//  2. Otherwise the AWS SDK default chain: AWS_ACCESS_KEY_ID env vars, web identity
//     (EKS IRSA), the shared config/credentials files, container credentials (ECS task
//     role, EKS Pod Identity) and finally the EC2 instance profile via IMDS.
//
// If BRUIN_AWS_ROLE_ARN is set, that role is then assumed via STS using the credentials
// from step 1 or 2.
func NewAWSSecretsManagerClientFromEnv(ctx context.Context, logger logger.Logger) (*AWSSecretsManagerClient, error) {
	cfg, err := loadAWSConfigFromEnv(ctx, logger)
	if err != nil {
		return nil, err
	}

	client := newAWSSecretsManagerClient(ctx, logger, secretsmanager.NewFromConfig(cfg), os.Getenv("BRUIN_AWS_SECRET_PREFIX"))
	if client.prefix != "" {
		logger.Debugf("AWS Secrets Manager: reading connections from secrets prefixed with '%s'", client.prefix)
	}

	return client, nil
}

func loadAWSConfigFromEnv(ctx context.Context, logger logger.Logger) (aws.Config, error) {
	accessKeyID := os.Getenv("BRUIN_AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("BRUIN_AWS_SECRET_ACCESS_KEY")
	if accessKeyID != "" && secretAccessKey == "" {
		return aws.Config{}, errors.New("BRUIN_AWS_SECRET_ACCESS_KEY env variable not set")
	}
	if accessKeyID == "" && secretAccessKey != "" {
		return aws.Config{}, errors.New("BRUIN_AWS_ACCESS_KEY_ID env variable not set")
	}

	assumeRole, err := awsAssumeRoleFromEnv()
	if err != nil {
		return aws.Config{}, err
	}

	// Only the region and, when given, static credentials are set here. Anything else,
	// such as a fixed profile, would stop the SDK from reaching IRSA, Pod Identity or IMDS.
	opts := []func(*awsconfig.LoadOptions) error{}
	if region := os.Getenv("BRUIN_AWS_REGION"); region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	if accessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, os.Getenv("BRUIN_AWS_SESSION_TOKEN")),
		))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, errors.Wrap(err, "failed to load AWS config")
	}
	if cfg.Region == "" {
		return aws.Config{}, errors.New("no AWS region configured for AWS Secrets Manager: set BRUIN_AWS_REGION, AWS_REGION or a region in your AWS config profile")
	}

	if cfg.Credentials != nil {
		cfg.Credentials = &credentialSourceLogger{provider: cfg.Credentials, logger: logger}
	}

	if assumeRole.roleARN != "" {
		logger.Debugf("AWS Secrets Manager: assuming role %s via STS", assumeRole.roleARN)
		cfg.Credentials = aws.NewCredentialsCache(newAWSAssumeRoleProvider(sts.NewFromConfig(cfg), assumeRole))
	}

	return cfg, nil
}

func awsAssumeRoleFromEnv() (awsAssumeRole, error) {
	role := awsAssumeRole{
		roleARN:     strings.TrimSpace(os.Getenv("BRUIN_AWS_ROLE_ARN")),
		externalID:  os.Getenv("BRUIN_AWS_EXTERNAL_ID"),
		sessionName: os.Getenv("BRUIN_AWS_ROLE_SESSION_NAME"),
	}
	if role.roleARN == "" {
		if role.externalID != "" || role.sessionName != "" {
			return awsAssumeRole{}, errors.New("BRUIN_AWS_EXTERNAL_ID and BRUIN_AWS_ROLE_SESSION_NAME require BRUIN_AWS_ROLE_ARN to be set")
		}
		return role, nil
	}

	parsed, err := arn.Parse(role.roleARN)
	if err != nil || parsed.Service != "iam" || !strings.HasPrefix(parsed.Resource, "role/") {
		return awsAssumeRole{}, errors.Errorf("BRUIN_AWS_ROLE_ARN must be an IAM role ARN like arn:aws:iam::123456789012:role/name, got '%s'", role.roleARN)
	}
	if role.sessionName == "" {
		role.sessionName = defaultAWSRoleSessionName
	}

	return role, nil
}

func newAWSAssumeRoleProvider(client stscreds.AssumeRoleAPIClient, role awsAssumeRole) *stscreds.AssumeRoleProvider {
	return stscreds.NewAssumeRoleProvider(client, role.roleARN, func(o *stscreds.AssumeRoleOptions) {
		o.RoleSessionName = role.sessionName
		if role.externalID != "" {
			o.ExternalID = aws.String(role.externalID)
		}
	})
}

// credentialSourceLogger logs, once and at debug level, where the AWS SDK found
// credentials (e.g. "WebIdentityCredentials" for IRSA). It never logs credential values.
type credentialSourceLogger struct {
	provider aws.CredentialsProvider
	logger   logger.Logger
	once     sync.Once
}

func (p *credentialSourceLogger) Retrieve(ctx context.Context) (aws.Credentials, error) {
	creds, err := p.provider.Retrieve(ctx)
	if err == nil {
		p.once.Do(func() {
			p.logger.Debugf("AWS Secrets Manager: using credentials from %s", creds.Source)
		})
	}

	return creds, err
}

// ProviderSources passes through the wrapped provider's sources so SDK user-agent metrics stay accurate.
func (p *credentialSourceLogger) ProviderSources() []aws.CredentialSource {
	if sources, ok := p.provider.(aws.CredentialProviderSource); ok {
		return sources.ProviderSources()
	}

	return nil
}

// IsCredentialsProvider lets aws.IsCredentialsProvider see through this wrapper.
func (p *credentialSourceLogger) IsCredentialsProvider(target aws.CredentialsProvider) bool {
	return aws.IsCredentialsProvider(p.provider, target)
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

	return newAWSSecretsManagerClient(context.Background(), logger, secretsmanager.NewFromConfig(cfg), ""), nil
}

func newAWSSecretsManagerClient(ctx context.Context, logger logger.Logger, client awsSecretsManagerClient, prefix string) *AWSSecretsManagerClient {
	if ctx == nil {
		ctx = context.Background()
	}
	c := &AWSSecretsManagerClient{
		client: client,
		prefix: prefix,
		ctx:    ctx,
	}
	c.cachedSecretBackend = newCachedSecretBackend(c, logger)

	return c
}

func (c *AWSSecretsManagerClient) readConnections(name string) (*config.Connections, error) {
	ctx, cancel := context.WithTimeout(c.ctx, defaultAWSRequestTimeout)
	defer cancel()

	secretName := c.prefix + name
	result, err := c.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return nil, newSecretNotFoundError("secret '%s' not found in AWS Secrets Manager", secretName)
		}
		return nil, errors.Wrapf(err, "failed to read secret '%s' from AWS Secrets Manager", secretName)
	}

	return readConnectionSecret(name, result.SecretString)
}

// listConnectionNames lists the secrets under the configured prefix and returns their names with the prefix removed.
func (c *AWSSecretsManagerClient) listConnectionNames() ([]string, error) {
	input := &secretsmanager.ListSecretsInput{}
	// A leading "!" negates a ListSecrets filter, so such a prefix is matched client-side only.
	if c.prefix != "" && !strings.HasPrefix(c.prefix, "!") {
		input.Filters = []types.Filter{{Key: types.FilterNameStringTypeName, Values: []string{c.prefix}}}
	}

	var names []string
	paginator := secretsmanager.NewListSecretsPaginator(c.client, input)
	for paginator.HasMorePages() {
		ctx, cancel := context.WithTimeout(c.ctx, defaultAWSRequestTimeout)
		page, err := paginator.NextPage(ctx)
		cancel()
		if err != nil {
			return nil, errors.Wrap(err, "failed to list secrets in AWS Secrets Manager")
		}

		for _, secret := range page.SecretList {
			// The server-side filter only narrows the listing; the prefix match that counts is this one.
			secretName := aws.ToString(secret.Name)
			if !strings.HasPrefix(secretName, c.prefix) || secretName == c.prefix {
				continue
			}
			names = append(names, strings.TrimPrefix(secretName, c.prefix))
		}
	}

	return names, nil
}
