package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/bruin-data/bruin/pkg/secrets"
	"github.com/urfave/cli/v3"
)

const secretsBackendFlagName = "secrets-backend"

func SecretsBackendFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    secretsBackendFlagName,
		Sources: cli.EnvVars("BRUIN_SECRETS_BACKEND"),
		Usage:   "the source of secrets if different from .bruin.yml. Possible values: 'vault', 'doppler', 'aws', 'azure'",
	}
}

func WithSecretsBackendContext(ctx context.Context, c *cli.Command) (context.Context, error) {
	if c == nil {
		return ctx, nil
	}

	backend := strings.TrimSpace(c.String(secretsBackendFlagName))
	if backend == "" {
		return ctx, nil
	}

	return context.WithValue(ctx, config.SecretsBackendContextKey, backend), nil
}

func secretsBackendFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	backend, ok := ctx.Value(config.SecretsBackendContextKey).(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(backend)
}

func connectionManagerFromConfig(ctx context.Context, cm *config.Config, log logger.Logger) (config.ConnectionAndDetailsGetter, []error) {
	secretsBackend := secretsBackendFromContext(ctx)
	switch secretsBackend {
	case "":
		return connection.NewManagerFromConfigWithContext(ctx, cm)
	case "vault":
		manager, err := secrets.NewVaultClientFromEnv(log) //nolint:contextcheck
		if err != nil {
			return nil, []error{fmt.Errorf("failed to initialize vault client: %w", err)}
		}
		return manager, nil
	case "doppler":
		manager, err := secrets.NewDopplerClientFromEnv(log)
		if err != nil {
			return nil, []error{fmt.Errorf("failed to initialize doppler client: %w", err)}
		}
		return manager, nil
	case "aws":
		manager, err := secrets.NewAWSSecretsManagerClientFromEnv(ctx, log)
		if err != nil {
			return nil, []error{fmt.Errorf("failed to initialize AWS Secrets Manager client: %w", err)}
		}
		return manager, nil
	case "azure":
		manager, err := secrets.NewAzureKeyVaultClientFromEnv(log)
		if err != nil {
			return nil, []error{fmt.Errorf("failed to initialize Azure Key Vault client: %w", err)}
		}
		return manager, nil
	default:
		return nil, []error{fmt.Errorf("unsupported secrets backend %q; possible values: vault, doppler, aws, azure", secretsBackend)}
	}
}
