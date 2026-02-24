package config

import (
	"context"
	"strings"

	errors2 "github.com/pkg/errors"
)

type ConnectionLookupDetails struct {
	SecretsBackend  string
	ConfigFilePath  string
	EnvironmentName string
}

func ConnectionLookupDetailsFromContext(ctx context.Context) ConnectionLookupDetails {
	details := ConnectionLookupDetails{}
	if ctx == nil {
		return details
	}

	if backend, ok := ctx.Value(SecretsBackendContextKey).(string); ok {
		details.SecretsBackend = backend
	}
	if configPath, ok := ctx.Value(ConfigFilePathContextKey).(string); ok {
		details.ConfigFilePath = configPath
	}
	if envName, ok := ctx.Value(EnvironmentNameContextKey).(string); ok {
		details.EnvironmentName = envName
	}

	return details
}

func ConnectionNotFoundError(details ConnectionLookupDetails, role, name string) error {
	role = strings.TrimSpace(role)
	prefix := ""
	if role != "" {
		prefix = role + " "
	}

	secretsBackend := strings.TrimSpace(details.SecretsBackend)
	if secretsBackend != "" {
		return errors2.Errorf("%sconnection '%s' not found in secrets backend '%s'", prefix, name, secretsBackend)
	}

	configFilePath := strings.TrimSpace(details.ConfigFilePath)
	if configFilePath == "" {
		configFilePath = ".bruin.yml"
	}

	environmentName := strings.TrimSpace(details.EnvironmentName)
	if environmentName == "" {
		environmentName = "default"
	}

	return errors2.Errorf(
		"%sconnection '%s' not found in config file '%s' under environment '%s'",
		prefix,
		name,
		configFilePath,
		environmentName,
	)
}

func ConnectionNotFoundErrorFromContext(ctx context.Context, role, name string) error {
	return ConnectionNotFoundError(ConnectionLookupDetailsFromContext(ctx), role, name)
}

func GetRequiredConnection(ctx context.Context, getter ConnectionGetter, role, name string) (any, error) {
	conn := getter.GetConnection(name)
	if conn == nil {
		return nil, ConnectionNotFoundErrorFromContext(ctx, role, name)
	}

	return conn, nil
}
