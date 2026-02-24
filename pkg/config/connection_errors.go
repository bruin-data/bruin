package config

import (
	"context"
	"fmt"
	"strings"
)

// MissingConnectionError is returned when a requested connection cannot be found.
type MissingConnectionError struct {
	Role            string
	Name            string
	SecretsBackend  string
	ConfigFilePath  string
	EnvironmentName string
}

func NewConnectionNotFoundError(ctx context.Context, role, name string) *MissingConnectionError {
	err := &MissingConnectionError{
		Role: role,
		Name: name,
	}

	if ctx == nil {
		return err
	}

	if backend, ok := ctx.Value(SecretsBackendContextKey).(string); ok {
		err.SecretsBackend = backend
	}
	if configPath, ok := ctx.Value(ConfigFilePathContextKey).(string); ok {
		err.ConfigFilePath = configPath
	}
	if envName, ok := ctx.Value(EnvironmentNameContextKey).(string); ok {
		err.EnvironmentName = envName
	}

	return err
}

func (e *MissingConnectionError) Error() string {
	role := strings.TrimSpace(e.Role)
	prefix := ""
	if role != "" {
		prefix = role + " "
	}

	secretsBackend := strings.TrimSpace(e.SecretsBackend)
	if secretsBackend != "" {
		return fmt.Sprintf("%sconnection '%s' not found in secrets backend '%s'", prefix, e.Name, secretsBackend)
	}

	configFilePath := strings.TrimSpace(e.ConfigFilePath)
	if configFilePath == "" {
		configFilePath = ".bruin.yml"
	}

	environmentName := strings.TrimSpace(e.EnvironmentName)
	if environmentName == "" {
		environmentName = "default"
	}

	return fmt.Sprintf(
		"%sconnection '%s' not found in config file '%s' under environment '%s'",
		prefix,
		e.Name,
		configFilePath,
		environmentName,
	)
}
