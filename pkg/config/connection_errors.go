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
	// Cause is why reading the connection from the secrets backend failed, when the connection
	// isn't missing but couldn't be read: the backend was unavailable, or the secret is invalid.
	Cause error
}

// ConnectionLookupErrors is implemented by secrets backends that remember why reading a
// connection failed. Their GetConnection returns nil either way, so a connection that doesn't
// exist and a backend that couldn't be read would otherwise look the same.
type ConnectionLookupErrors interface {
	// LookupError returns why the last read of the named connection failed, or nil if it
	// succeeded or the connection doesn't exist.
	LookupError(name string) error
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
	if lookupErrors, ok := ctx.Value(ConnectionLookupErrorsContextKey).(ConnectionLookupErrors); ok {
		err.Cause = lookupErrors.LookupError(name)
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
	if e.Cause != nil {
		if secretsBackend == "" {
			return fmt.Sprintf("%sconnection '%s' could not be read: %v", prefix, e.Name, e.Cause)
		}
		return fmt.Sprintf("%sconnection '%s' could not be read from secrets backend '%s': %v", prefix, e.Name, secretsBackend, e.Cause)
	}
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

// Unwrap returns why reading the connection failed, if it wasn't simply missing.
func (e *MissingConnectionError) Unwrap() error {
	return e.Cause
}

// SecretsBackendUnavailableError wraps a failure to read from the secrets backend itself, such as
// a timeout or a server error, as opposed to a secret that is missing or invalid.
type SecretsBackendUnavailableError struct {
	Err error
}

func (e *SecretsBackendUnavailableError) Error() string { return e.Err.Error() }

func (e *SecretsBackendUnavailableError) Unwrap() error { return e.Err }
