package config

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrConnectionNotFound matches, with errors.Is, a secrets backend's error for a connection
// that doesn't exist, as opposed to one that couldn't be read.
var ErrConnectionNotFound = errors.New("connection not found")

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
	// LookupError returns why reading the named connection failed during this run, or nil if it
	// never failed other than by the connection not existing.
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
		// The cause is not shown: it can carry the backend's address, or the connection's own
		// values when configuring it failed, and those aren't masked in the output.
		from := ""
		if secretsBackend != "" {
			from = fmt.Sprintf(" from secrets backend '%s'", secretsBackend)
		}
		var unavailable *SecretsBackendUnavailableError
		if errors.As(e.Cause, &unavailable) {
			return fmt.Sprintf("%sconnection '%s' could not be read%s: the secrets backend is unavailable", prefix, e.Name, from)
		}
		return fmt.Sprintf("%sconnection '%s' read%s is not a valid connection", prefix, e.Name, from)
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
