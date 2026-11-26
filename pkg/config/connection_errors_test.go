package config

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectionNotFoundError_ReturnsTypedError(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(t.Context(), ConfigFilePathContextKey, "/tmp/project/.bruin.yml")
	ctx = context.WithValue(ctx, EnvironmentNameContextKey, "prod")

	err := NewConnectionNotFoundError(ctx, "source", "pg-prod")

	var typedErr *MissingConnectionError
	require.ErrorAs(t, err, &typedErr)
	require.Equal(t, "source", typedErr.Role)
	require.Equal(t, "pg-prod", typedErr.Name)
	require.Equal(t, "/tmp/project/.bruin.yml", typedErr.ConfigFilePath)
	require.Equal(t, "prod", typedErr.EnvironmentName)
	require.Equal(
		t,
		"source connection 'pg-prod' not found in config file '/tmp/project/.bruin.yml' under environment 'prod'",
		err.Error(),
	)
}

func TestNewConnectionNotFoundError_UsesSecretsBackendMessage(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(t.Context(), SecretsBackendContextKey, "vault")
	ctx = context.WithValue(ctx, ConfigFilePathContextKey, "/tmp/project/.bruin.yml")
	ctx = context.WithValue(ctx, EnvironmentNameContextKey, "prod")

	err := NewConnectionNotFoundError(ctx, "destination", "bq-prod")

	require.Equal(t, "destination connection 'bq-prod' not found in secrets backend 'vault'", err.Error())
}

func TestMissingConnectionError_Defaults(t *testing.T) {
	t.Parallel()

	err := &MissingConnectionError{Name: "missing"}
	require.Equal(t, "connection 'missing' not found in config file '.bruin.yml' under environment 'default'", err.Error())
}

func TestNewConnectionNotFoundError_ReturnsTypedError(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(t.Context(), ConfigFilePathContextKey, "/repo/.bruin.yml")
	ctx = context.WithValue(ctx, EnvironmentNameContextKey, "dev")
	ctx = context.WithValue(ctx, SecretsBackendContextKey, "aws-secrets-manager")

	err := NewConnectionNotFoundError(ctx, "", "warehouse")

	var typedErr *MissingConnectionError
	require.ErrorAs(t, err, &typedErr)
	require.Equal(t, "connection 'warehouse' not found in secrets backend 'aws-secrets-manager'", err.Error())
}

type lookupErrors map[string]error

func (l lookupErrors) LookupError(name string) error { return l[name] }

func TestNewConnectionNotFoundError_SaysWhenTheSecretsBackendCouldNotBeRead(t *testing.T) {
	t.Parallel()

	cause := errors.New(`failed to read secret 'bq-prod' from Vault: Get "https://vault.internal/v1/kv/data/bq-prod": context deadline exceeded`)
	invalid := errors.New("failed to configure connection 'pg-prod': cannot parse `postgres://user:hunter2@db`")
	ctx := context.WithValue(t.Context(), SecretsBackendContextKey, "vault")
	ctx = context.WithValue(ctx, ConnectionLookupErrorsContextKey, ConnectionLookupErrors(lookupErrors{
		"bq-prod": &SecretsBackendUnavailableError{Err: cause},
		"pg-prod": invalid,
	}))

	err := NewConnectionNotFoundError(ctx, "", "bq-prod")
	require.Equal(t, "connection 'bq-prod' could not be read from secrets backend 'vault': the secrets backend is unavailable", err.Error())
	require.ErrorIs(t, err, cause)

	invalidErr := NewConnectionNotFoundError(ctx, "source", "pg-prod")
	require.Equal(t, "source connection 'pg-prod' read from secrets backend 'vault' is not a valid connection", invalidErr.Error())
	require.NotContains(t, invalidErr.Error(), "hunter2", "the cause isn't shown")
	require.ErrorIs(t, invalidErr, invalid)

	missing := NewConnectionNotFoundError(ctx, "", "other")
	require.Equal(t, "connection 'other' not found in secrets backend 'vault'", missing.Error())
	require.NoError(t, missing.Unwrap())
}
