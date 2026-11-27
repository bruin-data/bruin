package cmd

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/secrets"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestWithSecretsBackendContext_GlobalFlagBeforeAndAfterSubcommand(t *testing.T) { //nolint:paralleltest
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "before subcommand",
			args: []string{"bruin", "--secrets-backend", "azure", "query"},
		},
		{
			name: "after subcommand",
			args: []string{"bruin", "query", "--secrets-backend", "azure"},
		},
	}

	for _, tt := range tests { //nolint:paralleltest
		t.Run(tt.name, func(t *testing.T) {
			var got string
			app := &cli.Command{
				Name:   "bruin",
				Flags:  []cli.Flag{SecretsBackendFlag()},
				Before: WithSecretsBackendContext,
				Commands: []*cli.Command{
					{
						Name: "query",
						Action: func(ctx context.Context, c *cli.Command) error {
							got = secretsBackendFromContext(ctx)
							return nil
						},
					},
				},
			}

			err := app.Run(t.Context(), tt.args)
			require.NoError(t, err)
			require.Equal(t, "azure", got)
		})
	}
}

func TestConnectionManagerFromConfigRejectsUnknownSecretsBackend(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(t.Context(), config.SecretsBackendContextKey, "unknown")
	manager, errs := connectionManagerFromConfig(ctx, &config.Config{}, makeLogger(false))

	require.Nil(t, manager)
	require.Len(t, errs, 1)
	require.Contains(t, errs[0].Error(), `unsupported secrets backend "unknown"`)
}

func TestConnectionManagerFromConfigGCP(t *testing.T) {
	// Offline: building the client only parses the credentials; no request is made.
	t.Setenv("BRUIN_GCP_CREDENTIALS_JSON", `{"type": "authorized_user", "client_id": "id", "client_secret": "secret", "refresh_token": "refresh"}`)
	t.Setenv("BRUIN_GCP_PROJECT", "my-project")
	t.Setenv("BRUIN_GCP_SECRET_PREFIX", "")
	ctx := context.WithValue(t.Context(), config.SecretsBackendContextKey, "gcp")

	manager, errs := connectionManagerFromConfig(ctx, &config.Config{}, makeLogger(false))
	require.Empty(t, errs)
	require.IsType(t, &secrets.GCPSecretManagerClient{}, manager)
	require.Implements(t, (*config.ConnectionsConfigGetter)(nil), manager)

	t.Setenv("BRUIN_GCP_SECRET_PREFIX", "bruin/prod/")
	manager, errs = connectionManagerFromConfig(ctx, &config.Config{}, makeLogger(false))
	require.Nil(t, manager)
	require.Len(t, errs, 1)
	require.ErrorContains(t, errs[0], "failed to initialize GCP Secret Manager client: BRUIN_GCP_SECRET_PREFIX 'bruin/prod/' is not valid")
}
