package inference

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

type inferenceConnections struct {
	config.ConnectionAndDetailsGetter
	warehouse config.ConnectionGetter
}

func (c inferenceConnections) GetConnection(name string) any {
	if name == "warehouse" && c.warehouse != nil {
		return c.warehouse.GetConnection(name)
	}
	return c.ConnectionAndDetailsGetter.GetConnection(name)
}

func testProviderConnections(t *testing.T, warehouse config.ConnectionGetter, live bool) inferenceConnections {
	t.Helper()
	cfg := &config.Config{Environments: map[string]config.Environment{"default": {Connections: &config.Connections{}}}}
	if live {
		path := os.Getenv("BRUIN_INFERENCE_TEST_CONFIG")
		if path == "" {
			path = filepath.Join("..", "..", ".bruin.yml")
		}
		var err error
		cfg, err = config.LoadFromFileOrEnv(afero.NewOsFs(), path)
		require.NoError(t, err)
	} else {
		for _, provider := range []string{"opencode", "openai", "openrouter", "anthropic", "google", "typesafe"} {
			require.NoError(t, cfg.AddConnection("default", provider+"-default", provider, map[string]any{"api_key": provider + "-managed-key"}))
		}
		require.NoError(t, cfg.SelectEnvironment("default"))
	}
	manager, errs := connection.NewManagerFromConfig(cfg)
	require.Empty(t, errs)
	return inferenceConnections{ConnectionAndDetailsGetter: manager, warehouse: warehouse}
}

func liveAPIKey(t *testing.T, provider string) string {
	t.Helper()
	op := NewOperator(testProviderConnections(t, nil, true))
	groups, err := op.resolveGroups(nil, &assetConfig{groups: []requestGroup{{provider: provider}}})
	require.NoError(t, err)
	return groups[0].apiKey
}

func TestManagedInferenceCredentials(t *testing.T) {
	op, ti, warehouse, runner := fixture(t)
	t.Setenv("OPENCODE_API_KEY", "must-not-be-used")
	op.structured = func(_ context.Context, client *Client, _ string, _ string, _ []outputColumn) (map[string]any, error) {
		require.Equal(t, "opencode-managed-key", client.APIKey)
		return map[string]any{"category": "billing"}, nil
	}
	require.NoError(t, op.Run(t.Context(), ti))
	before := runner.calls
	ti.Asset.Parameters["inference_connection"] = "missing"
	require.ErrorContains(t, op.Run(t.Context(), ti), "does not exist")
	ti.Asset.Parameters["inference_connection"] = "typesafe-default"
	require.ErrorContains(t, op.Run(t.Context(), ti), `must have type "opencode"`)
	require.Equal(t, before, runner.calls)
	ti.Asset.Parameters["api_key_env"] = "OPENCODE_API_KEY"
	require.ErrorContains(t, ValidateAsset(ti.Asset), "api_key_env is not supported")
	delete(ti.Asset.Parameters, "api_key_env")
	delete(ti.Asset.Parameters, "inference_connection")

	manager, errs := connection.NewManagerFromConfig(&config.Config{SelectedEnvironment: &config.Environment{Connections: &config.Connections{
		OpenCode: []config.APIKeyConnection{{ConnectionMetadata: config.ConnectionMetadata{Name: "opencode-default"}}},
	}}})
	require.Empty(t, errs)
	op.conn = inferenceConnections{ConnectionAndDetailsGetter: manager, warehouse: warehouse}
	require.ErrorContains(t, op.Run(t.Context(), ti), "requires an API key")
	require.Equal(t, before, runner.calls, "ambient key must not rescue empty managed credentials")
}

type failingCredentialResolver struct {
	inferenceConnections
	err error
}

func (c failingCredentialResolver) ResolveConnection(string) (any, error) { return nil, c.err }

func TestManagedInferencePreservesBackendError(t *testing.T) {
	t.Parallel()
	backendErr := errors.New("secret backend unavailable")
	op := NewOperator(failingCredentialResolver{inferenceConnections: testProviderConnections(t, nil, false), err: backendErr})
	_, err := op.resolveGroups(nil, &assetConfig{groups: []requestGroup{{provider: "openai"}}})
	require.ErrorIs(t, err, backendErr)
}

func TestManagedInferenceGroupingAndDependencies(t *testing.T) {
	t.Parallel()
	asset := columnAsset(t)
	asset.Parameters["inference_connection"] = "opencode-default"
	asset.Columns[3].Inference.Connection = "other-account"
	pipe := &pipeline.Pipeline{DefaultConnections: pipeline.EmptyStringMap{"typesafe": "jev-account"}}
	providerConfig := &config.Config{Environments: map[string]config.Environment{"default": {Connections: &config.Connections{}}}}
	for _, entry := range []struct{ name, provider string }{{"opencode-default", "opencode"}, {"other-account", "opencode"}, {"jev-account", "typesafe"}} {
		require.NoError(t, providerConfig.AddConnection("default", entry.name, entry.provider, map[string]any{"api_key": entry.name + "-key"}))
	}
	require.NoError(t, providerConfig.SelectEnvironment("default"))
	manager, errs := connection.NewManagerFromConfig(providerConfig)
	require.Empty(t, errs)
	op := NewOperator(manager)
	cfg, err := readConfig(asset)
	require.NoError(t, err)
	groups, err := op.resolveGroups(pipe, cfg)
	require.NoError(t, err)
	require.Len(t, groups, 3)
	require.Equal(t, "jev-account-key", groups[0].apiKey)
	require.Equal(t, "opencode-default-key", groups[1].apiKey)
	require.Equal(t, "other-account-key", groups[2].apiKey)
	names, err := pipe.GetAllConnectionNamesForAsset(asset)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"warehouse", "jev-account", "opencode-default", "other-account"}, names)
	// Explicit and implicit references to the same effective connection must group.
	delete(asset.Parameters, "inference_connection")
	asset.Columns[3].Inference.Connection = "opencode-default"
	cfg, err = readConfig(asset)
	require.NoError(t, err)
	groups, err = op.resolveGroups(pipe, cfg)
	require.NoError(t, err)
	require.Len(t, groups, 2)
	require.Len(t, groups[1].columns, 2)
	asset.Columns[3].Inference.Model = "another-model"
	cfg, err = readConfig(asset)
	require.NoError(t, err)
	groups, err = op.resolveGroups(pipe, cfg)
	require.NoError(t, err)
	require.Len(t, groups, 3, "different models must not share a request")
}
