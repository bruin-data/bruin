package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestEnsureAIConnectionsDoesNotDuplicateExistingConnections(t *testing.T) {
	t.Parallel()

	targetRoot := t.TempDir()
	configPath := filepath.Join(targetRoot, ".bruin.yml")
	require.NoError(t, os.WriteFile(configPath, []byte(`default_environment: default
environments:
    default:
        connections:
            github:
                - name: existing-github
                  access_token: "${EXISTING_GITHUB_TOKEN}"
                  owner: existing
                  repo: repo
`), 0o644))

	result, err := ensureAIConnections(context.Background(), targetRoot, []aiConnectionType{aiConnectionBruinCloud, aiConnectionGitHub}, aiConnectionOptions{
		connectionResolver: func(connection aiConnectionType) (bool, error) {
			return true, nil
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"bruin"}, result.Added)

	cm := readTestBruinConfig(t, configPath)
	env := cm.Environments["default"]
	require.Len(t, env.Connections.GitHub, 1)
	require.Equal(t, "existing-github", env.Connections.GitHub[0].Name)
	require.Len(t, env.Connections.BruinCloud, 1)
}

func TestParseGitHubRemote(t *testing.T) {
	t.Parallel()

	tests := map[string][2]string{
		"git@github.com:bruin-data/bruin.git":       {"bruin-data", "bruin"},
		"https://github.com/bruin-data/bruin.git":   {"bruin-data", "bruin"},
		"ssh://git@github.com/bruin-data/bruin.git": {"bruin-data", "bruin"},
	}

	for remote, want := range tests {
		owner, repo := parseGitHubRemote(remote)
		require.Equal(t, want[0], owner)
		require.Equal(t, want[1], repo)
	}
}

func readTestBruinConfig(t *testing.T, path string) config.Config {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	var cm config.Config
	require.NoError(t, yaml.Unmarshal(content, &cm))
	return cm
}
