package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/99designs/keyring"
	"github.com/bruin-data/bruin/pkg/cloudauth"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

//nolint:paralleltest
func TestLogoutDestination(t *testing.T) {
	for _, tc := range []struct {
		name        string
		args        []string
		input       string
		interactive bool
		noGit       bool
		target      string
		wantError   string
	}{
		{name: "choose repository", input: "\r", interactive: true, target: "repo"},
		{name: "choose global", input: "\x1b[B\r", interactive: true, target: "global"},
		{name: "cancel ctrl c", input: "\x03", interactive: true, wantError: "logout cancelled"},
		{name: "cancel escape", input: "\x1b", interactive: true, wantError: "logout cancelled"},
		{name: "outside repository", input: "\r", interactive: true, noGit: true, target: "global"},
		{name: "cancel outside repository", input: "\x03", interactive: true, noGit: true, wantError: "logout cancelled"},
		{name: "explicit repository", args: []string{"--repo"}, target: "repo"},
		{name: "explicit global", args: []string{"--global"}, target: "global"},
		{name: "non-interactive", wantError: "specify --repo or --global in a non-interactive terminal"},
		{name: "conflicting scope", args: []string{"--repo", "--global"}, wantError: "--repo and --global cannot be used together"},
		{name: "unexpected argument", args: []string{"extra"}, wantError: "unexpected argument"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := loginTestRepo(t, loginExistingConfig)
			if tc.noGit {
				require.NoError(t, os.RemoveAll(filepath.Join(dir, ".git")))
			}
			secrets := keyring.NewArrayKeyring(nil)
			store := cloudauth.Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: func() (cloudauth.SecretStore, error) { return secrets, nil }}
			require.NoError(t, store.Save(cloudauth.Credential{Token: "global-token", TokenID: "1", APIURL: cloudauth.DefaultAPIURL}, nil))
			globalBefore, err := os.ReadFile(store.Path)
			require.NoError(t, err)
			deps := logoutDependencies{
				interactive: func() bool { return tc.interactive },
				globalStore: func() (cloudauth.Store, error) {
					require.Equal(t, "global", tc.target)
					return store, nil
				},
			}
			var output bytes.Buffer
			command := logoutCommand(deps)
			command.Reader = strings.NewReader(tc.input)
			command.Writer = &output
			err = command.Run(t.Context(), append([]string{"logout"}, tc.args...))
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				assert.NotContains(t, output.String(), "Token revoked")
				assert.NotContains(t, output.String(), "login removed")
			} else {
				require.NoError(t, err)
			}
			path := filepath.Join(dir, ".bruin.yml")
			if tc.target == "repo" && tc.wantError == "" {
				cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), path)
				require.NoError(t, err)
				assert.Empty(t, cm.CloudConnections())
				assert.Contains(t, output.String(), "Repository login removed")
			} else {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				assert.Equal(t, loginExistingConfig, string(data))
			}
			credential, err := store.Read(cloudauth.DefaultAPIURL)
			require.NoError(t, err)
			keys, err := secrets.Keys()
			require.NoError(t, err)
			if tc.target == "global" && tc.wantError == "" {
				assert.Nil(t, credential)
				assert.Empty(t, keys)
				assert.Contains(t, output.String(), "Global login removed")
			} else {
				require.NotNil(t, credential)
				assert.Equal(t, "global-token", credential.Token)
				assert.Len(t, keys, 1)
				data, err := os.ReadFile(store.Path)
				require.NoError(t, err)
				assert.Equal(t, globalBefore, data)
			}
			assert.NotContains(t, output.String(), "original-token")
			assert.NotContains(t, output.String(), "global-token")
		})
	}
}

func TestLogoutRemovesRepositoryConnection(t *testing.T) { //nolint:paralleltest
	dir := loginTestRepo(t, loginExistingConfig)
	var output bytes.Buffer
	isDebug := false
	command := &cli.Command{Name: "bruin", Writer: &output, Commands: []*cli.Command{Cloud(&isDebug)}}
	require.NoError(t, command.Run(t.Context(), []string{"bruin", "cloud", "logout", "--repo"}))
	cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	assert.Empty(t, cm.CloudConnections())
	assert.NotContains(t, output.String(), "original-token")
}
