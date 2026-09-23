package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
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

func loginTestRepo(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, exec.CommandContext(t.Context(), "git", "init", "-q", dir).Run())
	t.Chdir(dir)
	t.Setenv("BRUIN_CONFIG_FILE_CONTENT", "")
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("BRUIN_CLOUD_API_KEY", "")
	t.Setenv("BRUIN_CLOUD_BASE_URL", "")
	if content != "" {
		require.NoError(t, os.WriteFile(filepath.Join(dir, ".bruin.yml"), []byte(content), 0o600))
	}
	return dir
}

const loginExistingConfig = "default_environment: default\nenvironments:\n  default:\n    connections:\n      bruin:\n        - name: cloud\n          api_token: original-token\n"

func TestLoginMenu(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		keys      string
		selection int
		cancelled bool
	}{
		{name: "enter keeps default", keys: "\r"},
		{name: "down selects second", keys: "\x1b[B\r", selection: 1},
		{name: "up returns to first", keys: "\x1b[B\x1b[A\r"},
		{name: "select cancel", keys: "\x1b[B\x1b[B\r", selection: 2},
		{name: "interrupt", keys: "\x03", cancelled: true},
		{name: "escape", keys: "\x1b", cancelled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var output bytes.Buffer
			prompt := loginPrompt{reader: strings.NewReader(tc.keys), writer: &output, interactive: true}
			selected, err := prompt.choose(t.Context(), "Existing login", []string{"Use existing login", "Sign in again", "Cancel"})
			if tc.cancelled {
				require.ErrorContains(t, err, "cancelled")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.selection, selected)
			assert.Contains(t, output.String(), []string{"Use existing login", "Sign in again", "Cancel"}[tc.selection])
		})
	}
}

func TestLoginExistingConnectionDoesNotAuthorize(t *testing.T) { //nolint:paralleltest
	dir := loginTestRepo(t, loginExistingConfig)
	var output bytes.Buffer
	deps := loginDependencies{interactive: func() bool { return true }, authorize: func(context.Context, string, bool, io.Writer) (*cloudauth.Credential, error) {
		t.Fatal("must not authorize an existing connection automatically")
		return nil, nil
	}}
	command := loginCommand(deps)
	command.Reader = strings.NewReader("\r")
	command.Writer = &output
	require.NoError(t, command.Run(t.Context(), []string{"login", "oauth", "--repo"}))
	cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	selected, err := cm.ResolveCloudConnection()
	require.NoError(t, err)
	assert.Equal(t, "original-token", selected.Connection.APIToken)
	assert.Equal(t, "cloud", cm.Cloud.Connection)
	assert.NotContains(t, output.String(), "original-token")
}

//nolint:paralleltest
func TestLoginCreatesOrReplacesConnection(t *testing.T) {
	for _, content := range []string{"", loginExistingConfig} {
		t.Run(map[bool]string{true: "replace", false: "create"}[content != ""], func(t *testing.T) {
			dir := loginTestRepo(t, content)
			if content == "" {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "pipeline.yml"), []byte("name: test\n"), 0o600))
			}
			var output bytes.Buffer
			called := false
			deps := loginDependencies{interactive: func() bool { return false }, authorize: func(_ context.Context, target string, noBrowser bool, _ io.Writer) (*cloudauth.Credential, error) {
				called = true
				assert.Equal(t, "repo", target)
				assert.True(t, noBrowser)
				return &cloudauth.Credential{Token: "new-token", TokenID: "123", APIURL: cloudauth.DefaultAPIURL, DefaultTeam: "acme"}, nil
			}}
			command := loginCommand(deps)
			command.Writer = &output
			require.NoError(t, command.Run(t.Context(), []string{"login", "oauth", "--repo", "--reauth", "--no-browser"}))
			assert.True(t, called)
			cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
			require.NoError(t, err)
			assert.Len(t, cm.CloudConnections(), 1)
			assert.Equal(t, "new-token", cm.CloudConnections()[0].Connection.APIToken)
			assert.NotContains(t, output.String(), "new-token")
		})
	}
}

//nolint:paralleltest
func TestLoginProjectDetection(t *testing.T) {
	for _, tc := range []struct {
		name      string
		config    string
		pipeline  string
		subdir    string
		noGit     bool
		flag      string
		target    string
		wantError bool
	}{
		{name: "config only", config: loginExistingConfig, target: "repo"},
		{name: "root pipeline yml", pipeline: "pipeline.yml", target: "repo"},
		{name: "nested pipeline yaml from sibling directory", pipeline: "pipelines/example/pipeline.yaml", subdir: "scripts", target: "repo"},
		{name: "plain git repository", target: "global"},
		{name: "ignored dependency pipeline", pipeline: "node_modules/example/pipeline.yml", target: "global"},
		{name: "outside git repository", noGit: true, pipeline: "pipeline.yml", target: "global"},
		{name: "explicit repo without Bruin project", flag: "--repo", wantError: true},
		{name: "explicit repo outside git", noGit: true, pipeline: "pipeline.yml", flag: "--repo", wantError: true},
		{name: "explicit global in Bruin project", config: loginExistingConfig, flag: "--global", target: "global"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := loginTestRepo(t, tc.config)
			if tc.noGit {
				require.NoError(t, os.RemoveAll(filepath.Join(dir, ".git")))
			}
			if tc.pipeline != "" {
				pipeline := filepath.Join(dir, tc.pipeline)
				require.NoError(t, os.MkdirAll(filepath.Dir(pipeline), 0o700))
				require.NoError(t, os.WriteFile(pipeline, []byte("name: test\n"), 0o600))
			}
			if tc.subdir != "" {
				workingDir := filepath.Join(dir, tc.subdir)
				require.NoError(t, os.MkdirAll(workingDir, 0o700))
				t.Chdir(workingDir)
			}
			secrets := keyring.NewArrayKeyring(nil)
			store := cloudauth.Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: func() (cloudauth.SecretStore, error) { return secrets, nil }}
			called := false
			deps := loginDependencies{
				interactive: func() bool { return true },
				globalStore: func() (cloudauth.Store, error) { return store, nil },
				authorize: func(_ context.Context, target string, _ bool, _ io.Writer) (*cloudauth.Credential, error) {
					called = true
					require.Equal(t, tc.target, target)
					if tc.config == "" {
						assert.NoFileExists(t, filepath.Join(dir, ".bruin.yml"))
					}
					return &cloudauth.Credential{Token: "new-token", TokenID: "123", APIURL: cloudauth.DefaultAPIURL}, nil
				},
			}
			var output bytes.Buffer
			command := loginCommand(deps)
			command.Reader = strings.NewReader("\r")
			command.Writer = &output
			args := []string{"login", "oauth", "--reauth"}
			if tc.flag != "" {
				args = append(args, tc.flag)
			}
			err := command.Run(t.Context(), args)
			if tc.wantError {
				require.ErrorContains(t, err, "--repo requires a Bruin project")
				assert.False(t, called)
				assert.NoFileExists(t, filepath.Join(dir, ".bruin.yml"))
				return
			}
			require.NoError(t, err)
			assert.True(t, called)
			if tc.target == "repo" {
				assert.Contains(t, output.String(), "✓ This repository")
				cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
				require.NoError(t, err)
				require.Len(t, cm.CloudConnections(), 1)
				assert.Equal(t, "new-token", cm.CloudConnections()[0].Connection.APIToken)
				require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", dir, "check-ignore", "-q", ".bruin.yml").Run())
				assert.NoFileExists(t, store.Path)
			} else {
				assert.NotContains(t, output.String(), "Where should Bruin save this login?")
				if tc.flag == "" {
					assert.Contains(t, output.String(), "No Bruin project found; using global login.")
				}
				if tc.config == "" {
					assert.NoFileExists(t, filepath.Join(dir, ".bruin.yml"))
				} else {
					data, err := os.ReadFile(filepath.Join(dir, ".bruin.yml"))
					require.NoError(t, err)
					assert.Equal(t, tc.config, string(data))
				}
				credential, err := store.Read(cloudauth.DefaultAPIURL)
				require.NoError(t, err)
				assert.Equal(t, "new-token", credential.Token)
			}
		})
	}
}

func TestLoginDoesNotOverwriteOnDeniedAuthorization(t *testing.T) { //nolint:paralleltest
	dir := loginTestRepo(t, loginExistingConfig)
	deps := loginDependencies{interactive: func() bool { return false }, authorize: func(context.Context, string, bool, io.Writer) (*cloudauth.Credential, error) {
		return nil, errors.New("denied")
	}}
	command := loginCommand(deps)
	err := command.Run(t.Context(), []string{"login", "oauth", "--repo", "--reauth"})
	require.ErrorContains(t, err, "denied")
	data, err := os.ReadFile(filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	assert.Equal(t, loginExistingConfig, string(data))
}

func TestLoginTrackedConfigAndExistingNonInteractive(t *testing.T) { //nolint:paralleltest
	dir := loginTestRepo(t, loginExistingConfig)
	deps := loginDependencies{interactive: func() bool { return false }}
	err := loginCommand(deps).Run(t.Context(), []string{"login", "oauth", "--repo"})
	require.ErrorContains(t, err, "interactive")
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", dir, "add", "-f", ".bruin.yml").Run())
	err = loginCommand(deps).Run(t.Context(), []string{"login", "oauth", "--repo", "--reauth"})
	require.ErrorContains(t, err, "tracked")
}

func TestCloudAuthPrecedenceAndFailures(t *testing.T) {
	dir := loginTestRepo(t, loginExistingConfig)
	globalCalls := 0
	var globalErr error
	global := func() (*cloudauth.Credential, error) {
		globalCalls++
		return &cloudauth.Credential{Token: "global-token"}, globalErr
	}
	resolve := func(args ...string) (string, error) {
		var value string
		command := &cli.Command{Name: "test", Flags: []cli.Flag{apiKeyFlag()}, Action: func(_ context.Context, c *cli.Command) error {
			auth, err := resolveCloudAuthWithStore(c, global)
			value = auth.token
			return err
		}}
		err := command.Run(t.Context(), append([]string{"test"}, args...))
		return value, err
	}
	t.Setenv("BRUIN_CLOUD_API_KEY", "env-token")
	token, err := resolve("--api-key", "flag-token")
	require.NoError(t, err)
	assert.Equal(t, "flag-token", token)
	token, err = resolve()
	require.NoError(t, err)
	assert.Equal(t, "env-token", token)
	t.Setenv("BRUIN_CLOUD_API_KEY", "")
	token, err = resolve()
	require.NoError(t, err)
	assert.Equal(t, "original-token", token)
	assert.Zero(t, globalCalls)
	path := filepath.Join(dir, ".bruin.yml")
	require.NoError(t, os.WriteFile(path, []byte("environments: [invalid"), 0o600))
	_, err = resolve()
	require.Error(t, err)
	assert.Zero(t, globalCalls)
	require.NoError(t, os.Remove(path))
	token, err = resolve()
	require.NoError(t, err)
	assert.Equal(t, "global-token", token)
	assert.Equal(t, 1, globalCalls)
	globalErr = errors.New("credential store unavailable")
	_, err = resolve()
	require.ErrorIs(t, err, globalErr)
}

func TestLogoutRemovesOnlySelectedConnection(t *testing.T) { //nolint:paralleltest
	dir := loginTestRepo(t, loginExistingConfig)
	cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	ref := cm.CloudConnections()[0]
	require.NoError(t, config.SaveCloudConnection(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"), []byte(loginExistingConfig), ref, false, ""))
	var output bytes.Buffer
	command := Logout()
	command.Writer = &output
	require.NoError(t, command.Run(t.Context(), []string{"logout", "--repo"}))
	cm, err = config.LoadFromFileOrEnv(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	assert.Empty(t, cm.CloudConnections())
	assert.Empty(t, cm.Cloud.Connection)
	assert.NotContains(t, output.String(), "original-token")
}

func TestAuthStatusDoesNotPrintToken(t *testing.T) { //nolint:paralleltest
	loginTestRepo(t, loginExistingConfig)
	var output bytes.Buffer
	command := Auth()
	command.Writer = &output
	require.NoError(t, command.Run(t.Context(), []string{"auth", "status"}))
	assert.Contains(t, output.String(), "repo")
	assert.Contains(t, output.String(), "default/cloud")
	assert.NotContains(t, output.String(), "original-token")
}

func TestGlobalLoginKeepsTokenOutOfConfig(t *testing.T) { //nolint:paralleltest
	loginTestRepo(t, loginExistingConfig)
	secrets := keyring.NewArrayKeyring(nil)
	store := cloudauth.Store{Path: filepath.Join(t.TempDir(), "bruin", "cloud.yml"), Open: func() (cloudauth.SecretStore, error) { return secrets, nil }}
	calls := 0
	deps := loginDependencies{interactive: func() bool { return true }, globalStore: func() (cloudauth.Store, error) { return store, nil }, authorize: func(_ context.Context, target string, _ bool, _ io.Writer) (*cloudauth.Credential, error) {
		calls++
		assert.Equal(t, "global", target)
		return &cloudauth.Credential{Token: "global-private-token", TokenID: "1", APIURL: cloudauth.DefaultAPIURL}, nil
	}}
	var output bytes.Buffer
	command := loginCommand(deps)
	command.Reader = strings.NewReader("\x1b[B\r")
	command.Writer = &output
	require.NoError(t, command.Run(t.Context(), []string{"login", "oauth"}))
	data, err := os.ReadFile(store.Path)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "global-private-token")
	assert.NotContains(t, output.String(), "global-private-token")
	command = loginCommand(deps)
	command.Reader = strings.NewReader("\r")
	command.Writer = &output
	require.NoError(t, command.Run(t.Context(), []string{"login", "oauth", "--global"}))
	assert.Equal(t, 1, calls)
	credential, err := store.Read(cloudauth.DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "global-private-token", credential.Token)
}
