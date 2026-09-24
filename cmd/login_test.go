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

//nolint:paralleltest
func TestCloudLoginCommand(t *testing.T) {
	for _, tc := range []struct {
		name      string
		args      []string
		wantError string
	}{
		{name: "help", args: []string{"--help"}},
		{name: "conflicting scope", args: []string{"--repo", "--global"}, wantError: "--repo and --global cannot be used together"},
		{name: "removed oauth subcommand", args: []string{"oauth"}, wantError: "unexpected argument; use 'bruin cloud login --help'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			isDebug := false
			command := &cli.Command{Name: "bruin", Writer: &output, Commands: []*cli.Command{Cloud(&isDebug)}}
			err := command.Run(t.Context(), append([]string{"bruin", "cloud", "login"}, tc.args...))
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			assert.Contains(t, output.String(), "bruin cloud login")
			for _, flag := range []string{"--repo", "--global", "--no-browser", "--reauth", "--connection", "--environment"} {
				assert.Contains(t, output.String(), flag)
			}
			assert.NotContains(t, output.String(), "oauth")
			assert.NotContains(t, output.String(), "--team")
		})
	}
}

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
	require.NoError(t, command.Run(t.Context(), []string{"login", "--repo"}))
	cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	selected, err := cm.ResolveCloudConnection()
	require.NoError(t, err)
	assert.Equal(t, "original-token", selected.Connection.APIToken)
	data, err := os.ReadFile(filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	assert.Equal(t, loginExistingConfig, string(data))
	assert.NotContains(t, output.String(), "original-token")
}

func TestLoginCreatesConnection(t *testing.T) { //nolint:paralleltest
	dir := loginTestRepo(t, "")
	var output bytes.Buffer
	deps := loginDependencies{interactive: func() bool { return false }, authorize: func(_ context.Context, target string, noBrowser bool, _ io.Writer) (*cloudauth.Credential, error) {
		assert.Equal(t, "repo", target)
		assert.True(t, noBrowser)
		assert.NoFileExists(t, filepath.Join(dir, ".bruin.yml"))
		return &cloudauth.Credential{Token: "new-token", TokenID: "123", APIURL: cloudauth.DefaultAPIURL, DefaultTeam: "acme"}, nil
	}}
	require.ErrorContains(t, loginCommand(deps).Run(t.Context(), []string{"login", "--repo"}), "--repo requires a Bruin project")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pipeline.yml"), []byte("name: test\n"), 0o600))
	command := loginCommand(deps)
	command.Writer = &output
	require.NoError(t, command.Run(t.Context(), []string{"login", "--repo", "--no-browser"}))
	cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	require.Len(t, cm.CloudConnections(), 1)
	assert.Equal(t, "new-token", cm.CloudConnections()[0].Connection.APIToken)
	assert.NotContains(t, output.String(), "new-token")
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", dir, "check-ignore", "-q", ".bruin.yml").Run())
}

//nolint:paralleltest
func TestLoginProjectDetection(t *testing.T) {
	for _, tc := range []struct {
		name     string
		config   string
		pipeline string
		noGit    bool
		found    bool
	}{
		{name: "config only", config: loginExistingConfig, found: true},
		{name: "root pipeline yml", pipeline: "pipeline.yml", found: true},
		{name: "nested pipeline yaml", pipeline: "pipelines/example/pipeline.yaml", found: true},
		{name: "plain git repository"},
		{name: "ignored dependency pipeline", pipeline: "node_modules/example/pipeline.yml"},
		{name: "outside git repository", noGit: true, pipeline: "pipeline.yml"},
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
			path, err := repoLoginConfigPath()
			require.NoError(t, err)
			if tc.found {
				assert.Equal(t, filepath.Join(dir, ".bruin.yml"), path)
			} else {
				assert.Empty(t, path)
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
	err := command.Run(t.Context(), []string{"login", "--repo", "--reauth"})
	require.ErrorContains(t, err, "denied")
	data, err := os.ReadFile(filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	assert.Equal(t, loginExistingConfig, string(data))
}

func TestLoginTrackedConfigAndExistingNonInteractive(t *testing.T) { //nolint:paralleltest
	dir := loginTestRepo(t, loginExistingConfig)
	deps := loginDependencies{interactive: func() bool { return false }}
	err := loginCommand(deps).Run(t.Context(), []string{"login", "--repo"})
	require.ErrorContains(t, err, "interactive")
	require.NoError(t, exec.CommandContext(t.Context(), "git", "-C", dir, "add", "-f", ".bruin.yml").Run())
	err = loginCommand(deps).Run(t.Context(), []string{"login", "--repo", "--reauth"})
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
	globalErr = errors.New("global configuration unavailable")
	_, err = resolve()
	require.ErrorIs(t, err, globalErr)
}

func TestLogoutRemovesRepositoryConnection(t *testing.T) { //nolint:paralleltest
	dir := loginTestRepo(t, loginExistingConfig)
	var output bytes.Buffer
	command := Logout()
	command.Writer = &output
	require.NoError(t, command.Run(t.Context(), []string{"logout", "--repo"}))
	cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	assert.Empty(t, cm.CloudConnections())
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

func TestGlobalLoginOutsideBruinProject(t *testing.T) { //nolint:paralleltest
	dir := loginTestRepo(t, "")
	secrets := keyring.NewArrayKeyring(nil)
	store := cloudauth.Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: func() (cloudauth.SecretStore, error) { return secrets, nil }}
	deps := loginDependencies{interactive: func() bool { return true }, globalStore: func() (cloudauth.Store, error) { return store, nil }, authorize: func(_ context.Context, target string, _ bool, _ io.Writer) (*cloudauth.Credential, error) {
		assert.Equal(t, "global", target)
		return &cloudauth.Credential{Token: "global-private-token", TokenID: "1", APIURL: cloudauth.DefaultAPIURL}, nil
	}}
	var output bytes.Buffer
	command := loginCommand(deps)
	command.Reader = strings.NewReader("")
	command.Writer = &output
	require.NoError(t, command.Run(t.Context(), []string{"login"}))
	assert.NoFileExists(t, filepath.Join(dir, ".bruin.yml"))
	assert.NotContains(t, output.String(), "global-private-token")
	credential, err := store.Read(cloudauth.DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "global-private-token", credential.Token)
}
