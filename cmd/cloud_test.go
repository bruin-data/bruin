package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/bruin-data/bruin/pkg/bruincloud"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/fatih/color"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestResolveAPIKey_FlagPriority(t *testing.T) {
	t.Setenv("BRUIN_CLOUD_API_KEY", "env-key")

	app := &cli.Command{
		Name: "test",
		Flags: []cli.Flag{
			apiKeyFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			key, err := resolveAPIKey(c)
			require.NoError(t, err)
			assert.Equal(t, "flag-key", key)
			return nil
		},
	}

	err := app.Run(t.Context(), []string{"test", "--api-key", "flag-key"})
	require.NoError(t, err)
}

func TestResolveAPIKey_EnvVarFallback(t *testing.T) {
	t.Setenv("BRUIN_CLOUD_API_KEY", "env-key")

	app := &cli.Command{
		Name: "test",
		Flags: []cli.Flag{
			apiKeyFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			key, err := resolveAPIKey(c)
			require.NoError(t, err)
			assert.Equal(t, "env-key", key)
			return nil
		},
	}

	err := app.Run(t.Context(), []string{"test"})
	require.NoError(t, err)
}

func TestResolveAPIKey_NoKeyError(t *testing.T) {
	t.Setenv("BRUIN_CLOUD_API_KEY", "")

	// Run from a temp directory so resolveAPIKey can't find a .bruin.yml in the repo.
	t.Chdir(t.TempDir())

	app := &cli.Command{
		Name: "test",
		Flags: []cli.Flag{
			apiKeyFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			_, err := resolveAPIKey(c)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "API key is required")
			return nil
		},
	}

	err := app.Run(t.Context(), []string{"test"})
	require.NoError(t, err)
}

func TestResolveProjectID_UsesExplicitProject(t *testing.T) {
	t.Parallel()

	called := false
	projectID, err := resolveProjectID("project-123", func() ([]bruincloud.Project, error) {
		called = true
		return nil, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "project-123", projectID)
	assert.False(t, called)
}

func TestResolveProjectID_UsesSingleAccessibleProject(t *testing.T) {
	t.Parallel()

	projectID, err := resolveProjectID("", func() ([]bruincloud.Project, error) {
		return []bruincloud.Project{{ID: "project-123", Name: "only-project"}}, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "project-123", projectID)
}

func TestResolveProjectID_NoProjects(t *testing.T) {
	t.Parallel()

	_, err := resolveProjectID("", func() ([]bruincloud.Project, error) {
		return []bruincloud.Project{}, nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no Bruin Cloud projects found")
}

func TestResolveProjectID_MultipleProjects(t *testing.T) {
	t.Parallel()

	_, err := resolveProjectID("", func() ([]bruincloud.Project, error) {
		return []bruincloud.Project{
			{ID: "project-1", Name: "first"},
			{ID: "project-2", Name: "second"},
		}, nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "--project-id is required")
}

func TestResolveProjectID_ListProjectsError(t *testing.T) {
	t.Parallel()

	_, err := resolveProjectID("", func() ([]bruincloud.Project, error) {
		return nil, errors.New("boom")
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to list projects")
	assert.Contains(t, err.Error(), "boom")
}

const configWithDefaultTeamYML = `default_environment: default
cloud:
    default_team: acme-corp
environments:
    default:
        connections: {}
`

const configWithoutCloudYML = `default_environment: default
environments:
    default:
        connections: {}
`

// writeTempConfigRepo creates a temporary git repo (a bare .git dir is enough
// for repo detection) holding the given .bruin.yml, and returns its path. Pass
// an empty yaml to omit the config file entirely.
func writeTempConfigRepo(t *testing.T, bruinYML string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, ".git"), 0o755))
	if bruinYML != "" {
		require.NoError(t, os.WriteFile(filepath.Join(dir, ".bruin.yml"), []byte(bruinYML), 0o644))
	}
	return dir
}

// resolveTeamInDir runs resolveTeam inside dir with the given extra args (e.g.
// "--team", "x"), so precedence can be exercised against a real config on disk.
func resolveTeamInDir(t *testing.T, dir string, args ...string) string {
	t.Helper()
	t.Setenv("BRUIN_CLOUD_TEAM", "")
	t.Chdir(dir)

	var got string
	app := &cli.Command{
		Name:  "test",
		Flags: []cli.Flag{teamFlag()},
		Action: func(ctx context.Context, c *cli.Command) error {
			got = resolveTeam(c)
			return nil
		},
	}
	require.NoError(t, app.Run(t.Context(), append([]string{"test"}, args...)))
	return got
}

func TestResolveTeam_FlagWins(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	dir := writeTempConfigRepo(t, configWithDefaultTeamYML)
	assert.Equal(t, "flag-team", resolveTeamInDir(t, dir, "--team", "flag-team"))
}

func TestResolveTeam_DefaultFromConfig(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	dir := writeTempConfigRepo(t, configWithDefaultTeamYML)
	assert.Equal(t, "acme-corp", resolveTeamInDir(t, dir))
}

func TestResolveTeam_EmptyFlagFallsToDefault(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	dir := writeTempConfigRepo(t, configWithDefaultTeamYML)
	assert.Equal(t, "acme-corp", resolveTeamInDir(t, dir, "--team", ""))
}

func TestResolveTeam_NoneWhenUnset(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	dir := writeTempConfigRepo(t, configWithoutCloudYML)
	assert.Empty(t, resolveTeamInDir(t, dir))
}

func TestCloudConfigTeam_RoundTrip(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	// No token available, so set-team's best-effort validation stays offline.
	t.Setenv("BRUIN_CLOUD_API_KEY", "")
	dir := writeTempConfigRepo(t, configWithoutCloudYML)
	t.Chdir(dir)
	configPath := filepath.Join(dir, ".bruin.yml")

	setCmd := cloudConfigSetTeam()
	require.NoError(t, setCmd.Run(t.Context(), []string{"set-team", "acme-corp"}))

	cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), configPath)
	require.NoError(t, err)
	assert.Equal(t, "acme-corp", cm.GetDefaultTeam())

	unsetCmd := cloudConfigUnsetTeam()
	require.NoError(t, unsetCmd.Run(t.Context(), []string{"unset-team"}))

	cm, err = config.LoadFromFileOrEnv(afero.NewOsFs(), configPath)
	require.NoError(t, err)
	assert.Empty(t, cm.GetDefaultTeam())
}

func TestCloudConfigSetTeam_DoesNotClobberFileWhenEnvConfigSet(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	t.Setenv("BRUIN_CLOUD_API_KEY", "")
	// A config injected via BRUIN_CONFIG_FILE_CONTENT (as CI does) must not be
	// persisted back over the on-disk .bruin.yml, which would drop any
	// environments/connections absent from the env-provided config.
	t.Setenv("BRUIN_CONFIG_FILE_CONTENT", "default_environment: default\nenvironments:\n  default:\n    connections: {}\n")

	onDisk := `default_environment: default
environments:
    default:
        connections:
            bruin:
                - name: cloud
                  api_token: on-disk-token
`
	dir := writeTempConfigRepo(t, onDisk)
	t.Chdir(dir)
	configPath := filepath.Join(dir, ".bruin.yml")

	setCmd := cloudConfigSetTeam()
	require.NoError(t, setCmd.Run(t.Context(), []string{"set-team", "acme-corp"}))

	// Read the file back directly (ignoring the env var): the on-disk connection
	// must survive and the default team must be written.
	cm, err := config.LoadOrCreateWithoutPathAbsolutization(afero.NewOsFs(), configPath)
	require.NoError(t, err)
	assert.Equal(t, "acme-corp", cm.GetDefaultTeam())
	require.NotNil(t, cm.Environments["default"].Connections)
	require.Len(t, cm.Environments["default"].Connections.BruinCloud, 1)
	assert.Equal(t, "on-disk-token", cm.Environments["default"].Connections.BruinCloud[0].APIToken)
}

// runCloudProjectsListCapturingTeam runs the full "cloud projects list" command
// against a stub server and returns the X-Bruin-Team header it received (and
// whether the header was present at all).
func runCloudProjectsListCapturingTeam(t *testing.T, bruinYML string, args ...string) (string, bool) {
	t.Helper()

	var gotTeam string
	var present bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTeam = r.Header.Get("X-Bruin-Team")
		_, present = r.Header["X-Bruin-Team"]
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(server.Close)

	t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
	t.Setenv("BRUIN_CLOUD_API_KEY", "test-key")
	t.Setenv("BRUIN_CLOUD_TEAM", "")

	dir := writeTempConfigRepo(t, bruinYML)
	t.Chdir(dir)

	isDebug := false
	cmd := Cloud(&isDebug)
	require.NoError(t, cmd.Run(t.Context(), append([]string{"cloud", "projects", "list"}, args...)))
	return gotTeam, present
}

func TestCloudCommand_SendsDefaultTeamHeader(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	gotTeam, present := runCloudProjectsListCapturingTeam(t, configWithDefaultTeamYML)
	assert.True(t, present, "X-Bruin-Team header should be sent when a default team is set")
	assert.Equal(t, "acme-corp", gotTeam)
}

func TestCloudCommand_FlagOverridesDefaultTeamHeader(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	gotTeam, present := runCloudProjectsListCapturingTeam(t, configWithDefaultTeamYML, "--team", "override-team")
	assert.True(t, present)
	assert.Equal(t, "override-team", gotTeam)
}

func TestCloudCommand_NoTeamHeaderWhenUnset(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	_, present := runCloudProjectsListCapturingTeam(t, configWithoutCloudYML)
	assert.False(t, present, "no X-Bruin-Team header should be sent when neither --team nor a default is set")
}

func TestTeamErrorHint(t *testing.T) {
	t.Parallel()

	assert.Contains(t, teamErrorHint(&bruincloud.APIError{Code: "team_required", StatusCode: 409}), "set-team")
	assert.Contains(t, teamErrorHint(&bruincloud.APIError{Code: "team_not_in_scope", StatusCode: 403}), "unset-team")
	assert.Empty(t, teamErrorHint(&bruincloud.APIError{Code: "something_else", StatusCode: 400}))
	assert.Empty(t, teamErrorHint(errors.New("plain error")))
}

func TestCloudCommand_Help(t *testing.T) {
	t.Parallel()
	isDebug := false
	cmd := Cloud(&isDebug)
	require.NotNil(t, cmd)
	assert.Equal(t, "cloud", cmd.Name)
	assert.Len(t, cmd.Commands, 17)

	subNames := make([]string, len(cmd.Commands))
	for i, sub := range cmd.Commands {
		subNames[i] = sub.Name
	}
	assert.Contains(t, subNames, "teams")
	assert.Contains(t, subNames, "cost")
	assert.Contains(t, subNames, "projects")
	assert.Contains(t, subNames, "pipelines")
	assert.Contains(t, subNames, "runs")
	assert.Contains(t, subNames, "backfills")
	assert.Contains(t, subNames, "assets")
	assert.Contains(t, subNames, "instances")
	assert.Contains(t, subNames, "glossary")
	assert.Contains(t, subNames, "agents")
	assert.Contains(t, subNames, "connections")
	assert.Contains(t, subNames, "connection-sets")
	assert.Contains(t, subNames, "dashboards")
	assert.Contains(t, subNames, "scheduled-agents")
	assert.Contains(t, subNames, "skills")
	assert.Contains(t, subNames, "audit-logs")
	assert.Contains(t, subNames, "config")
}

func TestCloudSkillsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudSkills()
	require.NotNil(t, cmd)
	assert.Equal(t, "skills", cmd.Name)
	require.Len(t, cmd.Commands, 5)

	subNames := make([]string, len(cmd.Commands))
	for i, sub := range cmd.Commands {
		subNames[i] = sub.Name
	}
	assert.Contains(t, subNames, "list")
	assert.Contains(t, subNames, "create")
	assert.Contains(t, subNames, "update")
	assert.Contains(t, subNames, "delete")
	assert.Contains(t, subNames, "set-agents")
}

func TestCloudConnectionSetsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudConnectionSets()
	require.NotNil(t, cmd)
	assert.Equal(t, "connection-sets", cmd.Name)
	require.Len(t, cmd.Commands, 5)

	subNames := make([]string, len(cmd.Commands))
	for i, sub := range cmd.Commands {
		subNames[i] = sub.Name
	}
	assert.Contains(t, subNames, "list")
	assert.Contains(t, subNames, "get")
	assert.Contains(t, subNames, "create")
	assert.Contains(t, subNames, "update")
	assert.Contains(t, subNames, "delete")
}

func TestCloudLeafCommandsHaveTeamFlag(t *testing.T) {
	t.Parallel()
	isDebug := false

	checked := 0
	var walk func(*cli.Command)
	walk = func(c *cli.Command) {
		for _, sub := range c.Commands {
			// "cloud config" manages the default team itself and doesn't act on a
			// team, so its commands intentionally omit --team.
			if sub.Name == "config" {
				continue
			}
			// Every runnable command (a leaf, or a parent like "agents
			// connections" that has its own action) must carry --team.
			if sub.Action != nil || len(sub.Commands) == 0 {
				has := false
				for _, f := range sub.Flags {
					for _, n := range f.Names() {
						if n == "team" {
							has = true
						}
					}
				}
				assert.Truef(t, has, "cloud command %q should have --team", sub.Name)
				checked++
			}
			if len(sub.Commands) > 0 {
				walk(sub)
			}
		}
	}
	walk(Cloud(&isDebug))

	assert.Positive(t, checked)
}

func TestCloudProjectsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudProjects()
	require.NotNil(t, cmd)
	assert.Equal(t, "projects", cmd.Name)
	require.Len(t, cmd.Commands, 1)
	assert.Equal(t, "list", cmd.Commands[0].Name)
}

func TestCloudPipelinesCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudPipelines()
	require.NotNil(t, cmd)
	assert.Equal(t, "pipelines", cmd.Name)
	require.Len(t, cmd.Commands, 6)
}

func TestCloudRunsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudRuns()
	require.NotNil(t, cmd)
	assert.Equal(t, "runs", cmd.Name)
	require.Len(t, cmd.Commands, 6)

	subNames := make([]string, len(cmd.Commands))
	for i, sub := range cmd.Commands {
		subNames[i] = sub.Name
	}
	assert.Contains(t, subNames, "diagnose")
}

func TestCloudRunsTriggerCommand_OutputsCreatedRunID(t *testing.T) { //nolint:paralleltest // redirects global output
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/trigger-pipeline-run", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{
			"message": "Run triggered successfully",
			"project": "proj",
			"pipeline": "pipe",
			"run_id": "run-123"
		}`))
	}))
	t.Cleanup(server.Close)
	t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)

	originalStdout := os.Stdout
	originalColorOutput := color.Output
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = writer
	color.Output = writer
	t.Cleanup(func() {
		os.Stdout = originalStdout
		color.Output = originalColorOutput
		_ = reader.Close()
		_ = writer.Close()
	})

	cmd := cloudRunsTrigger()
	err = cmd.Run(t.Context(), []string{
		"trigger",
		"--api-key", "test-key",
		"--project-id", "proj",
		"--pipeline", "pipe",
		"--start-date", "2026-01-01",
		"--end-date", "2026-01-02",
	})
	require.NoError(t, err)
	os.Stdout = originalStdout
	color.Output = originalColorOutput
	require.NoError(t, writer.Close())

	output, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Contains(t, string(output), "Successfully triggered run 'run-123' for pipeline 'pipe' in project 'proj'")
}

func TestCloudAssetsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudAssets()
	require.NotNil(t, cmd)
	assert.Equal(t, "assets", cmd.Name)
	require.Len(t, cmd.Commands, 2)
}

func TestCloudInstancesCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudInstances()
	require.NotNil(t, cmd)
	assert.Equal(t, "instances", cmd.Name)
	require.Len(t, cmd.Commands, 4)
}

func TestCloudGlossaryCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudGlossary()
	require.NotNil(t, cmd)
	assert.Equal(t, "glossary", cmd.Name)
	require.Len(t, cmd.Commands, 2)
}

func TestCloudAgentsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudAgents()
	require.NotNil(t, cmd)
	assert.Equal(t, "agents", cmd.Name)
	require.Len(t, cmd.Commands, 18)

	subNames := make([]string, len(cmd.Commands))
	for i, sub := range cmd.Commands {
		subNames[i] = sub.Name
	}
	assert.Contains(t, subNames, "usage-stats")
	assert.Contains(t, subNames, "delete")
	assert.Contains(t, subNames, "connections")
	assert.Contains(t, subNames, "mcp")
	assert.Contains(t, subNames, "get-memory")
	assert.Contains(t, subNames, "set-memory")
	assert.Contains(t, subNames, "clear-memory")
	assert.Contains(t, subNames, "export-thread")
}

func TestCloudAgentsThreadsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := cloudAgentsThreads()
	require.NotNil(t, cmd)
	assert.Equal(t, "threads", cmd.Name)
	// The bare command still lists (default Action); management verbs are subcommands.
	require.NotNil(t, cmd.Action)
	require.Len(t, cmd.Commands, 4)

	subNames := make([]string, len(cmd.Commands))
	for i, sub := range cmd.Commands {
		subNames[i] = sub.Name
	}
	assert.Contains(t, subNames, "rename")
	assert.Contains(t, subNames, "archive")
	assert.Contains(t, subNames, "unarchive")
	assert.Contains(t, subNames, "delete")
}

func TestCloudDashboardsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudDashboards()
	require.NotNil(t, cmd)
	assert.Equal(t, "dashboards", cmd.Name)
	require.Len(t, cmd.Commands, 8)

	subNames := make([]string, len(cmd.Commands))
	for i, sub := range cmd.Commands {
		subNames[i] = sub.Name
	}
	assert.Contains(t, subNames, "list")
	assert.Contains(t, subNames, "get")
	assert.Contains(t, subNames, "versions")
	assert.Contains(t, subNames, "version")
	assert.Contains(t, subNames, "create")
	assert.Contains(t, subNames, "update")
	assert.Contains(t, subNames, "publish")
	assert.Contains(t, subNames, "delete")
}

// cliRunMu serializes cli.Command.Run: urfave/cli's ensureHelp resets the
// package-level cli.HelpFlag on every run, so concurrent runs race on it.
var cliRunMu sync.Mutex

func runCLI(ctx context.Context, cmd *cli.Command, args []string) error {
	cliRunMu.Lock()
	defer cliRunMu.Unlock()

	return cmd.Run(ctx, args)
}

func TestCloudDashboardsCreate_RejectsNonPositiveAgentID(t *testing.T) {
	t.Parallel()
	// An explicit non-positive --agent-id must fail fast (before any request)
	// rather than being silently dropped into the token-agent fallback. The
	// command signals failure via a non-zero exit code, so capture it through
	// the exit handler instead of Run's return value.
	for _, v := range []string{"0", "-3"} {
		cmd := cloudDashboardsCreate()
		exitCode := 0
		cmd.ExitErrHandler = func(_ context.Context, _ *cli.Command, err error) {
			var ec cli.ExitCoder
			if errors.As(err, &ec) {
				exitCode = ec.ExitCode()
			}
		}
		_ = runCLI(t.Context(), cmd, []string{"create", "--title", "T", "--api-key", "k", "--agent-id", v})
		assert.Equalf(t, 1, exitCode, "agent-id %q should be rejected", v)
	}
}

func TestCloudDashboardsPublish_RejectsNonPositiveDashboardID(t *testing.T) {
	t.Parallel()
	// A non-positive --dashboard-id must fail locally rather than sending a bad
	// request that resolves to a route mismatch.
	for _, v := range []string{"0", "-3"} {
		cmd := cloudDashboardsPublish()
		exitCode := 0
		cmd.ExitErrHandler = func(_ context.Context, _ *cli.Command, err error) {
			var ec cli.ExitCoder
			if errors.As(err, &ec) {
				exitCode = ec.ExitCode()
			}
		}
		_ = runCLI(t.Context(), cmd, []string{"publish", "--api-key", "k", "--dashboard-id", v})
		assert.Equalf(t, 1, exitCode, "dashboard-id %q should be rejected", v)
	}
}

func TestCloudAgentsConnections_RejectsNonPositiveAgentID(t *testing.T) {
	t.Parallel()
	// An explicit non-positive --agent-id must fail locally, not become a bad
	// API request with a generic remote error.
	for _, v := range []string{"0", "-3"} {
		cmd := cloudAgentsConnections()
		exitCode := 0
		cmd.ExitErrHandler = func(_ context.Context, _ *cli.Command, err error) {
			var ec cli.ExitCoder
			if errors.As(err, &ec) {
				exitCode = ec.ExitCode()
			}
		}
		_ = runCLI(t.Context(), cmd, []string{"connections", "--api-key", "k", "--agent-id", v})
		assert.Equalf(t, 1, exitCode, "agent-id %q should be rejected", v)
	}
}

func TestCloudAgentsMcp_RejectsNonPositiveAgentID(t *testing.T) {
	t.Parallel()
	// Each mcp subcommand must reject a non-positive --agent-id locally rather
	// than sending a bad request. Required flags are supplied so parsing reaches
	// the Action guard.
	cases := []struct {
		name string
		cmd  func() *cli.Command
		args []string
	}{
		{"list", cloudAgentsMcpList, []string{"list", "--api-key", "k"}},
		{"set", cloudAgentsMcpSet, []string{"set", "--api-key", "k", "--kind", "linear", "--connection", "c"}},
		{"remove", cloudAgentsMcpRemove, []string{"remove", "--api-key", "k", "--kind", "linear"}},
	}
	for _, tc := range cases {
		for _, v := range []string{"0", "-3"} {
			cmd := tc.cmd()
			exitCode := 0
			cmd.ExitErrHandler = func(_ context.Context, _ *cli.Command, err error) {
				var ec cli.ExitCoder
				if errors.As(err, &ec) {
					exitCode = ec.ExitCode()
				}
			}
			_ = runCLI(t.Context(), cmd, append(tc.args, "--agent-id", v))
			assert.Equalf(t, 1, exitCode, "%s: agent-id %q should be rejected", tc.name, v)
		}
	}
}

func TestCloudAgentsConnectionsAdd_RejectsNonPositiveAgentID(t *testing.T) {
	t.Parallel()
	for _, v := range []string{"0", "-3"} {
		cmd := cloudAgentsConnectionsAdd()
		exitCode := 0
		cmd.ExitErrHandler = func(_ context.Context, _ *cli.Command, err error) {
			var ec cli.ExitCoder
			if errors.As(err, &ec) {
				exitCode = ec.ExitCode()
			}
		}
		_ = runCLI(t.Context(), cmd, []string{"add", "--api-key", "k", "--agent-id", v})
		assert.Equalf(t, 1, exitCode, "agent-id %q should be rejected", v)
	}
}

func TestCloudAgentsConnectionsAdd_RequiresName(t *testing.T) {
	t.Parallel()
	// A positive agent-id but no --name must fail locally before any API call.
	cmd := cloudAgentsConnectionsAdd()
	exitCode := 0
	cmd.ExitErrHandler = func(_ context.Context, _ *cli.Command, err error) {
		var ec cli.ExitCoder
		if errors.As(err, &ec) {
			exitCode = ec.ExitCode()
		}
	}
	_ = runCLI(t.Context(), cmd, []string{"add", "--api-key", "k", "--agent-id", "7"})
	assert.Equal(t, 1, exitCode)
}

func TestCloudScheduledAgentsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudScheduledAgents()
	require.NotNil(t, cmd)
	assert.Equal(t, "scheduled-agents", cmd.Name)
	require.Len(t, cmd.Commands, 7)

	subNames := make([]string, len(cmd.Commands))
	for i, sub := range cmd.Commands {
		subNames[i] = sub.Name
	}
	assert.Contains(t, subNames, "list")
	assert.Contains(t, subNames, "get")
	assert.Contains(t, subNames, "create")
	assert.Contains(t, subNames, "update")
	assert.Contains(t, subNames, "trigger")
	assert.Contains(t, subNames, "delete")
	assert.Contains(t, subNames, "run-states")
}

func TestCloudScheduledAgentsRunStatesCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := cloudScheduledAgentsRunStates()
	require.NotNil(t, cmd)
	assert.Equal(t, "run-states", cmd.Name)
	require.Len(t, cmd.Commands, 4)

	subNames := make([]string, len(cmd.Commands))
	for i, sub := range cmd.Commands {
		subNames[i] = sub.Name
	}
	assert.Contains(t, subNames, "list")
	assert.Contains(t, subNames, "get")
	assert.Contains(t, subNames, "set")
	assert.Contains(t, subNames, "delete")
}

func TestMessagePairIDFromEnv(t *testing.T) {
	t.Run("reads a numeric value", func(t *testing.T) {
		t.Setenv("BRUIN_MESSAGE_PAIR_ID", " 42 ")
		id, ok := messagePairIDFromEnv()
		require.True(t, ok)
		assert.Equal(t, 42, id)
	})

	t.Run("skips when unset", func(t *testing.T) {
		t.Setenv("BRUIN_MESSAGE_PAIR_ID", "")
		_, ok := messagePairIDFromEnv()
		assert.False(t, ok)
	})

	t.Run("skips a non-numeric value", func(t *testing.T) {
		t.Setenv("BRUIN_MESSAGE_PAIR_ID", "abc")
		_, ok := messagePairIDFromEnv()
		assert.False(t, ok)
	})

	t.Run("skips zero and negative values", func(t *testing.T) {
		for _, v := range []string{"0", "-5"} {
			t.Setenv("BRUIN_MESSAGE_PAIR_ID", v)
			_, ok := messagePairIDFromEnv()
			assert.Falsef(t, ok, "value %q should be skipped", v)
		}
	})
}

func TestParseDashboardState(t *testing.T) {
	t.Parallel()

	t.Run("parses a JSON object", func(t *testing.T) {
		t.Parallel()
		got, err := parseJSONOrYAMLObject([]byte(`{"name":"d","rows":[]}`))
		require.NoError(t, err)
		assert.Equal(t, "d", got["name"])
	})

	t.Run("parses a YAML object", func(t *testing.T) {
		t.Parallel()
		got, err := parseJSONOrYAMLObject([]byte("name: d\nrows: []\n"))
		require.NoError(t, err)
		assert.Equal(t, "d", got["name"])
	})

	t.Run("preserves an unquoted date-like scalar as a string", func(t *testing.T) {
		t.Parallel()
		got, err := parseJSONOrYAMLObject([]byte("default: 2024-01-01\n"))
		require.NoError(t, err)
		assert.Equal(t, "2024-01-01", got["default"])
	})

	t.Run("returns nil for non-object documents", func(t *testing.T) {
		t.Parallel()
		for _, in := range []string{"", "null", `"scalar"`, "[1, 2]"} {
			got, err := parseJSONOrYAMLObject([]byte(in))
			require.NoError(t, err, "input %q", in)
			assert.Nil(t, got, "input %q", in)
		}
	})

	t.Run("errors on malformed YAML", func(t *testing.T) {
		t.Parallel()
		_, err := parseJSONOrYAMLObject([]byte("name: [unclosed"))
		require.Error(t, err)
	})
}

func TestExtractErrorLines_ErrorLevel(t *testing.T) {
	t.Parallel()
	logJSON := json.RawMessage(`{
		"logs": {
			"sections": [{
				"rows": [
					{"level": "info", "message": "Starting task"},
					{"level": "ERROR", "message": "Something went wrong"},
					{"level": "info", "message": "Cleaning up"},
					{"level": "CRITICAL", "message": "Fatal failure"}
				]
			}]
		}
	}`)
	lines := extractErrorLines(logJSON)
	assert.Equal(t, []string{"Something went wrong", "Fatal failure"}, lines)
}

func TestExtractErrorLines_MessagePatterns(t *testing.T) {
	t.Parallel()
	logJSON := json.RawMessage(`{
		"logs": {
			"sections": [{
				"rows": [
					{"level": "info", "message": "Running Bruin v0.11.479"},
					{"level": "info", "message": "Query:"},
					{"level": "info", "message": "Result: 1 (expected: 999)"},
					{"level": "info", "message": "Error: custom check failed"},
					{"level": "info", "message": "Check Failed"}
				]
			}]
		}
	}`)
	lines := extractErrorLines(logJSON)
	assert.Equal(t, []string{"Result: 1 (expected: 999)", "Error: custom check failed"}, lines)
}

func TestExtractErrorLines_FallbackWhenNoErrors(t *testing.T) {
	t.Parallel()
	logJSON := json.RawMessage(`{
		"logs": {
			"sections": [{
				"rows": [
					{"level": "info", "message": "line1"},
					{"level": "info", "message": "line2"},
					{"level": "info", "message": "line3"},
					{"level": "info", "message": "line4"},
					{"level": "info", "message": "line5"},
					{"level": "info", "message": "line6"},
					{"level": "info", "message": "line7"}
				]
			}]
		}
	}`)
	lines := extractErrorLines(logJSON)
	assert.Equal(t, []string{"line3", "line4", "line5", "line6", "line7"}, lines)
}

func TestExtractErrorLines_StripsANSI(t *testing.T) {
	t.Parallel()
	logJSON := json.RawMessage(`{
		"logs": {
			"sections": [{
				"rows": [
					{"level": "ERROR", "message": "\u001b[31;1mred error\u001b[0m"}
				]
			}]
		}
	}`)
	lines := extractErrorLines(logJSON)
	assert.Equal(t, []string{"red error"}, lines)
}

func TestExtractErrorLines_InvalidJSON(t *testing.T) {
	t.Parallel()
	lines := extractErrorLines(json.RawMessage(`not json`))
	assert.Nil(t, lines)
}

func TestCloudRunsDiagnoseCommand_Flags(t *testing.T) {
	t.Parallel()
	cmd := CloudRuns()
	var diagnoseCmd *cli.Command
	for _, sub := range cmd.Commands {
		if sub.Name == "diagnose" {
			diagnoseCmd = sub
			break
		}
	}
	require.NotNil(t, diagnoseCmd)
	assert.Equal(t, "diagnose", diagnoseCmd.Name)

	flagNames := make([]string, len(diagnoseCmd.Flags))
	for i, f := range diagnoseCmd.Flags {
		flagNames[i] = f.Names()[0]
	}
	assert.Contains(t, flagNames, "api-key")
	assert.Contains(t, flagNames, "output")
	assert.Contains(t, flagNames, "project-id")
	assert.Contains(t, flagNames, "pipeline")
	assert.Contains(t, flagNames, "run-id")
	assert.Contains(t, flagNames, "latest")
}

func TestValidateSplitFlags(t *testing.T) {
	t.Parallel()

	t.Run("no split, no chunk-size is valid", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, validateSplitFlags("", false, 1))
	})

	t.Run("chunk-size without split errors", func(t *testing.T) {
		t.Parallel()
		err := validateSplitFlags("", true, 7)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--chunk-size requires --split")
	})

	t.Run("valid split unit", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, validateSplitFlags("month", true, 2))
	})

	t.Run("invalid split unit errors", func(t *testing.T) {
		t.Parallel()
		err := validateSplitFlags("fortnight", false, 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid --split")
	})

	t.Run("chunk-size below one errors", func(t *testing.T) {
		t.Parallel()
		err := validateSplitFlags("day", true, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least 1")
	})
}

func TestParseRunVariables(t *testing.T) {
	t.Parallel()

	t.Run("empty input returns nil", func(t *testing.T) {
		t.Parallel()
		got, err := parseRunVariables(nil)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("quoted string value", func(t *testing.T) {
		t.Parallel()
		got, err := parseRunVariables([]string{`env="prod"`})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"env": "prod"}, got)
	})

	t.Run("boolean value", func(t *testing.T) {
		t.Parallel()
		got, err := parseRunVariables([]string{"debug=true"})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"debug": true}, got)
	})

	t.Run("json object form sets multiple keys", func(t *testing.T) {
		t.Parallel()
		got, err := parseRunVariables([]string{`{"region":"eu","retries":3}`})
		require.NoError(t, err)
		assert.Equal(t, "eu", got["region"])
		assert.EqualValues(t, 3, got["retries"])
	})

	t.Run("multiple flags are merged", func(t *testing.T) {
		t.Parallel()
		got, err := parseRunVariables([]string{`a="x"`, "b=true"})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"a": "x", "b": true}, got)
	})

	t.Run("unquoted bareword value errors", func(t *testing.T) {
		t.Parallel()
		_, err := parseRunVariables([]string{"env=prod"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid variable override")
	})
}

func TestParseCostFilters(t *testing.T) {
	t.Parallel()

	t.Run("eq and repeated in merge by field", func(t *testing.T) {
		t.Parallel()
		// urfave splits on commas, so `in` values arrive as repeated --filter flags.
		filters, err := parseCostFilters([]string{"user_email:eq:a@b.com", "pipeline_id:in:x", "pipeline_id:in:y"})
		require.NoError(t, err)
		require.Len(t, filters, 2)
		assert.Equal(t, bruincloud.CostFilter{Field: "user_email", Op: "eq", Value: "a@b.com"}, filters[0])
		assert.Equal(t, bruincloud.CostFilter{Field: "pipeline_id", Op: "in", Value: []string{"x", "y"}}, filters[1])
	})

	t.Run("bare token is rejected", func(t *testing.T) {
		t.Parallel()
		_, err := parseCostFilters([]string{"pipeline_id:in:a", "b"})
		require.Error(t, err)
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		filters, err := parseCostFilters(nil)
		require.NoError(t, err)
		assert.Empty(t, filters)
	})

	t.Run("malformed", func(t *testing.T) {
		t.Parallel()
		_, err := parseCostFilters([]string{"pipeline_id=x"})
		require.Error(t, err)
	})
}

func TestFormatCostCell(t *testing.T) {
	t.Parallel()
	assert.Empty(t, formatCostCell(nil))
	assert.Equal(t, "daily-etl", formatCostCell("daily-etl"))
	assert.Equal(t, "74123", formatCostCell(float64(74123)))
	assert.JSONEq(t, `{"pipeline_id":"p"}`, formatCostCell(map[string]any{"pipeline_id": "p"}))
}

func TestUpsertMcpServer(t *testing.T) {
	t.Parallel()

	t.Run("appends a new kind", func(t *testing.T) {
		t.Parallel()
		out := upsertMcpServer([]bruincloud.AgentMcpServer{{Kind: "linear", ConnectionName: "a"}}, "github", "b")
		require.Len(t, out, 2)
		assert.Equal(t, "linear", out[0].Kind)
		assert.Equal(t, "github", out[1].Kind)
		assert.Equal(t, "b", out[1].ConnectionName)
	})

	t.Run("updates an existing kind in place", func(t *testing.T) {
		t.Parallel()
		out := upsertMcpServer([]bruincloud.AgentMcpServer{
			{Kind: "linear", ConnectionName: "old"},
			{Kind: "github", ConnectionName: "gh"},
		}, "linear", "new")
		require.Len(t, out, 2)
		assert.Equal(t, "new", out[0].ConnectionName)
		assert.Equal(t, "gh", out[1].ConnectionName)
	})
}

func TestRemoveMcpServer(t *testing.T) {
	t.Parallel()

	t.Run("drops the kind and reports it", func(t *testing.T) {
		t.Parallel()
		out, removed := removeMcpServer([]bruincloud.AgentMcpServer{
			{Kind: "linear", ConnectionName: "a"},
			{Kind: "github", ConnectionName: "b"},
		}, "linear")
		assert.True(t, removed)
		require.Len(t, out, 1)
		assert.Equal(t, "github", out[0].Kind)
	})

	t.Run("reports when the kind is absent", func(t *testing.T) {
		t.Parallel()
		out, removed := removeMcpServer([]bruincloud.AgentMcpServer{{Kind: "linear", ConnectionName: "a"}}, "github")
		assert.False(t, removed)
		require.Len(t, out, 1)
	})
}
