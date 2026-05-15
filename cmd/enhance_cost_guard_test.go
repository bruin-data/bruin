package cmd

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasCostGuard(t *testing.T) {
	t.Parallel()
	hard := 1000.0
	bytesLimit := int64(1 << 30)

	cases := map[string]struct {
		conn *config.GoogleCloudPlatformConnection
		want bool
	}{
		"nil connection":              {nil, false},
		"no limits set":               {&config.GoogleCloudPlatformConnection{Name: "x"}, false},
		"max_query_cost set":          {&config.GoogleCloudPlatformConnection{Name: "x", MaxQueryCost: &hard}, true},
		"max_query_cost_soft set":     {&config.GoogleCloudPlatformConnection{Name: "x", MaxQueryCostSoft: &hard}, true},
		"max_billable_bytes set":      {&config.GoogleCloudPlatformConnection{Name: "x", MaxBillableBytes: &bytesLimit}, true},
		"max_billable_bytes_soft set": {&config.GoogleCloudPlatformConnection{Name: "x", MaxBillableBytesSoft: &bytesLimit}, true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, hasCostGuard(tc.conn))
		})
	}
}

func TestConnectionsMissingCostGuards(t *testing.T) {
	t.Parallel()
	hard := 5.0
	env := &config.Environment{
		Connections: &config.Connections{
			GoogleCloudPlatform: []config.GoogleCloudPlatformConnection{
				{Name: "guarded", MaxQueryCost: &hard},
				{Name: "unguarded"},
			},
		},
	}

	missing := connectionsMissingCostGuards(env, []string{"guarded", "unguarded", "absent"})
	// "absent" is not declared, so it's intentionally skipped — the regular
	// connection-not-found error path handles that.
	assert.Equal(t, []string{"unguarded"}, missing)
}

func TestApplyCostLimit(t *testing.T) {
	t.Parallel()
	env := &config.Environment{
		Connections: &config.Connections{
			GoogleCloudPlatform: []config.GoogleCloudPlatformConnection{
				{Name: "bq"},
			},
		},
	}

	require.NoError(t, applyCostLimit(env, "bq", 7.5))
	require.NotNil(t, env.Connections.GoogleCloudPlatform[0].MaxQueryCost)
	assert.InDelta(t, 7.5, *env.Connections.GoogleCloudPlatform[0].MaxQueryCost, 0.0001)

	err := applyCostLimit(env, "missing", 5)
	assert.Error(t, err)
}

func TestStdinCostGuardPrompter(t *testing.T) {
	t.Parallel()

	t.Run("blank input applies default and marks skipped", func(t *testing.T) {
		t.Parallel()
		var out bytes.Buffer
		p := &stdinCostGuardPrompter{in: strings.NewReader("\n"), out: &out}
		limit, skipped, err := p.Prompt("bq-default")
		require.NoError(t, err)
		assert.True(t, skipped)
		assert.InDelta(t, defaultBigQueryCostLimit, limit, 0.0001)
		assert.Contains(t, out.String(), "bq-default")
	})

	t.Run("skip keyword applies default", func(t *testing.T) {
		t.Parallel()
		p := &stdinCostGuardPrompter{in: strings.NewReader("skip\n"), out: &bytes.Buffer{}}
		limit, skipped, err := p.Prompt("c")
		require.NoError(t, err)
		assert.True(t, skipped)
		assert.InDelta(t, defaultBigQueryCostLimit, limit, 0.0001)
	})

	t.Run("numeric input is parsed", func(t *testing.T) {
		t.Parallel()
		p := &stdinCostGuardPrompter{in: strings.NewReader("2.5\n"), out: &bytes.Buffer{}}
		limit, skipped, err := p.Prompt("c")
		require.NoError(t, err)
		assert.False(t, skipped)
		assert.InDelta(t, 2.5, limit, 0.0001)
	})

	t.Run("dollar sign is tolerated", func(t *testing.T) {
		t.Parallel()
		p := &stdinCostGuardPrompter{in: strings.NewReader("$3\n"), out: &bytes.Buffer{}}
		limit, _, err := p.Prompt("c")
		require.NoError(t, err)
		assert.InDelta(t, 3.0, limit, 0.0001)
	})

	t.Run("invalid input errors", func(t *testing.T) {
		t.Parallel()
		p := &stdinCostGuardPrompter{in: strings.NewReader("not-a-number\n"), out: &bytes.Buffer{}}
		_, _, err := p.Prompt("c")
		assert.Error(t, err)
	})

	t.Run("negative input errors", func(t *testing.T) {
		t.Parallel()
		p := &stdinCostGuardPrompter{in: strings.NewReader("-1\n"), out: &bytes.Buffer{}}
		_, _, err := p.Prompt("c")
		assert.Error(t, err)
	})
}

// recordingPrompter feeds canned responses for each connection name in order
// and records the connections it was asked about.
type recordingPrompter struct {
	responses []promptResponse
	calls     []string
}

type promptResponse struct {
	limit   float64
	skipped bool
	err     error
}

func (p *recordingPrompter) Prompt(connName string) (float64, bool, error) {
	p.calls = append(p.calls, connName)
	if len(p.responses) == 0 {
		return defaultBigQueryCostLimit, true, nil
	}
	r := p.responses[0]
	p.responses = p.responses[1:]
	return r.limit, r.skipped, r.err
}

// setupBQRepo creates a temp git repo with a .bruin.yml, pipeline.yml, and an asset
// of the given type. It returns the repo root and the asset path.
func setupBQRepo(t *testing.T, assetType, connection, bruinYAML string) (string, string) {
	t.Helper()
	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, ".git"), 0o755))

	require.NoError(t, os.WriteFile(filepath.Join(dir, ".bruin.yml"), []byte(bruinYAML), 0o644))

	pipelineDir := filepath.Join(dir, "my-pipeline")
	require.NoError(t, os.MkdirAll(filepath.Join(pipelineDir, "assets"), 0o755))

	pipelineYAML := "name: my-pipeline\n"
	require.NoError(t, os.WriteFile(filepath.Join(pipelineDir, "pipeline.yml"), []byte(pipelineYAML), 0o644))

	assetContent := "/* @bruin\n" +
		"name: my_asset\n" +
		"type: " + assetType + "\n"
	if connection != "" {
		assetContent += "connection: " + connection + "\n"
	}
	assetContent += "@bruin */\n" +
		"SELECT 1;\n"

	assetPath := filepath.Join(pipelineDir, "assets", "my_asset.sql")
	require.NoError(t, os.WriteFile(assetPath, []byte(assetContent), 0o644))

	return dir, assetPath
}

// gitInit makes the temp directory look like a proper git repo so
// FindRepoFromPath doesn't need to walk above the test root.
func gitInit(t *testing.T, dir string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", "init", "--quiet")
	cmd.Dir = dir
	require.NoError(t, cmd.Run())
}

func TestRunBigQueryCostGuard_SkipsWhenNoBigQueryAssets(t *testing.T) {
	t.Parallel()

	bruinYAML := `default_environment: default
environments:
  default:
    connections:
      snowflake:
        - name: sf-default
          account: x
          username: u
          password: p
`
	dir, assetPath := setupBQRepo(t, "sf.sql", "sf-default", bruinYAML)
	gitInit(t, dir)

	prompter := &recordingPrompter{}
	err := runBigQueryCostGuard(t.Context(), assetPath, "", "plain", afero.NewOsFs(), prompter, false)
	require.NoError(t, err)
	assert.Empty(t, prompter.calls, "no prompt should be triggered for non-BQ assets")
}

func TestRunBigQueryCostGuard_PassThroughWhenLimitSet(t *testing.T) {
	t.Parallel()

	bruinYAML := `default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: bq-default
          project_id: proj
          max_query_cost: 12.5
`
	dir, assetPath := setupBQRepo(t, "bq.sql", "bq-default", bruinYAML)
	gitInit(t, dir)

	prompter := &recordingPrompter{}
	err := runBigQueryCostGuard(t.Context(), assetPath, "", "plain", afero.NewOsFs(), prompter, true)
	require.NoError(t, err)
	assert.Empty(t, prompter.calls, "connection with existing limit should not prompt")

	cm, err := config.LoadOrCreate(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	require.NotNil(t, cm.SelectedEnvironment)
	conn := cm.SelectedEnvironment.Connections.GoogleCloudPlatform[0]
	require.NotNil(t, conn.MaxQueryCost)
	assert.InDelta(t, 12.5, *conn.MaxQueryCost, 0.0001)
}

func TestRunBigQueryCostGuard_PromptsAndPersistsLimit(t *testing.T) {
	t.Parallel()

	bruinYAML := `default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: bq-default
          project_id: proj
`
	dir, assetPath := setupBQRepo(t, "bq.sql", "bq-default", bruinYAML)
	gitInit(t, dir)

	prompter := &recordingPrompter{
		responses: []promptResponse{{limit: 2.5, skipped: false}},
	}

	err := runBigQueryCostGuard(t.Context(), assetPath, "", "json", afero.NewOsFs(), prompter, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"bq-default"}, prompter.calls)

	cm, err := config.LoadOrCreate(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	require.NotNil(t, cm.SelectedEnvironment)
	require.Len(t, cm.SelectedEnvironment.Connections.GoogleCloudPlatform, 1)
	conn := cm.SelectedEnvironment.Connections.GoogleCloudPlatform[0]
	require.NotNil(t, conn.MaxQueryCost, "max_query_cost should be persisted")
	assert.InDelta(t, 2.5, *conn.MaxQueryCost, 0.0001)
}

func TestRunBigQueryCostGuard_AppliesDefaultWhenNonInteractive(t *testing.T) {
	t.Parallel()

	bruinYAML := `default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: bq-default
          project_id: proj
`
	dir, assetPath := setupBQRepo(t, "bq.sql", "bq-default", bruinYAML)
	gitInit(t, dir)

	prompter := &recordingPrompter{}
	err := runBigQueryCostGuard(t.Context(), assetPath, "", "json", afero.NewOsFs(), prompter, false)
	require.NoError(t, err)
	assert.Empty(t, prompter.calls, "non-interactive mode should never prompt")

	cm, err := config.LoadOrCreate(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	conn := cm.SelectedEnvironment.Connections.GoogleCloudPlatform[0]
	require.NotNil(t, conn.MaxQueryCost)
	assert.InDelta(t, defaultBigQueryCostLimit, *conn.MaxQueryCost, 0.0001)
}

func TestRunBigQueryCostGuard_DefaultConnectionResolution(t *testing.T) {
	t.Parallel()

	// Asset has no explicit `connection` field — it should resolve via
	// pipeline.default_connections.google_cloud_platform.
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, ".git"), 0o755))

	bruinYAML := `default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: bq-prod
          project_id: proj
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".bruin.yml"), []byte(bruinYAML), 0o644))

	pipelineDir := filepath.Join(dir, "my-pipeline")
	require.NoError(t, os.MkdirAll(filepath.Join(pipelineDir, "assets"), 0o755))
	pipelineYAML := `name: my-pipeline
default_connections:
  google_cloud_platform: bq-prod
`
	require.NoError(t, os.WriteFile(filepath.Join(pipelineDir, "pipeline.yml"), []byte(pipelineYAML), 0o644))

	assetContent := "/* @bruin\nname: my_asset\ntype: bq.sql\n@bruin */\nSELECT 1;\n"
	assetPath := filepath.Join(pipelineDir, "assets", "my_asset.sql")
	require.NoError(t, os.WriteFile(assetPath, []byte(assetContent), 0o644))
	gitInit(t, dir)

	prompter := &recordingPrompter{
		responses: []promptResponse{{limit: 4, skipped: false}},
	}

	err := runBigQueryCostGuard(context.Background(), assetPath, "", "json", afero.NewOsFs(), prompter, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"bq-prod"}, prompter.calls)

	cm, err := config.LoadOrCreate(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	conn := cm.SelectedEnvironment.Connections.GoogleCloudPlatform[0]
	require.NotNil(t, conn.MaxQueryCost)
	assert.InDelta(t, 4.0, *conn.MaxQueryCost, 0.0001)
}

func TestRunBigQueryCostGuard_FolderScansAllAssets(t *testing.T) {
	t.Parallel()

	bruinYAML := `default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: bq-a
          project_id: proj-a
        - name: bq-b
          project_id: proj-b
          max_query_cost: 10
      snowflake:
        - name: sf-c
          account: x
          username: u
          password: p
`
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, ".git"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".bruin.yml"), []byte(bruinYAML), 0o644))

	pipelineDir := filepath.Join(dir, "pipe")
	require.NoError(t, os.MkdirAll(filepath.Join(pipelineDir, "assets"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pipelineDir, "pipeline.yml"), []byte("name: pipe\n"), 0o644))

	writeAsset := func(name, assetType, conn string) {
		c := "/* @bruin\nname: " + name + "\ntype: " + assetType + "\nconnection: " + conn + "\n@bruin */\nSELECT 1;\n"
		require.NoError(t, os.WriteFile(filepath.Join(pipelineDir, "assets", name+".sql"), []byte(c), 0o644))
	}
	writeAsset("a", "bq.sql", "bq-a")
	writeAsset("b", "bq.sql", "bq-b")
	writeAsset("c", "sf.sql", "sf-c")

	gitInit(t, dir)

	prompter := &recordingPrompter{
		responses: []promptResponse{{limit: 1.0, skipped: false}},
	}

	err := runBigQueryCostGuard(t.Context(), pipelineDir, "", "json", afero.NewOsFs(), prompter, true)
	require.NoError(t, err)
	// Only bq-a is missing a cost guard; bq-b already has one and sf-c is non-BQ.
	assert.Equal(t, []string{"bq-a"}, prompter.calls)
}

func TestCollectBigQueryConnections_AssetWithoutPipeline(t *testing.T) {
	t.Parallel()
	// A path that doesn't lead to a real pipeline should return no connections,
	// not an error — the regular asset-loading flow will report the issue.
	out, err := collectBigQueryConnections(t.Context(), filepath.Join(t.TempDir(), "nope.sql"))
	require.NoError(t, err)
	assert.Nil(t, out)
}
