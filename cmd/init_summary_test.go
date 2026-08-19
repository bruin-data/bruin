//nolint:paralleltest
package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/fatih/color"
	"github.com/stretchr/testify/require"
)

func captureInitSummary(t *testing.T, cfg initConfigSummary, pipelinePath string) string {
	t.Helper()

	buf := new(bytes.Buffer)
	originalOutput := color.Output
	color.Output = buf
	t.Cleanup(func() { color.Output = originalOutput })

	printInitSummary(cfg, pipelinePath)

	return buf.String()
}

func writeBruinYml(t *testing.T, dir string) string {
	t.Helper()

	path := filepath.Join(dir, ".bruin.yml")
	require.NoError(t, os.WriteFile(path, []byte("environments: {}\n"), 0o600))

	return path
}

func TestPrintInitSummaryReportsCreatedConfigAtRepoRoot(t *testing.T) {
	repoRoot := t.TempDir()
	bruinYmlPath := writeBruinYml(t, repoRoot)
	pipelinePath := filepath.Join(repoRoot, "nested", "my-pipeline")

	output := captureInitSummary(t, initConfigSummary{
		path:         bruinYmlPath,
		merged:       true,
		locationNote: "This is your Git repo root, so it may sit several levels above the pipeline folder.",
	}, pipelinePath)

	require.Contains(t, output, "Config:   "+bruinYmlPath)
	require.Contains(t, output, "Pipeline: "+pipelinePath)
	require.Contains(t, output, "Created .bruin.yml at "+bruinYmlPath)
	require.Contains(t, output, "Git repo root")
	require.Contains(t, output, "Add your connection credentials to "+bruinYmlPath)
	require.Contains(t, output, "bruin validate "+pipelinePath)
	require.Contains(t, output, "bruin run "+pipelinePath)
}

func TestPrintInitSummaryReportsMergedIntoExistingConfig(t *testing.T) {
	projectRoot := t.TempDir()
	bruinYmlPath := writeBruinYml(t, projectRoot)

	output := captureInitSummary(t, initConfigSummary{
		path:         bruinYmlPath,
		existed:      true,
		merged:       true,
		locationNote: "This is your Bruin project root.",
	}, filepath.Join(projectRoot, "my-pipeline"))

	require.Contains(t, output, "Using existing .bruin.yml at "+bruinYmlPath+" (merged template config).")
	require.NotContains(t, output, "Created .bruin.yml")
	require.Contains(t, output, "This is your Bruin project root.")
}

// Templates such as migration-fivetran ship no .bruin.yml, so init must not claim
// it merged anything into the config it found.
func TestPrintInitSummaryReportsUnchangedExistingConfig(t *testing.T) {
	projectRoot := t.TempDir()
	bruinYmlPath := writeBruinYml(t, projectRoot)

	output := captureInitSummary(t, initConfigSummary{
		path:    bruinYmlPath,
		existed: true,
	}, filepath.Join(projectRoot, "my-pipeline"))

	require.Contains(t, output, "Using existing .bruin.yml at "+bruinYmlPath+" (left unchanged).")
	require.NotContains(t, output, "merged template config")
	require.NotContains(t, output, "Created .bruin.yml")
}

func TestPrintInitSummaryWithoutConfigFile(t *testing.T) {
	projectRoot := t.TempDir()

	output := captureInitSummary(t, initConfigSummary{
		path:         filepath.Join(projectRoot, ".bruin.yml"),
		locationNote: "This is your Bruin project root.",
	}, filepath.Join(projectRoot, "my-pipeline"))

	require.NotContains(t, output, "Config:")
	require.NotContains(t, output, "This is your Bruin project root.")
	require.Contains(t, output, "Create a .bruin.yml with your connection credentials")
	require.Contains(t, output, "bruin validate ")
}

func TestFileExists(t *testing.T) {
	dir := t.TempDir()
	filePath := writeBruinYml(t, dir)

	require.True(t, fileExists(filePath))
	require.False(t, fileExists(filepath.Join(dir, "missing.yml")))
	require.False(t, fileExists(dir))
}
