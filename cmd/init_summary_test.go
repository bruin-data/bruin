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

func captureInitSummary(t *testing.T, bruinYmlPath, pipelinePath string, configExisted, configAtRepoRoot bool) string {
	t.Helper()

	buf := new(bytes.Buffer)
	originalOutput := color.Output
	color.Output = buf
	t.Cleanup(func() { color.Output = originalOutput })

	printInitSummary(bruinYmlPath, pipelinePath, configExisted, configAtRepoRoot)

	return buf.String()
}

func TestPrintInitSummaryReportsCreatedConfigAtRepoRoot(t *testing.T) {
	repoRoot := t.TempDir()
	bruinYmlPath := filepath.Join(repoRoot, ".bruin.yml")
	require.NoError(t, os.WriteFile(bruinYmlPath, []byte("environments: {}\n"), 0o600))

	pipelinePath := filepath.Join(repoRoot, "nested", "my-pipeline")
	output := captureInitSummary(t, bruinYmlPath, pipelinePath, false, true)

	require.Contains(t, output, "Config:   "+bruinYmlPath)
	require.Contains(t, output, "Pipeline: "+pipelinePath)
	require.Contains(t, output, "Created .bruin.yml at "+bruinYmlPath)
	require.Contains(t, output, "Git repo root")
	require.Contains(t, output, "Add your connection credentials to "+bruinYmlPath)
	require.Contains(t, output, "bruin validate "+pipelinePath)
	require.Contains(t, output, "bruin run "+pipelinePath)
}

func TestPrintInitSummaryReportsReusedConfig(t *testing.T) {
	projectRoot := t.TempDir()
	bruinYmlPath := filepath.Join(projectRoot, ".bruin.yml")
	require.NoError(t, os.WriteFile(bruinYmlPath, []byte("environments: {}\n"), 0o600))

	output := captureInitSummary(t, bruinYmlPath, filepath.Join(projectRoot, "my-pipeline"), true, false)

	require.Contains(t, output, "Using existing .bruin.yml at "+bruinYmlPath)
	require.Contains(t, output, "merged template config")
	require.NotContains(t, output, "Created .bruin.yml")
	require.Contains(t, output, "Bruin project root")
}

func TestPrintInitSummaryWithoutConfigFile(t *testing.T) {
	projectRoot := t.TempDir()
	bruinYmlPath := filepath.Join(projectRoot, ".bruin.yml")

	output := captureInitSummary(t, bruinYmlPath, filepath.Join(projectRoot, "my-pipeline"), false, true)

	require.NotContains(t, output, "Config:")
	require.Contains(t, output, "Create a .bruin.yml with your connection credentials")
}

func TestFileExists(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "file.yml")
	require.NoError(t, os.WriteFile(filePath, []byte("x"), 0o600))

	require.True(t, fileExists(filePath))
	require.False(t, fileExists(filepath.Join(dir, "missing.yml")))
	require.False(t, fileExists(dir))
}
