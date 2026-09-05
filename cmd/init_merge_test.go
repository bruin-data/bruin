//nolint:paralleltest
package cmd

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

// captureExitCode stops cli.Exit from tearing down the test binary and returns
// the code the command asked for.
func captureExitCode(t *testing.T) *int {
	t.Helper()

	original := cli.OsExiter
	code := 0
	cli.OsExiter = func(c int) { code = c }
	t.Cleanup(func() { cli.OsExiter = original })

	return &code
}

func initGitRepository(t *testing.T, root string) {
	t.Helper()

	gitInit := exec.CommandContext(t.Context(), "git", "init")
	gitInit.Dir = root
	output, err := gitInit.CombinedOutput()
	require.NoError(t, err, string(output))
}

func createExistingPipeline(t *testing.T, root string) string {
	t.Helper()

	pipelinePath := filepath.Join(root, "pipelines", "existing")
	require.NoError(t, os.MkdirAll(filepath.Join(pipelinePath, "assets"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pipelinePath, "pipeline.yml"), []byte("name: existing\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(pipelinePath, "README.md"), []byte("existing readme\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(pipelinePath, "assets", "existing.sql"), []byte("select 42\n"), 0o644))

	return pipelinePath
}

func mergeFilePaths(files []templateMergeFile) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.path)
	}

	return paths
}

func TestInitMergeCopiesTemplateAssetsAndMergesConfig(t *testing.T) {
	targetRoot := t.TempDir()
	t.Chdir(targetRoot)
	initGitRepository(t, targetRoot)
	pipelinePath := createExistingPipeline(t, targetRoot)
	configPath := filepath.Join(targetRoot, ".bruin.yml")
	require.NoError(t, os.WriteFile(configPath, []byte(`default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: existing-duckdb
          path: existing.db
        - name: duckdb-default
          path: keep-existing.db
`), 0o644))

	err := Init().Run(t.Context(), []string{"init", "default", pipelinePath, "--merge"})
	require.NoError(t, err)

	pipelineContent, err := os.ReadFile(filepath.Join(pipelinePath, "pipeline.yml"))
	require.NoError(t, err)
	require.Equal(t, "name: existing\n", string(pipelineContent))

	readmeContent, err := os.ReadFile(filepath.Join(pipelinePath, "README.md"))
	require.NoError(t, err)
	require.Equal(t, "existing readme\n", string(readmeContent))

	existingAsset, err := os.ReadFile(filepath.Join(pipelinePath, "assets", "existing.sql"))
	require.NoError(t, err)
	require.Equal(t, "select 42\n", string(existingAsset))
	require.FileExists(t, filepath.Join(pipelinePath, "assets", "player_stats.sql"))
	require.FileExists(t, filepath.Join(pipelinePath, "assets", "my_python_asset.py"))
	require.FileExists(t, filepath.Join(pipelinePath, "assets", "players.asset.yml"))

	configContent, err := os.ReadFile(configPath)
	require.NoError(t, err)
	require.Contains(t, string(configContent), "name: existing-duckdb")
	require.Contains(t, string(configContent), "name: duckdb-default")
	require.Contains(t, string(configContent), "path: keep-existing.db")
	require.NotContains(t, string(configContent), "path: duckdb.db")
	require.Contains(t, string(configContent), "name: chess-default")
	require.NoDirExists(t, filepath.Join(targetRoot, "bruin"))
}

func TestInitMergeRequiresAPipelinePath(t *testing.T) {
	targetRoot := t.TempDir()
	t.Chdir(targetRoot)

	exitCode := captureExitCode(t)
	err := Init().Run(t.Context(), []string{"init", "default", "--merge"})
	require.Error(t, err)
	require.Equal(t, 1, *exitCode)
	require.NoDirExists(t, filepath.Join(targetRoot, "default"))
}

func TestInitMergeRejectsInPlace(t *testing.T) {
	targetRoot := t.TempDir()
	t.Chdir(targetRoot)
	initGitRepository(t, targetRoot)
	pipelinePath := createExistingPipeline(t, targetRoot)

	exitCode := captureExitCode(t)
	err := Init().Run(t.Context(), []string{"init", "default", pipelinePath, "--merge", "--in-place"})
	require.Error(t, err)
	require.Equal(t, 1, *exitCode)
	require.NoFileExists(t, filepath.Join(pipelinePath, "assets", "player_stats.sql"))
	require.NoFileExists(t, filepath.Join(targetRoot, ".bruin.yml"))
}

func TestMergeTemplateConfigIntoProjectCreatesConfigAtRepoRoot(t *testing.T) {
	targetRoot := t.TempDir()
	initGitRepository(t, targetRoot)
	pipelinePath := createExistingPipeline(t, targetRoot)
	templateConfig, err := loadTemplateBruinConfig("default")
	require.NoError(t, err)

	summary, err := mergeTemplateConfigIntoProject(pipelinePath, templateConfig)
	require.NoError(t, err)
	require.False(t, summary.existed)
	require.True(t, summary.merged)
	require.Equal(t, filepath.Join(targetRoot, ".bruin.yml"), summary.path)

	configContent, err := os.ReadFile(summary.path)
	require.NoError(t, err)
	require.Contains(t, string(configContent), "name: duckdb-default")
}

func TestInitMergeRejectsCollisionsBeforeWriting(t *testing.T) {
	targetRoot := t.TempDir()
	pipelinePath := createExistingPipeline(t, targetRoot)
	collidingAssetPath := filepath.Join(pipelinePath, "assets", "player_stats.sql")
	require.NoError(t, os.WriteFile(collidingAssetPath, []byte("select 'keep me'\n"), 0o644))

	files, err := loadTemplateMergeFiles("default")
	require.NoError(t, err)
	err = collectMergeConflicts(pipelinePath, files)
	require.Error(t, err)
	require.Contains(t, err.Error(), filepath.Join("assets", "player_stats.sql"))

	collidingAsset, readErr := os.ReadFile(collidingAssetPath)
	require.NoError(t, readErr)
	require.Equal(t, "select 'keep me'\n", string(collidingAsset))
	require.NoFileExists(t, filepath.Join(pipelinePath, "assets", "my_python_asset.py"))
	require.NoFileExists(t, filepath.Join(pipelinePath, "assets", "players.asset.yml"))
}

func TestInitMergeReportsParentConflictsPipelineRelative(t *testing.T) {
	targetRoot := t.TempDir()
	pipelinePath := createExistingPipeline(t, targetRoot)
	// A regular file where the template expects a directory.
	require.NoError(t, os.WriteFile(filepath.Join(pipelinePath, "assets", "stripe_raw"), []byte("not a directory\n"), 0o644))

	files, err := loadTemplateMergeFiles("stripe-bigquery")
	require.NoError(t, err)
	err = collectMergeConflicts(pipelinePath, files)
	require.Error(t, err)
	require.Contains(t, err.Error(), filepath.Join("assets", "stripe_raw"))
	require.NotContains(t, err.Error(), targetRoot)
	require.NoFileExists(t, filepath.Join(pipelinePath, "assets", "stripe_stage", "customers.sql"))
}

func TestInitMergeRequiresExistingPipeline(t *testing.T) {
	targetRoot := t.TempDir()
	targetPath := filepath.Join(targetRoot, "not-a-pipeline")
	require.NoError(t, os.MkdirAll(targetPath, 0o755))

	files, err := loadTemplateMergeFiles("default")
	require.NoError(t, err)
	err = collectMergeConflicts(targetPath, files)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no pipeline definition file")
	require.NoDirExists(t, filepath.Join(targetPath, "assets"))
}

func TestMergeTemplateFilesPreservesNestedAssetDirectories(t *testing.T) {
	targetRoot := t.TempDir()
	pipelinePath := createExistingPipeline(t, targetRoot)

	files, err := loadTemplateMergeFiles("stripe-bigquery")
	require.NoError(t, err)
	require.Contains(t, mergeFilePaths(files), "assets/stripe_raw/customer.asset.yml")
	require.NoError(t, collectMergeConflicts(pipelinePath, files))
	require.NoError(t, writeTemplateFiles(pipelinePath, files))

	require.FileExists(t, filepath.Join(pipelinePath, "assets", "stripe_raw", "customer.asset.yml"))
	require.FileExists(t, filepath.Join(pipelinePath, "assets", "stripe_reports", "monthly_subscription_kpis.sql"))
	// Non-mergeable template files stay behind.
	require.NoFileExists(t, filepath.Join(pipelinePath, "dashboards", "stripe-billing-analytics.yml"))
}

func TestLoadTemplateMergeFilesIncludesMacrosAndRequirements(t *testing.T) {
	duckdbFiles, err := loadTemplateMergeFiles("duckdb")
	require.NoError(t, err)
	duckdbPaths := mergeFilePaths(duckdbFiles)
	require.Contains(t, duckdbPaths, "assets/macro_example.sql")
	// macro_example.sql calls count_by, which only exists in macros/.
	require.Contains(t, duckdbPaths, "macros/aggregations.sql")
	require.NotContains(t, duckdbPaths, "pipeline.yml")
	require.NotContains(t, duckdbPaths, ".bruin.yml")

	pythonFiles, err := loadTemplateMergeFiles("python")
	require.NoError(t, err)
	pythonPaths := mergeFilePaths(pythonFiles)
	require.Contains(t, pythonPaths, "assets/python311/asset.py")
	require.Contains(t, pythonPaths, "requirements.txt")
}

func TestMergeTemplateFilesCopiesMacros(t *testing.T) {
	targetRoot := t.TempDir()
	pipelinePath := createExistingPipeline(t, targetRoot)

	files, err := loadTemplateMergeFiles("duckdb")
	require.NoError(t, err)
	require.NoError(t, collectMergeConflicts(pipelinePath, files))
	require.NoError(t, writeTemplateFiles(pipelinePath, files))

	require.FileExists(t, filepath.Join(pipelinePath, "assets", "macro_example.sql"))
	require.FileExists(t, filepath.Join(pipelinePath, "macros", "aggregations.sql"))
}

func TestLoadTemplateMergeFilesIncludesWrappedPipelines(t *testing.T) {
	files, err := loadTemplateMergeFiles("self-heal-demo")
	require.NoError(t, err)

	paths := mergeFilePaths(files)
	require.Contains(t, paths, "assets/orders.asset.yml")
	require.Contains(t, paths, "assets/daily_activity.sql")
}

func TestLoadEcommerceMergeFilesKeepsOnlyMergeablePaths(t *testing.T) {
	choices := &EcommerceChoices{
		Warehouse: warehouseClickHouse,
		Payments:  paymentsStripe,
		Marketing: marketingKlaviyo,
		Ads:       []string{adsFacebook},
		Analytics: analyticsGA4,
	}

	generated, err := buildEcommerceFiles(choices)
	require.NoError(t, err)
	require.Contains(t, generated, "pipeline.yml")

	files, err := loadEcommerceMergeFiles(choices)
	require.NoError(t, err)

	paths := mergeFilePaths(files)
	require.Contains(t, paths, "assets/raw/stripe_charges.asset.yml")
	require.Contains(t, paths, "assets/raw/facebook_campaigns.asset.yml")
	require.Contains(t, paths, "assets/raw/klaviyo_campaigns.asset.yml")
	require.Contains(t, paths, "assets/raw/ga4_events.asset.yml")
	// pipeline.yml is generated but deliberately not merged.
	require.NotContains(t, paths, "pipeline.yml")
	require.Len(t, files, len(generated)-1)
}

func TestInitMergeLeavesProjectUntouchedWhenAssetsCollide(t *testing.T) {
	targetRoot := t.TempDir()
	t.Chdir(targetRoot)
	initGitRepository(t, targetRoot)
	pipelinePath := createExistingPipeline(t, targetRoot)
	require.NoError(t, os.WriteFile(filepath.Join(pipelinePath, "assets", "player_stats.sql"), []byte("select 'keep me'\n"), 0o644))

	exitCode := captureExitCode(t)
	err := Init().Run(t.Context(), []string{"init", "default", pipelinePath, "--merge"})
	require.Error(t, err)
	require.Equal(t, 1, *exitCode)

	// The config merge must not have run: a conflicting copy is aborted first.
	require.NoFileExists(t, filepath.Join(targetRoot, ".bruin.yml"))
	require.NoFileExists(t, filepath.Join(targetRoot, ".gitignore"))
	require.NoFileExists(t, filepath.Join(pipelinePath, "assets", "my_python_asset.py"))
}

func TestProjectConfigSummaryReportsExistingConfig(t *testing.T) {
	targetRoot := t.TempDir()
	initGitRepository(t, targetRoot)
	pipelinePath := createExistingPipeline(t, targetRoot)
	configPath := filepath.Join(targetRoot, ".bruin.yml")
	require.NoError(t, os.WriteFile(configPath, []byte("default_environment: default\n"), 0o644))

	summary := projectConfigSummary(pipelinePath)
	require.Equal(t, configPath, summary.path)
	require.True(t, summary.existed)
	require.False(t, summary.merged)
	require.NotEmpty(t, summary.locationNote)
}

func TestProjectConfigSummaryIsEmptyOutsideAGitRepository(t *testing.T) {
	targetRoot := t.TempDir()
	pipelinePath := createExistingPipeline(t, targetRoot)

	require.Equal(t, initConfigSummary{}, projectConfigSummary(pipelinePath))
}

func TestWriteTemplateFilesRemovesWhatItWroteOnFailure(t *testing.T) {
	if runtime.GOOS == osWindows {
		t.Skip("directory permissions do not block file creation on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}

	targetRoot := t.TempDir()
	pipelinePath := createExistingPipeline(t, targetRoot)

	files, err := loadTemplateMergeFiles("stripe-bigquery")
	require.NoError(t, err)
	require.NoError(t, collectMergeConflicts(pipelinePath, files))

	// A read-only directory makes the copy fail partway through, after the
	// alphabetically earlier assets/stripe_raw files have been written.
	blockedDir := filepath.Join(pipelinePath, "assets", "stripe_reports")
	require.NoError(t, os.MkdirAll(blockedDir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(blockedDir, 0o755) })

	require.Error(t, writeTemplateFiles(pipelinePath, files))

	// Nothing the failed copy wrote is left behind, so a retry is still possible.
	require.NoFileExists(t, filepath.Join(pipelinePath, "assets", "stripe_raw", "customer.asset.yml"))
	require.NoError(t, os.Chmod(blockedDir, 0o755))
	require.NoError(t, collectMergeConflicts(pipelinePath, files))
	require.NoError(t, writeTemplateFiles(pipelinePath, files))
	require.FileExists(t, filepath.Join(pipelinePath, "assets", "stripe_raw", "customer.asset.yml"))
	require.FileExists(t, filepath.Join(pipelinePath, "assets", "stripe_reports", "monthly_subscription_kpis.sql"))
}

func TestInitMergeValidatesDestinationBeforeLoadingTheTemplate(t *testing.T) {
	targetRoot := t.TempDir()
	t.Chdir(targetRoot)
	initGitRepository(t, targetRoot)
	targetPath := filepath.Join(targetRoot, "not-a-pipeline")
	require.NoError(t, os.MkdirAll(targetPath, 0o755))

	exitCode := captureExitCode(t)
	err := Init().Run(t.Context(), []string{"init", "default", targetPath, "--merge"})
	require.Error(t, err)
	require.Equal(t, 1, *exitCode)
	require.NoDirExists(t, filepath.Join(targetPath, "assets"))
	require.NoFileExists(t, filepath.Join(targetRoot, ".bruin.yml"))
}

func TestWriteFileAtomicallyLeavesNoTemporaryFiles(t *testing.T) {
	targetRoot := t.TempDir()
	path := filepath.Join(targetRoot, ".bruin.yml")
	require.NoError(t, os.WriteFile(path, []byte("old\n"), 0o644))

	require.NoError(t, writeFileAtomically(path, []byte("new\n")))

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "new\n", string(content))

	entries, err := os.ReadDir(targetRoot)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, ".bruin.yml", entries[0].Name())
}

func TestWriteFileAtomicallyKeepsTheOriginalOnFailure(t *testing.T) {
	if runtime.GOOS == osWindows {
		t.Skip("directory permissions do not block file creation on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}

	targetRoot := t.TempDir()
	path := filepath.Join(targetRoot, ".bruin.yml")
	require.NoError(t, os.WriteFile(path, []byte("keep me\n"), 0o644))
	require.NoError(t, os.Chmod(targetRoot, 0o555))
	t.Cleanup(func() { _ = os.Chmod(targetRoot, 0o755) })

	require.Error(t, writeFileAtomically(path, []byte("truncated")))

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "keep me\n", string(content))
}

func TestWriteFileAtomicallyPreservesExistingPermissions(t *testing.T) {
	if runtime.GOOS == osWindows {
		t.Skip("file modes are not meaningful on Windows")
	}

	targetRoot := t.TempDir()
	path := filepath.Join(targetRoot, ".bruin.yml")
	require.NoError(t, os.WriteFile(path, []byte("old\n"), 0o600))

	require.NoError(t, writeFileAtomically(path, []byte("new\n")))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestWriteFileAtomicallyUsesDefaultPermissionsForNewFiles(t *testing.T) {
	if runtime.GOOS == osWindows {
		t.Skip("file modes are not meaningful on Windows")
	}

	targetRoot := t.TempDir()
	path := filepath.Join(targetRoot, ".bruin.yml")

	require.NoError(t, writeFileAtomically(path, []byte("new\n")))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm())
}
