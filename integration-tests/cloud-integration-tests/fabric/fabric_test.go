package fabric

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
)

func TestFabricWorkflows(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	configFlags := []string{"--config-file", filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}

	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	tableName := "products_" + runID

	tempDir := t.TempDir()
	tempPipelineDir := filepath.Join(tempDir, "fw-asset-query")
	tempAssetsDir := filepath.Join(tempPipelineDir, "assets")
	require.NoError(t, os.MkdirAll(tempAssetsDir, 0o755))

	assetTemplatePath := filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products.sql")
	assetContent, err := os.ReadFile(assetTemplatePath)
	require.NoError(t, err)
	assetContent = []byte(strings.ReplaceAll(string(assetContent), "__RUN_ID__", "_"+runID))

	tempAssetPath := filepath.Join(tempAssetsDir, "products.sql")
	require.NoError(t, os.WriteFile(tempAssetPath, assetContent, 0o644))

	pipelineTemplatePath := filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/pipeline.yml")
	pipelineContent, err := os.ReadFile(pipelineTemplatePath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(tempPipelineDir, "pipeline.yml"), pipelineContent, 0o644))

	mergeTable := "bruin_test.merge_orders_" + runID
	mergeTemplateDir := filepath.Join(currentFolder, "test-pipelines/native-merge-pipeline")
	mergePipelineDir := filepath.Join(tempDir, "fw-native-merge")
	mergeAssetPath := filepath.Join(mergePipelineDir, "assets", "merge_orders.sql")
	require.NoError(t, os.MkdirAll(filepath.Join(mergePipelineDir, "assets"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(mergePipelineDir, "resources"), 0o755))
	copyWithRunID(t, filepath.Join(mergeTemplateDir, "pipeline.yml"), filepath.Join(mergePipelineDir, "pipeline.yml"), runID)
	for _, resource := range []string{"merge_orders_initial.sql", "merge_orders_update.sql", "merge_orders_duplicate.sql"} {
		copyWithRunID(t, filepath.Join(mergeTemplateDir, "resources", resource), filepath.Join(mergePipelineDir, "resources", resource), runID)
	}

	useMergeAsset := func(resource string) e2e.Task {
		return e2e.Task{
			Name:     "use " + resource + " as the merge asset",
			Command:  "cp",
			Args:     []string{filepath.Join(mergePipelineDir, "resources", resource), mergeAssetPath},
			Expected: e2e.Output{ExitCode: 0},
			Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
		}
	}
	runMergeAsset := func(name string, exitCode int, contains ...string) e2e.Task {
		return e2e.Task{
			Name:     name,
			Command:  binary,
			Args:     append(append([]string{"run"}, configFlags...), "--env", "default", mergeAssetPath),
			Expected: e2e.Output{ExitCode: exitCode, Contains: contains},
			Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByContains},
		}
	}
	queryMergeTable := func(name, expectedCSV string) e2e.Task {
		return e2e.Task{
			Name:    name,
			Command: binary,
			Args: append(append([]string{"query"}, configFlags...), "--connection", "fabric-default", "--output", "csv", "--query",
				"SELECT order_id, status, CAST(amount AS VARCHAR(20)) AS amount, CONVERT(VARCHAR(19), updated_at, 120) AS updated_at, CONVERT(VARCHAR(19), created_at, 120) AS created_at FROM "+mergeTable+" ORDER BY order_id;"),
			Expected: e2e.Output{ExitCode: 0, CSVFile: filepath.Join(mergeTemplateDir, "expectations", expectedCSV)},
			Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByCSV},
		}
	}

	tests := []struct {
		name     string
		workflow e2e.Workflow
	}{
		{
			name: "fabric-products-create-and-validate",
			workflow: e2e.Workflow{
				Name: "fabric-products-create-and-validate",
				Steps: []e2e.Task{
					{
						Name:       "initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: tempPipelineDir,
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "create the initial products table",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", tempAssetPath),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "query the products table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "fabric-default", "--query", "SELECT product_id, product_name, CAST(price AS VARCHAR(20)) AS price, stock FROM bruin_test."+tableName+" ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/expected_products_table.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
				},
			},
		},
		{
			name: "fabric-native-merge",
			workflow: e2e.Workflow{
				Name: "fabric-native-merge",
				Steps: []e2e.Task{
					{
						Name:       "initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: mergePipelineDir,
						Expected:   e2e.Output{ExitCode: 0},
						Asserts:    []func(*e2e.Task) error{e2e.AssertByExitCode},
					},
					useMergeAsset("merge_orders_initial.sql"),
					// The table does not exist yet and no --full-refresh is given, so the
					// refresh_restricted asset has to create it from its columns.
					runMergeAsset("merge into a missing table", 0),
					queryMergeTable("query the table after the first run", "after_initial.csv"),
					useMergeAsset("merge_orders_update.sql"),
					runMergeAsset("merge newer, stale, and new rows", 0),
					queryMergeTable("query the table after the update", "after_update.csv"),
					useMergeAsset("merge_orders_duplicate.sql"),
					runMergeAsset("fail on duplicate primary keys", 1, "more than one row for the same primary key (order_id)"),
					queryMergeTable("query the table after the failed run", "after_update.csv"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, tt.workflow.Run())
		})
	}
}

func copyWithRunID(t *testing.T, src, dst, runID string) {
	t.Helper()

	content, err := os.ReadFile(src)
	require.NoError(t, err)
	content = []byte(strings.ReplaceAll(string(content), "__RUN_ID__", "_"+runID))
	require.NoError(t, os.WriteFile(dst, content, 0o644))
}
