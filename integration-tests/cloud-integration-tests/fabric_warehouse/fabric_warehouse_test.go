package fabric_warehouse

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
)

func TestFabricWarehouseWorkflows(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	configFlags := []string{"--config-file", filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}

	runID := fmt.Sprintf("%d", time.Now().UnixNano())
	tableName := fmt.Sprintf("products_%s", runID)

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

	tests := []struct {
		name     string
		workflow e2e.Workflow
	}{
		{
			name: "fabric-warehouse-products-create-and-validate",
			workflow: e2e.Workflow{
				Name: "fabric-warehouse-products-create-and-validate",
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
						Name:    "ensure schema exists",
						Command: binary,
						Args: append(append([]string{"query"}, configFlags...), "--connection", "fabric_warehouse-default", "--query",
							"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bruin_test') EXEC('CREATE SCHEMA bruin_test');"),
						Env: []string{},
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
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "fabric_warehouse-default", "--query", fmt.Sprintf("SELECT product_id, product_name, CAST(price AS VARCHAR(20)) AS price, stock FROM bruin_test.%s ORDER BY product_id;", tableName), "--output", "csv"),
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
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, tt.workflow.Run())
		})
	}
}
