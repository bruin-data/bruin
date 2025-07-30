package bigquery

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
)

func TestBigQueryIndividualTasks(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	configFlags := []string{"--config-file", filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}

	tasks := []e2e.Task{
		{
			Name:    "bigquery-run-pipeline",
			Command: binary,
			Args:    append(append([]string{"run"}, configFlags...), "--env", "default", "--full-refresh", filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products.sql")),
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "bigquery-run-single-asset",
			Command: binary,
			Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products.sql")),
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
	}

	for _, task := range tasks {
		t.Run(task.Name, func(t *testing.T) {
			t.Parallel()

			err := task.Run()
			require.NoError(t, err, "Task %s failed: %v", task.Name, err)

			t.Logf("Task '%s' completed successfully", task.Name)
		})
	}
}

func TestBigQueryWorkflows(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	tests := []struct {
		name     string
		workflow func(tempDir string, configFlags []string, binary string) e2e.Workflow
	}{
		{
			name: "bigquery-products-create-and-validate",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "bigquery-products-create-and-validate",
					Steps: []e2e.Task{
						{
							Name:    "create the initial products table",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/asset-query-pipeline/assets/products.sql")),
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
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT PRODUCT_ID, PRODUCT_NAME, PRICE, STOCK FROM dataset.products ORDER BY PRODUCT_ID;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/asset-query-pipeline/expected_products_table.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
					},
				}
			},
		},
		{
			name: "bigquery-scd2_by_column",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
				Name: "athena-scd2_by_column",
				Steps: []e2e.Task{
						{
							Name:    "scd2-by-column: drop table if exists",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "DROP TABLE IF EXISTS test.menu;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 1,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-column: restore menu asset to initial state",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/scd2-pipelines/resources/menu_original.sql"), filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")},
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-column: create the initial table",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-column: query the initial table",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/scd2_by_col_expected_initial.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "scd2-by-column: copy menu_updated_01.sql to menu.sql",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/scd2-pipelines/resources/menu_updated_01.sql"), filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")},
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-column: run menu_updated_01.sql with SCD2 materialization",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-column: query the updated table 01",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/scd2_by_col_expected_updated_01.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "scd2-by-column: copy menu_updated_02.sql to menu.sql",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/scd2-pipelines/resources/menu_updated_02.sql"), filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")},
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-column: run menu_updated_02.sql with SCD2 materialization",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-column: query the updated table 02",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/scd2_by_col_expected_updated_02.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "scd2-by-column: drop the table (expect error but table will be dropped)",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "athena-default", "--query", "DROP TABLE IF EXISTS test.menu;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-column: confirm the table is dropped",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "athena-default", "--query", "SELECT * FROM test.menu;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 1, // Should fail because table doesn't exist
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
					},
				}
			},
		},
		{
			name: "bigquery-scd2-by-time",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "bigquery-scd2-by-time",
					Steps: []e2e.Task{
						{
							Name:    "scd2-by-time: drop table if exists",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql"), "--query", "DROP TABLE IF EXISTS test.products;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 1,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode, // Should fail because table doesn't exist
							},
						},
						{
							Name:    "scd2-by-time: restore products asset to initial state",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/scd2-pipelines/resources/products_original.sql"), filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")},
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-time: create the initial products table",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-time: query the initial table",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql"), "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/expectations/scd2_by_time_expected_initial.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "scd2-by-time: copy products_updated_01.sql to products.sql",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/scd2-pipelines/resources/products_updated_01.sql"), filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")},
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-time: run products_updated_01.sql with SCD2 materialization",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-time: query the updated table 01",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql"), "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/expectations/scd2_by_time_expected_update_01.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "scd2-by-time: copy products_updated_02.sql to products.sql",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/scd2-pipelines/resources/products_updated_02.sql"), filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")},
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-time: run products_updated_02.sql with SCD2 materialization",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-time: query the updated table 02",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql"), "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/expectations/scd2_by_time_expected_update_02.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "scd2-by-time: drop the table (expect error but table will be dropped)",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "athena-default", "--query", "DROP TABLE IF EXISTS test.products;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-time: confirm the table is dropped",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "athena-default", "--query", "SELECT * FROM test.products;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 1, // Should fail because table doesn't exist
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
					},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tempDir, tempErr := os.MkdirTemp("", "bigquery-test-*")
			require.NoError(t, tempErr, "Failed to create temporary directory")
			defer os.RemoveAll(tempDir)

			srcPipelines := filepath.Join(currentFolder, "test-pipelines")
			destPipelines := filepath.Join(tempDir, "test-pipelines")
			copyErr := copyDir(srcPipelines, destPipelines)
			require.NoError(t, copyErr, "Failed to copy test-pipelines to tempDir")
			runGitInitInTempPipelines(t, tempDir)

			configFlags := []string{"--config-file", filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}
			workflow := tt.workflow(tempDir, configFlags, binary)
			err := workflow.Run()
			require.NoError(t, err, "Workflow %s failed: %v", workflow.Name, err)
			t.Logf("Workflow '%s' completed successfully", workflow.Name)
		})
	}
}

func copyDir(src string, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		targetPath := filepath.Join(dst, relPath)
		if info.IsDir() {
			return os.MkdirAll(targetPath, info.Mode())
		}
		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer srcFile.Close()
		dstFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
		if err != nil {
			return err
		}
		defer dstFile.Close()
		_, err = io.Copy(dstFile, srcFile)
		return err
	})
}

func runGitInitInTempPipelines(t *testing.T, tempDir string) {
	gitDir := filepath.Join(tempDir, "test-pipelines")
	cmd := exec.Command("git", "init")
	cmd.Dir = gitDir
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to run 'git init' in %s: %s", gitDir, string(output))
}
