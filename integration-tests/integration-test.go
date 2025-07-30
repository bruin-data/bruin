package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

var currentFolder string

var (
	stateForFirstRun = &scheduler.PipelineState{
		Parameters: scheduler.RunConfig{
			Downstream:   false,
			Workers:      16,
			Environment:  "",
			Force:        false,
			PushMetadata: false,
			NoLogFile:    false,
			FullRefresh:  false,
			UsePip:       false,
			Tag:          "",
			ExcludeTag:   "",
			Only:         nil,
		},
		Metadata: scheduler.Metadata{
			Version: "dev",
			OS:      runtime.GOOS,
		},
		State: []*scheduler.PipelineAssetState{
			{
				Name:   "product_categories",
				Status: "succeeded",
			},
			{
				Name:   "product_price_summary",
				Status: "succeeded",
			},
			{
				Name:   "products",
				Status: "succeeded",
			},
			{
				Name:   "shipping_providers",
				Status: "failed",
			},
		},
		Version:           "1.0.0",
		CompatibilityHash: "e62a4c57b82d5452bc57cab24f45eb4abda2a737b0269492de0030fba452ed7e",
	}
	stateForContinueRun = &scheduler.PipelineState{
		Parameters: scheduler.RunConfig{
			Downstream:   false,
			StartDate:    "2024-12-22 00:00:00.000000",
			EndDate:      "2024-12-22 23:59:59.999999",
			Workers:      16,
			Environment:  "",
			Force:        false,
			PushMetadata: false,
			NoLogFile:    false,
			FullRefresh:  false,
			UsePip:       false,
			Tag:          "",
			ExcludeTag:   "",
			Only:         nil,
		},
		Metadata: scheduler.Metadata{
			Version: "dev",
			OS:      runtime.GOOS,
		},
		State: []*scheduler.PipelineAssetState{
			{
				Name:   "product_categories",
				Status: "skipped",
			},
			{
				Name:   "product_price_summary",
				Status: "skipped",
			},
			{
				Name:   "products",
				Status: "skipped",
			},

			{
				Name:   "shipping_providers",
				Status: "succeeded",
			},
		},
		Version:           "1.0.0",
		CompatibilityHash: "e62a4c57b82d5452bc57cab24f45eb4abda2a737b0269492de0030fba452ed7e",
	}
)

func main() {
	wd, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	currentFolder = filepath.Join(wd, "integration-tests")

	if runtime.GOOS == "windows" {
		out, err := exec.Command("mv", "bin/bruin", "bin/bruin.exe").Output()
		if err != nil {
			fmt.Printf("failed to rename binary for execution on windows: %s\n", out)
			panic(err)
		}
	}

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable = "bruin.exe"
	}
	binary := filepath.Join(wd, "bin", executable)

	includeIngestr := os.Getenv("INCLUDE_INGESTR") == "1"
	runIntegrationTests(binary, currentFolder, includeIngestr)
	runIntegrationWorkflow(binary, currentFolder)
}

func runIntegrationWorkflow(binary string, currentFolder string) {
	tempdir, err := os.MkdirTemp(os.TempDir(), "bruin-test")
	if err != nil {
		fmt.Println("Failed to create temporary directory:", err)
		os.Exit(1)
	}

	workflows := getWorkflow(binary, currentFolder, tempdir)

	for _, workflow := range workflows {
		err := workflow.Run()
		if err != nil {
			fmt.Printf("Assert error: %v\n", err)
			os.Exit(1)
		}
	}
}

func runIntegrationTests(binary string, currentFolder string, includeIngestr bool) {
	tests := getTasks(binary, currentFolder)
	if includeIngestr {
		ingestrTasks := getIngestrTasks(binary, currentFolder)
		tests = append(tests, ingestrTasks...)
	}
	for _, test := range tests {
		if err := test.Run(); err != nil {
			fmt.Printf("%s Assert error: %v\n", test.Name, err)
			os.Exit(1)
		}
	}
}

func getWorkflow(binary string, currentFolder string, tempdir string) []e2e.Workflow {
	tempfile := GetTempFile(tempdir, "shipping_providers.sql")
	return []e2e.Workflow{
		{
			Name: "continue after failure",
			Steps: []e2e.Task{
				{
					Name:    "run first time",
					Command: binary,
					Args:    []string{"run", "--start-date", "2024-01-01", "--end-date", "2024-12-31", filepath.Join(currentFolder, "continue")},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 1,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertCustomState(filepath.Join(currentFolder, "/logs/runs/continue_duckdb"), stateForFirstRun),
					},
				},
				{
					Name:    "copy shipping_providers.sql to tempfile",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "continue/assets/shipping_providers.sql"), tempfile},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "copy shipping_providers.sql to continue",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "shipping_providers.sql"), filepath.Join(currentFolder, "continue/assets/shipping_providers.sql")},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "run continue",
					Command: binary,
					Args:    []string{"run", "--start-date", "2024-01-01", "--end-date", "2024-12-31", "--continue", filepath.Join(currentFolder, "continue")},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertCustomState(filepath.Join(currentFolder, "/logs/runs/continue_duckdb"), stateForContinueRun),
					},
				},
				{
					Name:    "copy broken shipping_providers.sql back to continue",
					Command: "cp",
					Args:    []string{tempfile, filepath.Join(currentFolder, "continue/assets/shipping_providers.sql")},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
			},
		},
		{
			Name: "Bruin init",
			Steps: []e2e.Task{
				{
					Name:       "create a test directory",
					Command:    "mkdir",
					WorkingDir: tempdir,
					Args:       []string{"-p", filepath.Join(tempdir, "test-bruin-init")},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:       "run git init",
					Command:    "git",
					Args:       []string{"init"},
					WorkingDir: filepath.Join(tempdir, "test-bruin-init"),
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:       "run bruin init",
					Command:    binary,
					Args:       []string{"init", "clickhouse"},
					WorkingDir: filepath.Join(tempdir, "test-bruin-init"),
					Expected: e2e.Output{
						ExitCode: 0,
						Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_bruin.yaml")),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByYAML,
					},
				},
				{
					Name:       "run bruin init 2",
					Command:    binary,
					Args:       []string{"init", "clickhouse", "clickhouse2"},
					WorkingDir: filepath.Join(tempdir, "test-bruin-init"),
					Expected: e2e.Output{
						ExitCode: 0,
						Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_bruin.yaml")),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByYAML,
					},
				},
				{
					Name:       "run bruin init chess",
					Command:    binary,
					Args:       []string{"init", "chess"},
					WorkingDir: filepath.Join(tempdir, "test-bruin-init"),
					Expected: e2e.Output{
						ExitCode: 0,
						Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_bruin_chess.yaml")),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByYAML,
					},
				},
			},
		},
		{
			Name: "Time materialization",
			Steps: []e2e.Task{
				{
					Name:    "restore asset to initial state",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "resources/products.sql"), filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline/assets/products.sql")},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "create the table",
					Command: binary,
					Args:    []string{"run", "--full-refresh", "--env", "env-time-materialization", filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline")},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "query the initial table",
					Command: binary,
					Args:    []string{"query", "--env", "env-time-materialization", "--asset", filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline/assets/products.sql"), "--query", "SELECT * FROM PRODUCTS;", "--output", "json"},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
						Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline/expectations/initial_expected.json")),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByOutputJSON,
					},
				},
				{
					Name:    "copy products_updated.sql to products.sql",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "resources/products_updated.sql"), filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline/assets/products.sql")},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "update table with time materialization",
					Command: binary,
					Args:    []string{"run", "--start-date", "2025-03-01", "--end-date", "2025-03-31", "--env", "env-time-materialization", filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline/assets/products.sql")},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "query the updated table with time materialization",
					Command: binary,
					Args:    []string{"query", "--env", "env-time-materialization", "--asset", filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline/assets/products.sql"), "--query", "SELECT * FROM PRODUCTS;", "--output", "json"},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
						Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline/expectations/final_expected.json")),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByOutputJSON,
					},
				},
			},
		},
		{
			Name: "Run pipeline with nameless asset",
			Steps: []e2e.Task{
				{
					Name:    "create the table",
					Command: binary,
					Args:    []string{"run", "--env", "env-run-nameless-asset", filepath.Join(currentFolder, "test-pipelines/run-nameless-asset-pipeline")},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "query the table",
					Command: binary,
					Args:    []string{"query", "--env", "env-run-nameless-asset", "--asset", filepath.Join(currentFolder, "test-pipelines/run-nameless-asset-pipeline/assets/test2/shipping_providers.sql"), "--query", "SELECT * FROM test2.shipping_providers", "--output", "json"},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
						Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/run-nameless-asset-pipeline/expected.json")),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByOutputJSON,
					},
				},
			},
		},
		{
			Name: "interval modifiers",
			Steps: []e2e.Task{
				{
					Name:    "interval-modifiers",
					Command: binary,
					Args:    []string{"run", "--apply-interval-modifiers", "-env", "env-interval-modifiers", "--start-date", "2025-04-02T09:30:00.000Z", "--end-date", "2025-04-02T11:30:00.000Z", filepath.Join(currentFolder, "test-pipelines/interval-modifiers-pipeline/assets/products.sql")},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "query the table",
					Command: binary,
					Args:    []string{"query", "--env", "env-interval-modifiers", "--asset", filepath.Join(currentFolder, "test-pipelines/interval-modifiers-pipeline/assets/products.sql"), "--query", "SELECT * FROM products", "--output", "json"},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
						Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/interval-modifiers-pipeline/expectations/final_expected.json")),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByOutputJSON,
					},
				},
			},
		},
		{
			Name: "run pipeline with variables",
			Steps: []e2e.Task{
				{
					Name:    "run pipeline",
					Command: binary,
					Args: []string{
						"run",
						filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/users.sql"),
					},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "validate output (run pipeline)",
					Command: binary,
					Args: []string{
						"query",
						"--connection", "duckdb-variables",
						"--query", `SELECT name FROM public.users`,
					},
					WorkingDir: currentFolder,
					Expected: e2e.Output{
						ExitCode: 0,
						Output:   "┌──────┐\n│ NAME │\n├──────┤\n│ jhon │\n│ erik │\n└──────┘\n",
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByOutputString,
					},
				},
				{
					Name:    "run pipeline with json override",
					Command: binary,
					Args: []string{
						"run",
						"--var", `{"users": ["mark", "nicholas"]}`,
						filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/users.sql"),
					},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "validate output (run pipeline with json override)",
					Command: binary,
					Args: []string{
						"query",
						"--connection", "duckdb-variables",
						"--query", `SELECT name FROM public.users`,
					},
					WorkingDir: currentFolder,
					Expected: e2e.Output{
						ExitCode: 0,
						Output:   "┌──────────┐\n│ NAME     │\n├──────────┤\n│ mark     │\n│ nicholas │\n└──────────┘\n",
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByOutputString,
					},
				},
				{
					Name:    "run pipeline with key=value override",
					Command: binary,
					Args: []string{
						"run",
						"--var", `users=["tanaka", "yamaguchi"]`,
						filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/users.sql"),
					},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "validate output (run pipeline with key=value override)",
					Command: binary,
					Args: []string{
						"query",
						"--connection", "duckdb-variables",
						"--query", `SELECT name FROM public.users`,
					},
					WorkingDir: currentFolder,
					Expected: e2e.Output{
						ExitCode: 0,
						Output:   "┌───────────┐\n│ NAME      │\n├───────────┤\n│ tanaka    │\n│ yamaguchi │\n└───────────┘\n",
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByOutputString,
					},
				},
				{
					Name:    "get databases from duckdb",
					Command: binary,
					Args: []string{
						"internal",
						"fetch-databases",
						"--connection", "duckdb-variables",
					},
					WorkingDir: currentFolder,
					Expected: e2e.Output{
						ExitCode: 0,
						Contains: []string{"PUBLIC"},
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByContains,
					},
				},
			},
		},
		{
			Name: "run pipeline with scd2 by column",
			Steps: []e2e.Task{
				{
					Name:    "scd2_by_col: restore asset to initial state",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/resources/menu_original.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-column-pipeline/assets/menu.sql")},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2_by_col: create the table",
					Command: binary,
					Args:    []string{"run", "--full-refresh", "--config-file", filepath.Join(currentFolder, ".bruin.yml"), filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-column-pipeline")},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2_by_col: query the initial table",
					Command: binary,
					Args:    []string{"query", "--asset", filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-column-pipeline/assets/menu.sql"), "--config-file", filepath.Join(currentFolder, ".bruin.yml"), "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
						CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/expected_initial.csv"),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByCSV,
					},
				},
				{
					Name:    "scd2_by_col: copy menu_updated.sql to menu.sql",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/resources/menu_updated.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-column-pipeline/assets/menu.sql")},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2_by_col: update table with scd2_by_column materialization",
					Command: binary,
					Args:    []string{"run", "--config-file", filepath.Join(currentFolder, ".bruin.yml"), filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-column-pipeline/assets/menu.sql")},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2_by_col: query the scd2_by_column materialized table",
					Command: binary,
					Args:    []string{"query", "--config-file", filepath.Join(currentFolder, ".bruin.yml"), "--asset", filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price,_is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
						CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/final_expected.csv"),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByCSV,
					},
				},
			},
		},
		{
			Name: "run pipeline with scd2 by time",
			Steps: []e2e.Task{	
				{
					Name:    "scd2_by_time: restore asset to initial state",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/resources/products_original.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-time-pipeline/assets/products.sql")},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2_by_time: create the table",
					Command: binary,
					Args:    []string{"run", "--full-refresh", "--config-file", filepath.Join(currentFolder, ".bruin.yml"), filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-time-pipeline")},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2_by_time: query the initial table",
					Command: binary,
					Args:    []string{"query", "--asset", filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-time-pipeline/assets/products.sql"), "--config-file", filepath.Join(currentFolder, ".bruin.yml"), "--query", "SELECT  product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
						CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-time-pipeline/expectations/scd2_time_initial_expected.csv"),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByCSV,
					},
				},
				{
					Name:    "scd2_by_time: copy products_updated.sql to products.sql",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/resources/products_updated.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-time-pipeline/assets/products.sql")},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2_by_time: update table with scd2_by_time materialization",
					Command: binary,
					Args:    []string{"run", "--config-file", filepath.Join(currentFolder, ".bruin.yml"), filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-time-pipeline/assets/products.sql")},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2_by_time: query the scd2_by_time materialized table",
					Command: binary,
					Args:    []string{"query", "--config-file", filepath.Join(currentFolder, ".bruin.yml"), "--asset", filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-time-pipeline/assets/products.sql"), "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
						CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-time-pipeline/expectations/scd2_time_final_expected.csv"),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByCSV,
					},
				},
				{
					Name:    "scd2_by_time: copy products_latest.sql to products.sql",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/resources/products_latest.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-time-pipeline/assets/products.sql")},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2_by_time: update table again with scd2_by_time materialization",
					Command: binary,
					Args:    []string{"run", "--config-file", filepath.Join(currentFolder, ".bruin.yml"), filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-time-pipeline/assets/products.sql")},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2_by_time: query the scd2_by_time materialized table",
					Command: binary,
					Args:    []string{"query", "--config-file", filepath.Join(currentFolder, ".bruin.yml"), "--asset", filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-time-pipeline/assets/products.sql"), "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
						CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-time-pipeline/expectations/scd2_time_latest_expected.csv"),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByCSV,
					},
				},
				{
					Name:    "scd2_by_time: restore asset to initial state",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/resources/products_original.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/duck-test-pipes/scd2-by-time-pipeline/assets/products.sql")},
					Env:     []string{},
		
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
			},
		},
	}
}

//nolint:maintidx
func getTasks(binary string, currentFolder string) []e2e.Task {
	return []e2e.Task{
		{
			Name:    "builtin-policies",
			Command: binary,
			Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/policies-builtin")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Successfully validated 2 assets across 1 pipeline"},
			},
			WorkingDir: currentFolder,
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "custom-policies",
			Command: binary,
			Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/policies-custom")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Successfully validated 1 assets across 1 pipeline"},
			},
			WorkingDir: currentFolder,
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "policy-selector",
			Command: binary,
			Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/policies-selector")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Successfully validated 1 assets across 1 pipeline"},
			},
			WorkingDir: currentFolder,
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "policy-non-compliance",
			Command: binary,
			Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/policies-non-compliant")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 1,
				Output:   "Checked 1 pipeline and found 3 issues",
			},
			WorkingDir: currentFolder,
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "policy-validate-single-asset",
			Command: binary,
			Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/policies-validate-single-asset/assets/target.sql")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			WorkingDir: currentFolder,
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "policy-variables",
			Command: binary,
			Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/policies-variables/assets/target.sql")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			WorkingDir: currentFolder,
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "policy-variables-fail",
			Command: binary,
			Args:    []string{"validate", "--var", `message="This should fail"`, filepath.Join(currentFolder, "test-pipelines/policies-variables/assets/target.sql")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 1,
			},
			WorkingDir: currentFolder,
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:          "parse-whole-pipeline",
			Command:       binary,
			Args:          []string{"internal", "parse-pipeline", filepath.Join(currentFolder, "test-pipelines/parse-whole-pipeline")},
			Env:           []string{},
			SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-whole-pipeline/expectations/pipeline.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "render-variables",
			Command:       binary,
			Args:          []string{"render", filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/users.sql")},
			Env:           []string{},
			SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"CREATE TABLE public.users", "SELECT 'jhon' as name", "SELECT 'erik' as name"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "render-variables-override-json",
			Command: binary,
			Args: []string{
				"render",
				"--var", `{"users": ["mark", "nicholas"]}`,
				filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/users.sql")},
			Env:           []string{},
			SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"CREATE TABLE public.users", "SELECT 'mark' as name", "SELECT 'nicholas' as name"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "render-variables-override-key-val",
			Command: binary,
			Args: []string{
				"render",
				"--var", `users=["mark", "nicholas"]`,
				filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/users.sql")},
			Env:           []string{},
			SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"CREATE TABLE public.users", "SELECT 'mark' as name", "SELECT 'nicholas' as name"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "python-happy-path-run",
			Command: binary,
			Args: []string{
				"run",
				filepath.Join(currentFolder, "test-pipelines/happy-path/assets/happy.py"),
			},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "python-variable-injection",
			Command: binary,
			Args: []string{
				"run",
				filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/load.py"),
			},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"env: dev", "users: jhon,erik"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "python-variable-injection-with-override",
			Command: binary,
			Args: []string{
				"run",
				"--var", `env="prod"`, `--var`, `{"users": ["sakamoto", "shin"]}`,
				filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/load.py"),
			},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"env: prod", "users: sakamoto,shin"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "python-variable-injection-with-env-override",
			Command: binary,
			Args: []string{
				"run",
				filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/load.py"),
			},
			Env: []string{`BRUIN_VARS={"env":"prod","users":["james","kirk"]}`},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"env: prod", "users: james,kirk"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "run-with-tags",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-with-tags", "--tag", "include", "--exclude-tag", "exclude", "--start-date", "2024-01-01", "--end-date", "2024-12-31", filepath.Join(currentFolder, "test-pipelines/run-with-tags-pipeline")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "query-asset",
			Command: binary,
			Args:    []string{"query", "--env", "env-query-asset", "--output", "json", "--asset", filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products.sql")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/expected.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:    "query-export",
			Command: binary,
			Args:    []string{"query", "--env", "env-query-export", "--output", "json", "--asset", filepath.Join(currentFolder, "test-pipelines/query-export-pipeline/assets/products.sql"), "--export"},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/query-export-pipeline/expected.csv")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByQueryResultCSV,
			},
		},
		{
			Name:    "run-with-filters",
			Command: binary,
			Args:    []string{"run", "-env", "env-run-with-filters", "--tag", "include", "--exclude-tag", "exclude", "--start-date", "2024-01-01", "--end-date", "2024-12-31", filepath.Join(currentFolder, "test-pipelines/run-with-filters-pipeline")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"bruin run completed", "Finished: shipping_provider", "Finished: products", "Finished: products:price:positive"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "format-if-fail",
			Command: binary,
			Args:    []string{"format", "--fail-if-changed", filepath.Join(currentFolder, "test-pipelines/format-if-changed-pipeline/assets/correctly-formatted.sql")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "run-main-with-filters",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-main-with-filters", "--tag", "include", "--exclude-tag", "exclude", "--only", "main", "--start-date", "2024-01-01", "--end-date", "2024-12-31", filepath.Join(currentFolder, "test-pipelines/run-main-with-filters-pipeline")},
			Env:     []string{},

			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"bruin run completed", "Finished: shipping_provider", "Finished: products"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "run-with-downstream",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-with-downstream", "--downstream", filepath.Join(currentFolder, "test-pipelines/run-with-downstream-pipeline/assets/products.sql")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"bruin run completed", "Finished: products", "Finished: products:price:positive", "Finished: product_price_summary", "Finished: product_price_summary:product_count:non_negative", "Finished: product_price_summary:total_stock:non_negative"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "run-main-with-downstream",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-main-with-downstream", "--downstream", "--only", "main", filepath.Join(currentFolder, "test-pipelines/run-main-with-downstream-pipeline/assets/products.sql")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"bruin run completed", "Finished: products", "Finished: product_price_summary"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "push-metadata",
			Command: binary,
			Args:    []string{"run", "--env", "env-push-metadata", "--push-metadata", "--only", "push-metadata", filepath.Join(currentFolder, "test-pipelines/push-metadata-pipeline")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Running:  shopify_raw.products:metadata-push", "Running:  shopify_raw.inventory_items:metadata-push"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "validate-happy-path",
			Command: binary,
			Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/happy-path")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "validate-with-exclude-tags",
			Command: binary,
			Args:    []string{"validate", "--exclude-tag", "exclude", filepath.Join(currentFolder, "test-pipelines/validate-with-exclude-tag")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{" Successfully validated 4 assets across 1 pipeline, all good."},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "run-use-uv",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-use-uv", "--use-uv", filepath.Join(currentFolder, "test-pipelines/run-use-uv-pipeline")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "run-custom-check-count-false",
			Command: binary,
			Args:    []string{"run", "--env", "env-custom-check-count-false", filepath.Join(currentFolder, "test-pipelines/custom-check-count-false")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 1,
				Contains: []string{"custom check 'row_count' has returned 4 instead of the expected 7"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "run-custom-check-count-true",
			Command: binary,
			Args:    []string{"run", "--env", "env-custom-check-count-true", filepath.Join(currentFolder, "test-pipelines/custom-check-count-true")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:          "parse-asset-happy-path-asset-py",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-happy-path/assets/asset.py")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\"", "\"extends\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-happy-path/expectations/asset.py.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-happy-path-chess-games",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-happy-path/assets/chess_games.asset.yml")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\"", "\"extends\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-happy-path/expectations/chess_games.asset.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-happy-path-chess-profiles",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-happy-path/assets/chess_profiles.asset.yml")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\"", "\"extends\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-happy-path/expectations/chess_profiles.asset.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-happy-path-player-summary",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-happy-path/assets/player_summary.sql")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\"", "\"extends\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-happy-path/expectations/player_summary.sql.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:    "parse-asset-faulty-pipeline-error-sql",
			Command: binary,
			Args:    []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/faulty-pipeline/assets/error.sql")},
			Env:     []string{},

			Expected: e2e.Output{
				ExitCode: 1,
				Contains: []string{"error creating asset from file", "unmarshal errors"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:          "validate-missing-upstream",
			Command:       binary,
			Args:          []string{"validate", "-o", "json", filepath.Join(currentFolder, "test-pipelines/missing-upstream-pipeline/assets/nonexistent.sql")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\"", "\"extends\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/missing-upstream-pipeline/expectations/missing_upstream.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:    "run-malformed-sql",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-malformed-sql", filepath.Join(currentFolder, "test-pipelines/run-malformed-pipeline/assets/malformed.sql")},
			Env:     []string{},

			Expected: e2e.Output{
				ExitCode: 1,
				Contains: []string{"Parser Error: syntax error at or near \"S_ELECT_\"", "1 failed"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:          "internal-connections",
			Command:       binary,
			Args:          []string{"internal", "connections"},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\"", "\"extends\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_connections_schema.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "connections-list",
			Command:       binary,
			Args:          []string{"connections", "list", "-o", "json", currentFolder},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\"", "\"extends\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_connections.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-lineage",
			Command:       binary,
			Args:          []string{"internal", "parse-pipeline", "-c", filepath.Join(currentFolder, "test-pipelines/parse-lineage-pipeline")},
			Env:           []string{},
			SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-lineage-pipeline/expectations/lineage.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-lineage",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", "-c", filepath.Join(currentFolder, "test-pipelines/parse-asset-lineage-pipeline/assets/example.sql")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\"", "\"extends\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-asset-lineage-pipeline/expectations/lineage-asset.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:    "run-seed-data",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-seed-data", filepath.Join(currentFolder, "test-pipelines/run-seed-data/assets/seed.asset.yml")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"bruin run completed"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:          "parse-asset-seed-data",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/run-seed-data/assets/seed.asset.yml")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\"", "\"extends\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/run-seed-data/expectations/seed.asset.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:    "run-asset-default-option-pipeline",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-default-option", filepath.Join(currentFolder, "test-pipelines/parse-default-option")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Successfully validated 4 assets", "bruin run completed", "Finished: chess_playground.player_summary", "Finished: chess_playground.games", "Finished: python_asset"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:          "parse-asset-default-option-pipeline",
			Command:       binary,
			Args:          []string{"internal", "parse-pipeline", filepath.Join(currentFolder, "test-pipelines/parse-default-option")},
			Env:           []string{},
			SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-default-option/expectations/pipeline.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-default-option-asset-py",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-default-option/assets/asset.py")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\"", "\"extends\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-default-option/expectations/asset.py.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-default-option-chess-games",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-default-option/assets/chess_games.asset.yml")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\"", "\"extends\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-default-option/expectations/chess_games.asset.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-extends",
			Command:       binary,
			Args:          []string{"internal", "parse-pipeline", filepath.Join(currentFolder, "test-pipelines/parse-asset-extends")},
			Env:           []string{},
			SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-asset-extends/expectations/pipeline.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:    "run-python-materialization",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-python-materialization", filepath.Join(currentFolder, "test-pipelines/run-python-materialization")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Successfully validated 1 assets", "bruin run completed", "Finished: materialize.country"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "run-non-wait-symbolic",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-non-wait-symbolic", filepath.Join(currentFolder, "test-pipelines/run-non-wait-symbolic")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 1,
				Contains: []string{"Running:  example", "Finished: example", "Catalog Error: Table with name my does not exist!", "Failed: my-other-asset"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "test-render-template-this",
			Command: binary,
			Args:    []string{"run", "--env", "env-render-template-this", filepath.Join(currentFolder, "test-pipelines/render-template-this-pipeline")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Successfully validated 2 assets", "bruin run completed", "Finished: render_this.my_asset_2"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "test-ddl-duckdb",
			Command: binary,
			Args:    []string{"run", "--env", "env-duckdb-ddl", filepath.Join(currentFolder, "test-pipelines/duckdb-ddl-pipeline")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Successfully validated 2 assets", "bruin run completed", "Finished: my_schema.table_check"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "skip-python-assets-without-bruin-header",
			Command: binary,
			Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/empty-py-asset")},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
	}
}

func getIngestrTasks(binary string, currentFolder string) []e2e.Task {
	return []e2e.Task{
		{
			Name:    "ingestr-pipeline",
			Command: binary,
			Args:    []string{"run", "-env", "env-ingestr", filepath.Join(currentFolder, "test-pipelines/ingestr-pipeline")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"bruin run completed", "Finished: chess_playground.profiles", "Finished: chess_playground.games", "Finished: chess_playground.player_summary", "Finished: chess_playground.player_summary:total_games:positive"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
	}
}

func GetTempFile(tempdir string, filename string) string {
	tempfile, err := os.CreateTemp(tempdir, filename)
	if err != nil {
		fmt.Println("Failed to create temporary file:", err)
		os.Exit(1)
	}

	return tempfile.Name()
}
