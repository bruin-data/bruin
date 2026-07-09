package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/date"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/bruin-data/bruin/pkg/unittest"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

// UnitTest runs unit tests for SQL assets. The path may point at a single asset
// or a whole pipeline (in which case every asset that defines unit_tests is
// tested). Each test is compiled to a single read-only query that injects the
// mocked inputs as inline CTEs, run against the asset's configured connection
// (no DDL, no artifacts on the target), and its output is compared to the
// expectation.
func UnitTest() *cli.Command {
	return &cli.Command{
		Name:                      "unit-test",
		Usage:                     "run unit tests for SQL assets against their configured connection",
		ArgsUsage:                 "[path to an asset or a pipeline]",
		DisableSliceFlagSeparator: true,
		Flags: []cli.Flag{
			startDateFlag,
			endDateFlag,
			&cli.StringSliceFlag{
				Name:  "var",
				Usage: "override pipeline variables with custom values",
			},
			&cli.StringFlag{
				Name:  "environment",
				Usage: "the environment to use when resolving the asset's connection",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			inputPath := c.Args().Get(0)
			if inputPath == "" {
				inputPath = "."
			}

			if vars := c.StringSlice("var"); len(vars) > 0 {
				DefaultPipelineBuilder.AddPipelineMutator(variableOverridesMutator(vars))
			}

			startDate, err := date.ParseTime(c.String("start-date"))
			if err != nil {
				errorPrinter.Printf("Please give a valid start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS).\n")
				return cli.Exit("", 1)
			}
			endDate, err := date.ParseTime(c.String("end-date"))
			if err != nil {
				errorPrinter.Printf("Please give a valid end date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS).\n")
				return cli.Exit("", 1)
			}

			// A single asset path tests just that asset; any directory (including
			// the default ".") tests every pipeline found beneath it, so CI can run
			// a whole repo's unit tests in one call.
			targets, err := collectUnitTestTargets(ctx, inputPath)
			if err != nil {
				printError(err, "", "Failed to resolve the unit tests to run:")
				return cli.Exit("", 1)
			}
			if !anyPipelineHasUnitTests(targets) {
				fmt.Println("No unit tests found.")
				return nil
			}

			parser, err := sqlparser.NewSQLParser(false)
			if err != nil {
				printError(err, "", "Failed to initialize the SQL parser:")
				return cli.Exit("", 1)
			}
			defer parser.Close()
			if err := parser.Start(); err != nil {
				printError(err, "", "Failed to start the SQL parser:")
				return cli.Exit("", 1)
			}

			manager, err := resolveConnectionManager(ctx, inputPath, c.String("environment"))
			if err != nil {
				printError(err, "", "Failed to resolve the asset connections:")
				return cli.Exit("", 1)
			}

			passed, failed := 0, 0
			for _, target := range targets {
				// Macros are per-pipeline, so load them for each pipeline tested.
				macroContent, err := jinja.LoadMacros(fs, target.pipeline.MacrosPath)
				if err != nil {
					printError(err, "", "Failed to load macros:")
					return cli.Exit("", 1)
				}
				schemas := assetColumnSchemas(target.pipeline)
				for _, asset := range target.assets {
					p, f := runAssetUnitTests(ctx, parser, manager, target.pipeline, asset, startDate, endDate, macroContent, schemas)
					passed += p
					failed += f
				}
			}

			fmt.Printf("\n%d passed, %d failed\n", passed, failed)
			if failed > 0 {
				return cli.Exit("", 1)
			}
			return nil
		},
	}
}

// unitTestTarget is a built pipeline and the assets within it to test.
type unitTestTarget struct {
	pipeline *pipeline.Pipeline
	assets   []*pipeline.Asset
}

// collectUnitTestTargets resolves what to test from the given path. A single
// asset path yields just that asset within its pipeline; any directory yields
// every pipeline found beneath it (each with all of its assets), mirroring how
// `bruin validate` fans out over a repo.
func collectUnitTestTargets(ctx context.Context, inputPath string) ([]unitTestTarget, error) {
	if isPathReferencingAsset(inputPath) {
		root, err := path.GetPipelineRootFromTask(inputPath, PipelineDefinitionFiles)
		if err != nil {
			return nil, fmt.Errorf("failed to find the pipeline for %q: %w", inputPath, err)
		}
		pl, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, root, pipeline.WithMutate())
		if err != nil {
			return nil, fmt.Errorf("failed to build the pipeline: %w", err)
		}
		asset := pl.GetAssetByPath(inputPath)
		if asset == nil {
			return nil, fmt.Errorf("could not find an asset at %q within pipeline %q", inputPath, pl.Name)
		}
		return []unitTestTarget{{pipeline: pl, assets: []*pipeline.Asset{asset}}}, nil
	}

	pipelinePaths, err := path.GetPipelinePaths(inputPath, PipelineDefinitionFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to find pipelines under %q: %w", inputPath, err)
	}
	targets := make([]unitTestTarget, 0, len(pipelinePaths))
	for _, pp := range pipelinePaths {
		pl, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pp, pipeline.WithMutate())
		if err != nil {
			return nil, fmt.Errorf("failed to build the pipeline at %q: %w", pp, err)
		}
		targets = append(targets, unitTestTarget{pipeline: pl, assets: pl.Assets})
	}
	return targets, nil
}

func anyPipelineHasUnitTests(targets []unitTestTarget) bool {
	for _, t := range targets {
		if anyUnitTests(t.assets) {
			return true
		}
	}
	return false
}

func anyUnitTests(assets []*pipeline.Asset) bool {
	for _, a := range assets {
		if len(a.UnitTests) > 0 {
			return true
		}
	}
	return false
}

// assetColumnSchemas maps each asset name to its declared columns, so a unit
// test mocking that asset can build a typed fixture from the declaration.
func assetColumnSchemas(pl *pipeline.Pipeline) map[string][]pipeline.Column {
	out := make(map[string][]pipeline.Column, len(pl.Assets))
	for _, a := range pl.Assets {
		if len(a.Columns) > 0 {
			out[a.Name] = a.Columns
		}
	}
	return out
}

// resolveConnectionManager loads the project config and builds the connection
// manager used to resolve a SQL asset's connection.
func resolveConnectionManager(ctx context.Context, pipelinePath, env string) (config.ConnectionAndDetailsGetter, error) {
	repoRoot, err := git.FindRepoFromPath(pipelinePath)
	if err != nil {
		return nil, fmt.Errorf("failed to find the git repository root: %w", err)
	}
	configFilePath := filepath.Join(repoRoot.Path, ".bruin.yml")
	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load the config: %w", err)
	}
	if env != "" {
		if err := cm.SelectEnvironment(env); err != nil {
			return nil, fmt.Errorf("failed to use the environment %q: %w", env, err)
		}
	}
	ctx = context.WithValue(ctx, config.ConfigFilePathContextKey, configFilePath)
	ctx = context.WithValue(ctx, config.EnvironmentNameContextKey, cm.SelectedEnvironmentName)
	manager, errs := connectionManagerFromConfig(ctx, cm, makeLogger(false))
	if len(errs) > 0 {
		return nil, errs[0]
	}
	return manager, nil
}

// runAssetUnitTests runs an asset's unit tests against its configured connection.
// Each test is compiled to a single read-only CTE-injected query (no DDL, no
// artifacts on the target), run via the connection, and compared. Returns
// pass/fail counts.
func runAssetUnitTests(ctx context.Context, parser unittest.Rewriter, manager config.ConnectionAndDetailsGetter, pl *pipeline.Pipeline, asset *pipeline.Asset, startDate, endDate time.Time, macroContent string, schemas map[string][]pipeline.Column) (passed, failed int) {
	if len(asset.UnitTests) == 0 {
		return 0, 0
	}

	dialect, err := sqlparser.AssetTypeToDialect(asset.Type)
	if err != nil {
		errorPrinter.Printf("ERROR  %s — unit tests are only supported for SQL assets (type %q)\n", asset.Name, asset.Type)
		return 0, len(asset.UnitTests)
	}

	connName, err := pl.GetConnectionNameForAsset(asset)
	if err != nil {
		errorPrinter.Printf("ERROR  %s — failed to resolve the connection: %v\n", asset.Name, err)
		return 0, len(asset.UnitTests)
	}
	querier, ok := manager.GetConnection(connName).(schemaQuerier)
	if !ok {
		errorPrinter.Printf("ERROR  %s — connection %q is missing or cannot run queries\n", asset.Name, connName)
		return 0, len(asset.UnitTests)
	}

	for _, test := range asset.UnitTests {
		result, err := evaluateUnitTest(ctx, parser, querier, dialect, pl, asset, test, startDate, endDate, macroContent, schemas)
		if reportUnitTestResult(asset.Name, test.Name, result, err) {
			passed++
		} else {
			failed++
		}
	}
	return passed, failed
}

// evaluateUnitTest runs one unit test against the connection. It asserts the
// asset's final output and the output of each CTE under expected.ctes, each as a
// read-only CTE-injected SELECT (no DDL). It returns a combined result that
// passes only if every assertion holds, or a setup error (resolve/render/build).
func evaluateUnitTest(ctx context.Context, parser unittest.Rewriter, querier schemaQuerier, dialect string, pl *pipeline.Pipeline, asset *pipeline.Asset, test pipeline.UnitTest, startDate, endDate time.Time, macroContent string, schemas map[string][]pipeline.Column) (*unittest.UnitTestResult, error) {
	inputs, err := unittest.ResolveFixtures(pl.Fixtures, test)
	if err != nil {
		return nil, err
	}
	test.Inputs = inputs

	renderedSQL, err := renderAssetSQLForTest(ctx, pl, asset, startDate, endDate, macroContent, test)
	if err != nil {
		return nil, fmt.Errorf("failed to render: %w", err)
	}

	// Build the fixture-injected query once; the final-output check runs it, and
	// each CTE assertion derives `SELECT * FROM <cte>` from it (no re-injection).
	baseSQL, err := unittest.BuildWarehouseQuery(parser, dialect, renderedSQL, test, schemas)
	if err != nil {
		return nil, err
	}

	var failures []string

	// Final output, skipped only when the test asserts CTEs but no final rows/count.
	if assertsFinalOutput(test.Expected) {
		qr, err := querier.SelectWithSchema(ctx, &query.Query{Query: baseSQL})
		if err != nil {
			return nil, err
		}
		if r := unittest.CompareResult(test.Expected, qr.Columns, qr.Rows); !r.Passed {
			failures = append(failures, "final output: "+r.Message)
		}
	}

	// Intermediate CTE assertions, in a stable order.
	for _, name := range sortedCTENames(test.Expected.CTEs) {
		cteSQL, err := parser.SelectFromCTE(baseSQL, dialect, name)
		if err != nil {
			return nil, fmt.Errorf("cte %q: %w", name, err)
		}
		qr, err := querier.SelectWithSchema(ctx, &query.Query{Query: cteSQL})
		if err != nil {
			return nil, fmt.Errorf("cte %q: %w", name, err)
		}
		if r := unittest.CompareCTEResult(test.Expected.CTEs[name], qr.Columns, qr.Rows); !r.Passed {
			failures = append(failures, fmt.Sprintf("cte %q: %s", name, r.Message))
		}
	}

	if len(failures) > 0 {
		return &unittest.UnitTestResult{Message: strings.Join(failures, "\n")}, nil
	}
	return &unittest.UnitTestResult{Passed: true}, nil
}

// assertsFinalOutput reports whether the test pins the asset's final output. A
// test that only lists ctes (no rows/count) needs no final-query run.
func assertsFinalOutput(e pipeline.UnitTestExpected) bool {
	return e.Rows != nil || e.Count != nil || len(e.CTEs) == 0
}

func sortedCTENames(ctes map[string]pipeline.UnitTestCTEExpected) []string {
	names := make([]string, 0, len(ctes))
	for name := range ctes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// reportUnitTestResult prints the PASS/FAIL/ERROR line for one test (with an
// expected-vs-actual diff on failure) and reports whether it passed.
func reportUnitTestResult(assetName, testName string, result *unittest.UnitTestResult, err error) bool {
	switch {
	case err != nil:
		errorPrinter.Printf("ERROR  %s :: %s — %v\n", assetName, testName, err)
		return false
	case result.Passed:
		successPrinter.Printf("PASS   %s :: %s\n", assetName, testName)
		return true
	default:
		errorPrinter.Printf("FAIL   %s :: %s\n", assetName, testName)
		for _, line := range strings.Split(strings.TrimRight(result.Message, "\n"), "\n") {
			fmt.Printf("         %s\n", line)
		}
		return false
	}
}

// renderAssetSQLForTest renders the asset SQL with Jinja (macros + dates and any
// per-test variable overrides), returning the single rendered statement.
func renderAssetSQLForTest(ctx context.Context, pl *pipeline.Pipeline, asset *pipeline.Asset, startDate, endDate time.Time, macroContent string, test pipeline.UnitTest) (string, error) {
	// Test-level variables override the pipeline's for this render only. The
	// renderer reads variables from pl.Variables (via CloneForAsset), so swap in
	// a merged copy and restore the original afterwards rather than mutating the
	// shared pipeline's variables in place.
	if len(test.Variables) > 0 {
		original := pl.Variables
		pl.Variables = mergeVariables(original, test.Variables)
		defer func() { pl.Variables = original }()
	}

	// A test-level execution_time also pins the run's execution date for Jinja,
	// so date macros line up with the frozen SQL clock; otherwise use the default.
	execDate := defaultExecutionDate
	if test.ExecutionTime != "" {
		if t, err := date.ParseTime(test.ExecutionTime); err == nil {
			execDate = t
		}
	}

	runCtx := context.WithValue(ctx, pipeline.RunConfigStartDate, startDate)
	runCtx = context.WithValue(runCtx, pipeline.RunConfigEndDate, endDate)
	runCtx = context.WithValue(runCtx, pipeline.RunConfigExecutionDate, execDate)
	runCtx = context.WithValue(runCtx, pipeline.RunConfigRunID, "unit-test")

	renderer := jinja.NewRendererWithStartEndDatesAndMacros(&startDate, &endDate, &execDate, pl.Name, "unit-test", pl.Variables.Value(), macroContent)
	forAsset, err := renderer.CloneForAsset(runCtx, pl, asset)
	if err != nil {
		return "", err
	}

	extractor := &query.WholeFileExtractor{Fs: fs, Renderer: forAsset}
	queries, err := extractor.ExtractQueriesFromString(asset.ExecutableFile.Content)
	if err != nil {
		return "", err
	}
	if len(queries) != 1 {
		return "", fmt.Errorf("unit tests require a single-statement SQL asset, but %q produced %d statements", asset.Name, len(queries))
	}
	return queries[0].Query, nil
}

// mergeVariables returns a copy of base with the given overrides applied as each
// variable's default value, introducing variables base does not declare, without
// mutating base.
func mergeVariables(base pipeline.Variables, overrides map[string]any) pipeline.Variables {
	merged := make(pipeline.Variables, len(base)+len(overrides))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range overrides {
		entry := map[string]any{}
		if existing, ok := base[k]; ok {
			for ek, ev := range existing {
				entry[ek] = ev
			}
		}
		entry["default"] = v
		merged[k] = entry
	}
	return merged
}
