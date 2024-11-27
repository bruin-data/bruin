package cmd

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	path2 "path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/athena"
	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/databricks"
	"github.com/bruin-data/bruin/pkg/date"
	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/ingestr"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"github.com/bruin-data/bruin/pkg/synapse"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/fatih/color"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
	"github.com/xlab/treeprint"
)

const LogsFolder = "logs"

var (
	yesterday        = time.Now().AddDate(0, 0, -1)
	defaultStartDate = time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
	defaultEndDate   = time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 0, time.UTC)

	startDateFlag = &cli.StringFlag{
		Name:        "start-date",
		Usage:       "the start date of the range the pipeline will run for in YYYY-MM-DD, YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff format",
		DefaultText: "beginning of yesterday, e.g. " + defaultStartDate.Format("2006-01-02 15:04:05.000000"),
		Value:       defaultStartDate.Format("2006-01-02 15:04:05.000000"),
	}
	endDateFlag = &cli.StringFlag{
		Name:        "end-date",
		Usage:       "the end date of the range the pipeline will run for in YYYY-MM-DD, YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff format",
		DefaultText: "end of yesterday, e.g. " + defaultEndDate.Format("2006-01-02 15:04:05") + ".999999",
		Value:       defaultEndDate.Format("2006-01-02 15:04:05") + ".999999",
	}
)

func Run(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "run",
		Usage:     "run a Bruin pipeline",
		ArgsUsage: "[path to the task file]",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "downstream",
				Usage: "pass this flag if you'd like to run all the downstream tasks as well",
			},
			&cli.IntFlag{
				Name:  "workers",
				Usage: "number of workers to run the tasks in parallel",
				Value: 16,
			},
			startDateFlag,
			endDateFlag,
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"e", "env"},
				Usage:   "the environment to use",
			},
			&cli.BoolFlag{
				Name:  "push-metadata",
				Usage: "push the metadata to the destination database if supports, currently supported: BigQuery",
			},
			&cli.BoolFlag{
				Name:    "force",
				Aliases: []string{"f"},
				Usage:   "force the validation even if the environment is a production environment",
			},
			&cli.BoolFlag{
				Name:  "no-log-file",
				Usage: "do not create a log file for this run",
			},
			&cli.BoolFlag{
				Name:    "full-refresh",
				Aliases: []string{"r"},
				Usage:   "truncate the table before running",
			},
			&cli.BoolFlag{
				Name:  "use-uv",
				Usage: "use uv for managing Python dependencies",
			},
			&cli.StringFlag{
				Name:    "tag",
				Aliases: []string{"t"},
				Usage:   "pick the assets with the given tag",
			},
			&cli.StringSliceFlag{
				Name:        "only",
				DefaultText: "'main', 'checks', 'push-metadata'",
				Usage:       "limit the types of tasks to run. By default it will run main and checks, while push-metadata is optional if defined in the pipeline definition",
			},
			&cli.BoolFlag{
				Name:  "exp-use-winget-for-uv",
				Usage: "use powershell to manage and install uv on windows, on non-windows systems this has no effect.",
			},
		},
		Action: func(c *cli.Context) error {
			defer func() {
				if err := recover(); err != nil {
					log.Println("=======================================")
					log.Println("Bruin encountered an unexpected error, please report the issue to the Bruin team.")
					log.Println(err)
					log.Println("=======================================")
				}
			}()

			logger := makeLogger(*isDebug)
			inputPath := c.Args().Get(0)
			if inputPath == "" {
				errorPrinter.Printf("Please give a task or pipeline path: bruin run <path to the task definition>)\n")
				return cli.Exit("", 1)
			}

			startDate, err := date.ParseTime(c.String("start-date"))
			logger.Debug("given start date: ", startDate)
			if err != nil {
				errorPrinter.Printf("Please give a valid start date: bruin run --start-date <start date>)\n")
				errorPrinter.Printf("A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats. \n")
				errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
				errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
				return cli.Exit("", 1)
			}

			endDate, err := date.ParseTime(c.String("end-date"))
			logger.Debug("given end date: ", endDate)
			if err != nil {
				errorPrinter.Printf("Please give a valid end date: bruin run --start-date <start date>)\n")
				errorPrinter.Printf("A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats. \n")
				errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
				errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
				return cli.Exit("", 1)
			}

			pipelinePath := inputPath
			repoRoot, err := git.FindRepoFromPath(inputPath)
			if err != nil {
				errorPrinter.Printf("Failed to find the git repository root: %v\n", err)
				return cli.Exit("", 1)
			}

			runningForAnAsset := isPathReferencingAsset(inputPath)
			if runningForAnAsset && c.String("tag") != "" {
				errorPrinter.Printf("You cannot use the '--tag' flag when running a single asset.\n")
				return cli.Exit("", 1)
			}

			var task *pipeline.Asset
			runDownstreamTasks := false
			if runningForAnAsset {
				task, err = DefaultPipelineBuilder.CreateAssetFromFile(inputPath)
				if err != nil {
					errorPrinter.Printf("Failed to build asset: %v. Are you sure you used the correct path?\n", err.Error())

					return cli.Exit("", 1)
				}
				if task == nil {
					errorPrinter.Printf("The given file path doesn't seem to be a Bruin task definition: '%s'\n", inputPath)
					return cli.Exit("", 1)
				}

				pipelinePath, err = path.GetPipelineRootFromTask(inputPath, pipelineDefinitionFile)
				if err != nil {
					errorPrinter.Printf("Failed to find the pipeline this task belongs to: '%s'\n", inputPath)
					return cli.Exit("", 1)
				}

				if c.Bool("downstream") {
					infoPrinter.Println("The downstream tasks will be executed as well.")
					runDownstreamTasks = true
				}
			}

			if !runningForAnAsset && c.Bool("downstream") {
				infoPrinter.Println("Ignoring the '--downstream' flag since you are running the whole pipeline")
			}

			configFilePath := path2.Join(repoRoot.Path, ".bruin.yml")
			cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
			if err != nil {
				errorPrinter.Printf("Failed to load the config file at '%s': %v\n", configFilePath, err)
				return cli.Exit("", 1)
			}

			logger.Debugf("loaded the config from path '%s'", configFilePath)

			err = switchEnvironment(c, cm, os.Stdin)
			if err != nil {
				return err
			}

			connectionManager, errs := connection.NewManagerFromConfig(cm)
			if len(errs) > 0 {
				printErrors(errs, c.String("output"), "Failed to register connections")
				return cli.Exit("", 1)
			}

			foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(pipelinePath)
			if err != nil {
				errorPrinter.Println("failed to build pipeline, are you sure you have referred the right path?")
				errorPrinter.Println("\nHint: You need to run this command with a path to either the pipeline directory or the asset file itself directly.")

				return cli.Exit("", 1)
			}

			// handle log files
			runID := time.Now().Format("2006_01_02_15_04_05")
			if !c.Bool("no-log-file") {
				logFileName := fmt.Sprintf("%s__%s", runID, foundPipeline.Name)
				if runningForAnAsset {
					logFileName = fmt.Sprintf("%s__%s__%s", runID, foundPipeline.Name, task.Name)
				}

				logPath, err := filepath.Abs(fmt.Sprintf("%s/%s/%s.log", repoRoot.Path, LogsFolder, logFileName))
				if err != nil {
					errorPrinter.Printf("Failed to create log file: %v\n", err)
					return cli.Exit("", 1)
				}

				fn, err2 := logOutput(logPath)
				if err2 != nil {
					errorPrinter.Printf("Failed to create log file: %v\n", err2)
					return cli.Exit("", 1)
				}

				defer fn()
				color.Output = os.Stdout

				err = git.EnsureGivenPatternIsInGitignore(afero.NewOsFs(), repoRoot.Path, LogsFolder+"/*.log")
				if err != nil {
					errorPrinter.Printf("Failed to add the log file to .gitignore: %v\n", err)
					return cli.Exit("", 1)
				}
			}
			infoPrinter.Printf("Analyzed the pipeline '%s' with %d assets.\n", foundPipeline.Name, len(foundPipeline.Assets))

			if runningForAnAsset {
				infoPrinter.Printf("Running only the asset '%s'\n", task.Name)
			}

			rules, err := lint.GetRules(fs, &git.RepoFinder{}, true)
			if err != nil {
				errorPrinter.Printf("An error occurred while linting the pipelines: %v\n", err)
				return cli.Exit("", 1)
			}

			rules = lint.FilterRulesBySpeed(rules, true)

			linter := lint.NewLinter(path.GetPipelinePaths, DefaultPipelineBuilder, rules, logger)
			res, err := linter.LintPipelines([]*pipeline.Pipeline{foundPipeline})
			err = reportLintErrors(res, err, lint.Printer{RootCheckPath: pipelinePath}, "")
			if err != nil {
				return cli.Exit("", 1)
			}

			runMain := true
			runChecks := true
			runPushMetadata := c.Bool("push-metadata") || foundPipeline.MetadataPush.HasAnyEnabled()

			onlyFlags := c.StringSlice("only")
			if len(onlyFlags) > 0 {
				runMain = slices.Contains(onlyFlags, "main")
				runChecks = slices.Contains(onlyFlags, "checks")
				runPushMetadata = slices.Contains(onlyFlags, "push-metadata")

				for _, flag := range onlyFlags {
					if flag != "main" && flag != "checks" && flag != "push-metadata" {
						errorPrinter.Printf("Invalid value for '--only' flag: '%s', available values are 'main', 'checks', and 'push-metadata'\n", flag)
						return cli.Exit("", 1)
					}
				}
			}

			s := scheduler.NewScheduler(logger, foundPipeline)

			// mark all the instances to be skipped, then conditionally mark the ones to run to be pending
			s.MarkAll(scheduler.Pending)
			if task != nil {
				logger.Debug("marking single task to run: ", task.Name)
				s.MarkAll(scheduler.Succeeded)
				s.MarkAsset(task, scheduler.Pending, runDownstreamTasks)

				if c.String("tag") != "" {
					errorPrinter.Printf("You cannot use the '--tag' flag when running a single asset.\n")
					return cli.Exit("", 1)
				}
			}

			sendTelemetry(s, c)

			tag := c.String("tag")
			if tag != "" {
				assetsByTag := foundPipeline.GetAssetsByTag(tag)
				if len(assetsByTag) == 0 {
					errorPrinter.Printf("No assets found with the tag '%s'\n", tag)
					return cli.Exit("", 1)
				}

				logger.Debugf("marking assets with tag '%s' to run", tag)
				s.MarkAll(scheduler.Succeeded)
				s.MarkByTag(tag, scheduler.Pending, runDownstreamTasks)

				infoPrinter.Printf("Running only the assets with tag '%s', found %d assets.\n", tag, len(assetsByTag))
			}

			if !runMain {
				logger.Debug("disabling main instances if any")
				s.MarkPendingInstancesByType(scheduler.TaskInstanceTypeMain, scheduler.Succeeded)
			}
			if !runChecks {
				logger.Debug("disabling check instances if any")
				s.MarkPendingInstancesByType(scheduler.TaskInstanceTypeColumnCheck, scheduler.Succeeded)
				s.MarkPendingInstancesByType(scheduler.TaskInstanceTypeCustomCheck, scheduler.Succeeded)
			}
			if !runPushMetadata {
				logger.Debug("disabling metadata push instances if any")
				s.MarkPendingInstancesByType(scheduler.TaskInstanceTypeMetadataPush, scheduler.Succeeded)
			}

			if s.InstanceCountByStatus(scheduler.Pending) == 0 {
				warningPrinter.Println("No tasks to run.")
				return nil
			}

			infoPrinter.Printf("\nStarting the pipeline execution...\n")
			infoPrinter.Println()

			mainExecutors, err := setupExecutors(s, cm, connectionManager, startDate, endDate, foundPipeline.Name, runID, c.Bool("full-refresh"), c.Bool("use-uv"))
			if err != nil {
				errorPrinter.Println(err.Error())
				return cli.Exit("", 1)
			}

			ex, err := executor.NewConcurrent(logger, mainExecutors, c.Int("workers"))
			if err != nil {
				errorPrinter.Printf("Failed to create executor: %v\n", err)
				return cli.Exit("", 1)
			}

			runCtx := context.Background()
			runCtx = context.WithValue(runCtx, pipeline.RunConfigFullRefresh, c.Bool("full-refresh"))
			runCtx = context.WithValue(runCtx, pipeline.RunConfigStartDate, startDate)
			runCtx = context.WithValue(runCtx, pipeline.RunConfigEndDate, endDate)
			runCtx = context.WithValue(runCtx, executor.KeyIsDebug, isDebug)
			runCtx = context.WithValue(runCtx, python.CtxUseWingetForUv, c.Bool("exp-use-winget-for-uv")) //nolint:staticcheck

			ex.Start(runCtx, s.WorkQueue, s.Results)

			start := time.Now()
			results := s.Run(runCtx)
			duration := time.Since(start)

			successPrinter.Printf("\n\nExecuted %d tasks in %s\n", len(results), duration.Truncate(time.Millisecond).String())
			errorsInTaskResults := make([]*scheduler.TaskExecutionResult, 0)
			for _, res := range results {
				if res.Error != nil {
					errorsInTaskResults = append(errorsInTaskResults, res)
				}
			}

			if len(errorsInTaskResults) > 0 {
				printErrorsInResults(errorsInTaskResults, s)
				return cli.Exit("", 1)
			}

			return nil
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

func printErrorsInResults(errorsInTaskResults []*scheduler.TaskExecutionResult, s *scheduler.Scheduler) {
	data := make(map[string][]*scheduler.TaskExecutionResult, len(errorsInTaskResults))
	for _, result := range errorsInTaskResults {
		assetName := result.Instance.GetAsset().Name
		data[assetName] = append(data[assetName], result)
	}

	tree := treeprint.New()
	for assetName, results := range data {
		assetBranch := tree.AddBranch(assetName)

		columnBranches := make(map[string]treeprint.Tree, len(results))

		for _, result := range results {
			switch instance := result.Instance.(type) {
			case *scheduler.ColumnCheckInstance:
				colBranch, exists := columnBranches[instance.Column.Name]
				if !exists {
					colBranch = assetBranch.AddBranch("[Column] " + instance.Column.Name)
					columnBranches[instance.Column.Name] = colBranch
				}

				checkBranch := colBranch.AddBranch("[Check] " + instance.Check.Name)
				checkBranch.AddNode(fmt.Sprintf("'%s'", result.Error))

			case *scheduler.CustomCheckInstance:
				customBranch := assetBranch.AddBranch("[Custom Check] " + instance.Check.Name)
				customBranch.AddNode(fmt.Sprintf("'%s'", result.Error))

			default:
				assetBranch.AddNode(fmt.Sprintf("'%s'", result.Error))
			}
		}
	}
	errorPrinter.Println(fmt.Sprintf("Failed assets %d", len(data)))
	errorPrinter.Println(tree.String())
	upstreamFailedTasks := s.GetTaskInstancesByStatus(scheduler.UpstreamFailed)
	if len(upstreamFailedTasks) > 0 {
		errorPrinter.Printf("The following tasks are skipped due to their upstream failing:\n")

		skippedAssets := make(map[string]int, 0)
		for _, t := range upstreamFailedTasks {
			if _, ok := skippedAssets[t.GetAsset().Name]; !ok {
				skippedAssets[t.GetAsset().Name] = 0
			}

			if t.GetType() == scheduler.TaskInstanceTypeMain {
				continue
			}

			skippedAssets[t.GetAsset().Name] += 1
		}

		for asset, checkCount := range skippedAssets {
			if checkCount == 0 {
				errorPrinter.Printf("  - %s\n", asset)
			} else {
				errorPrinter.Printf("  - %s %s\n", asset, faint(fmt.Sprintf("(and %d checks)", checkCount)))
			}
		}
	}
}

func setupExecutors(
	s *scheduler.Scheduler,
	config *config.Config,
	conn *connection.Manager,
	startDate,
	endDate time.Time,
	pipelineName string,
	runID string,
	fullRefresh bool,
	useUvForPython bool,
) (map[pipeline.AssetType]executor.Config, error) {
	mainExecutors := executor.DefaultExecutorsV2

	// this is a heuristic we apply to find what might be the most common type of custom check in the pipeline
	// this should go away once we incorporate URIs into the assets
	estimateCustomCheckType := s.FindMajorityOfTypes(pipeline.AssetTypeBigqueryQuery)

	if s.WillRunTaskOfType(pipeline.AssetTypePython) {
		jinjaVariables := jinja.PythonEnvVariables(&startDate, &endDate, pipelineName, runID, fullRefresh)
		if useUvForPython {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeMain] = python.NewLocalOperatorWithUv(config, conn, jinjaVariables)
		} else {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeMain] = python.NewLocalOperator(config, jinjaVariables)
		}
	}

	renderer := jinja.NewRendererWithStartEndDates(&startDate, &endDate, pipelineName, runID)
	wholeFileExtractor := &query.WholeFileExtractor{
		Fs:       fs,
		Renderer: renderer,
	}

	customCheckRunner := ansisql.NewCustomCheckOperator(conn, renderer)

	if s.WillRunTaskOfType(pipeline.AssetTypeBigqueryQuery) || estimateCustomCheckType == pipeline.AssetTypeBigqueryQuery {
		bqOperator := bigquery.NewBasicOperator(conn, wholeFileExtractor, bigquery.NewMaterializer(fullRefresh))

		bqCheckRunner, err := bigquery.NewColumnCheckOperator(conn)
		if err != nil {
			return nil, err
		}

		metadataPushOperator := bigquery.NewMetadataPushOperator(conn)

		mainExecutors[pipeline.AssetTypeBigqueryQuery][scheduler.TaskInstanceTypeMain] = bqOperator
		mainExecutors[pipeline.AssetTypeBigqueryQuery][scheduler.TaskInstanceTypeColumnCheck] = bqCheckRunner
		mainExecutors[pipeline.AssetTypeBigqueryQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		mainExecutors[pipeline.AssetTypeBigqueryQuery][scheduler.TaskInstanceTypeMetadataPush] = metadataPushOperator

		// we set the Python runners to run the checks on BigQuery assuming that there won't be many usecases where a user has both BQ and Snowflake
		if estimateCustomCheckType == pipeline.AssetTypeBigqueryQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = bqCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeMetadataPush] = metadataPushOperator
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypePostgresQuery) || estimateCustomCheckType == pipeline.AssetTypePostgresQuery ||
		s.WillRunTaskOfType(pipeline.AssetTypeRedshiftQuery) || estimateCustomCheckType == pipeline.AssetTypeRedshiftQuery {
		pgCheckRunner := postgres.NewColumnCheckOperator(conn)
		pgOperator := postgres.NewBasicOperator(conn, wholeFileExtractor, postgres.NewMaterializer(fullRefresh))

		mainExecutors[pipeline.AssetTypeRedshiftQuery][scheduler.TaskInstanceTypeMain] = pgOperator
		mainExecutors[pipeline.AssetTypeRedshiftQuery][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypeRedshiftQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypePostgresQuery][scheduler.TaskInstanceTypeMain] = pgOperator
		mainExecutors[pipeline.AssetTypePostgresQuery][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypePostgresQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		// we set the Python runners to run the checks on Snowflake assuming that there won't be many usecases where a user has both BQ and Snowflake
		if estimateCustomCheckType == pipeline.AssetTypePostgresQuery || estimateCustomCheckType == pipeline.AssetTypeRedshiftQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	shouldInitiateSnowflake := s.WillRunTaskOfType(pipeline.AssetTypeSnowflakeQuery) || s.WillRunTaskOfType(pipeline.AssetTypeSnowflakeQuerySensor) || estimateCustomCheckType == pipeline.AssetTypeSnowflakeQuery
	if shouldInitiateSnowflake {
		sfOperator := snowflake.NewBasicOperator(conn, wholeFileExtractor, snowflake.NewMaterializer(fullRefresh))

		sfCheckRunner := snowflake.NewColumnCheckOperator(conn)

		sfQuerySensor := snowflake.NewQuerySensor(conn, renderer, 30)

		mainExecutors[pipeline.AssetTypeSnowflakeQuery][scheduler.TaskInstanceTypeMain] = sfOperator
		mainExecutors[pipeline.AssetTypeSnowflakeQuery][scheduler.TaskInstanceTypeColumnCheck] = sfCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeQuerySensor][scheduler.TaskInstanceTypeMain] = sfQuerySensor
		mainExecutors[pipeline.AssetTypeSnowflakeQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = sfCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		// we set the Python runners to run the checks on Snowflake assuming that there won't be many usecases where a user has both BQ and Snowflake
		if estimateCustomCheckType == pipeline.AssetTypeSnowflakeQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = sfCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeMsSQLQuery) || estimateCustomCheckType == pipeline.AssetTypeMsSQLQuery ||
		s.WillRunTaskOfType(pipeline.AssetTypeSynapseQuery) || estimateCustomCheckType == pipeline.AssetTypeSynapseQuery {
		msOperator := mssql.NewBasicOperator(conn, wholeFileExtractor, mssql.NewMaterializer(fullRefresh))
		synapseOperator := synapse.NewBasicOperator(conn, wholeFileExtractor, synapse.NewMaterializer(fullRefresh))

		msCheckRunner := mssql.NewColumnCheckOperator(conn)
		synapseCheckRunner := synapse.NewColumnCheckOperator(conn)

		mainExecutors[pipeline.AssetTypeMsSQLQuery][scheduler.TaskInstanceTypeMain] = msOperator
		mainExecutors[pipeline.AssetTypeMsSQLQuery][scheduler.TaskInstanceTypeColumnCheck] = msCheckRunner
		mainExecutors[pipeline.AssetTypeMsSQLQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeSynapseQuery][scheduler.TaskInstanceTypeMain] = synapseOperator
		mainExecutors[pipeline.AssetTypeSynapseQuery][scheduler.TaskInstanceTypeColumnCheck] = synapseCheckRunner
		mainExecutors[pipeline.AssetTypeSynapseQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		// we set the Python runners to run the checks on MsSQL
		if estimateCustomCheckType == pipeline.AssetTypeMsSQLQuery || estimateCustomCheckType == pipeline.AssetTypeSynapseQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = msCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeDatabricksQuery) || estimateCustomCheckType == pipeline.AssetTypeDatabricksQuery {
		databricksOperator := databricks.NewBasicOperator(conn, wholeFileExtractor, databricks.NewMaterializer(fullRefresh))
		databricksCheckRunner := databricks.NewColumnCheckOperator(conn)

		mainExecutors[pipeline.AssetTypeDatabricksQuery][scheduler.TaskInstanceTypeMain] = databricksOperator
		mainExecutors[pipeline.AssetTypeDatabricksQuery][scheduler.TaskInstanceTypeColumnCheck] = databricksCheckRunner
		mainExecutors[pipeline.AssetTypeDatabricksQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		// we set the Python runners to run the checks on MsSQL
		if estimateCustomCheckType == pipeline.AssetTypeDatabricksQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = databricksOperator
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeIngestr) || estimateCustomCheckType == pipeline.AssetTypeIngestr {
		ingestrOperator, err := ingestr.NewBasicOperator(conn)
		if err != nil {
			return nil, err
		}
		ingestrCheckRunner := ingestr.NewColumnCheckOperator(&mainExecutors)
		ingestrCustomCheckRunner := ingestr.NewCustomCheckOperator(&mainExecutors)

		mainExecutors[pipeline.AssetTypeIngestr][scheduler.TaskInstanceTypeMain] = ingestrOperator
		mainExecutors[pipeline.AssetTypeIngestr][scheduler.TaskInstanceTypeColumnCheck] = ingestrCheckRunner
		mainExecutors[pipeline.AssetTypeIngestr][scheduler.TaskInstanceTypeCustomCheck] = ingestrCustomCheckRunner
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeAthenaQuery) || estimateCustomCheckType == pipeline.AssetTypeAthenaQuery {
		athenaOperator := athena.NewBasicOperator(conn, wholeFileExtractor, athena.NewMaterializer(fullRefresh))
		athenaCustomCheckRunner := ansisql.NewCustomCheckOperator(conn, renderer)
		athenaCheckRunner := athena.NewColumnCheckOperator(conn)

		mainExecutors[pipeline.AssetTypeAthenaQuery][scheduler.TaskInstanceTypeMain] = athenaOperator
		mainExecutors[pipeline.AssetTypeAthenaQuery][scheduler.TaskInstanceTypeColumnCheck] = athenaCheckRunner
		mainExecutors[pipeline.AssetTypeAthenaQuery][scheduler.TaskInstanceTypeCustomCheck] = athenaCustomCheckRunner
		if estimateCustomCheckType == pipeline.AssetTypeAthenaQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = athenaCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = athenaCustomCheckRunner
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeDuckDBQuery) || estimateCustomCheckType == pipeline.AssetTypeDuckDBQuery {
		duckDBOperator := duck.NewBasicOperator(conn, wholeFileExtractor, duck.NewMaterializer(fullRefresh))
		duckDBCustomCheckRunner := ansisql.NewCustomCheckOperator(conn, renderer)
		duckDBCheckRunner := duck.NewColumnCheckOperator(conn)

		mainExecutors[pipeline.AssetTypeDuckDBQuery][scheduler.TaskInstanceTypeMain] = duckDBOperator
		mainExecutors[pipeline.AssetTypeDuckDBQuery][scheduler.TaskInstanceTypeColumnCheck] = duckDBCheckRunner
		mainExecutors[pipeline.AssetTypeDuckDBQuery][scheduler.TaskInstanceTypeCustomCheck] = duckDBCustomCheckRunner
		if estimateCustomCheckType == pipeline.AssetTypeDuckDBQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = duckDBCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = duckDBCustomCheckRunner
		}
	}

	return mainExecutors, nil
}

func isPathReferencingAsset(p string) bool {
	if strings.HasSuffix(p, pipelineDefinitionFile) {
		return false
	}

	if isDir(p) {
		return false
	}

	return true
}

func isDir(path string) bool {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false
	}

	return fileInfo.IsDir()
}

type clearFileWriter struct {
	file *os.File
	m    sync.Mutex
}

func (c *clearFileWriter) Write(p []byte) (int, error) {
	c.m.Lock()
	defer c.m.Unlock()
	_, err := c.file.Write([]byte(Clean(string(p))))
	return len(p), err
}

func logOutput(logPath string) (func(), error) {
	err := os.MkdirAll(filepath.Dir(logPath), 0o755)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create log directory")
	}

	// open file read/write | create if not exist | clear file at open if exists
	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open log file")
	}

	// save existing stdout | MultiWriter writes to saved stdout and file
	mw := io.MultiWriter(os.Stdout, &clearFileWriter{f, sync.Mutex{}})

	// get pipe reader and writer | writes to pipe writer come out pipe reader
	r, w, err := os.Pipe()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create log pipe")
	}

	// replace stdout,stderr with pipe writer | all writes to stdout, stderr will go through pipe instead (fmt.print, log)
	os.Stdout = w
	os.Stderr = w

	// writes with log.Print should also write to mw
	log.SetOutput(mw)

	// create channel to control exit | will block until all copies are finished
	exit := make(chan bool)

	go func() {
		// copy all reads from pipe to multiwriter, which writes to stdout and file
		_, err := io.Copy(mw, r)
		if err != nil {
			panic(err)
		}
		// when r or w is closed copy will finish and true will be sent to channel
		exit <- true
	}()

	// function to be deferred in main until program exits
	return func() {
		// close writer then block on exit channel | this will let mw finish writing before the program exits
		_ = w.Close()
		<-exit
		// close file after all writes have finished
		_ = f.Close()
	}, nil
}

const ansi = "[\u001B\u009B][[\\]()#;?]*(?:(?:(?:[a-zA-Z\\d]*(?:;[a-zA-Z\\d]*)*)?\u0007)|(?:(?:\\d{1,4}(?:;\\d{0,4})*)?[\\dA-PRZcf-ntqry=><~]))"

var re = regexp.MustCompile(ansi)

func Clean(str string) string {
	return re.ReplaceAllString(str, "")
}

func sendTelemetry(s *scheduler.Scheduler, c *cli.Context) {
	assetStats := make(map[string]int)
	for _, asset := range s.GetTaskInstancesByStatus(scheduler.Pending) {
		_, ok := assetStats[string(asset.GetAsset().Type)]
		if !ok {
			assetStats[string(asset.GetAsset().Type)] = 0
		}
		assetStats[string(asset.GetAsset().Type)]++
	}

	telemetry.SendEventWithAssetStats("run_assets", assetStats, c)
}
