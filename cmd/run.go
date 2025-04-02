package cmd

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	path2 "path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/athena"
	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/clickhouse"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/databricks"
	"github.com/bruin-data/bruin/pkg/date"
	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/emr_serverless"
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
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/bruin-data/bruin/pkg/synapse"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/fatih/color"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
	"github.com/xlab/treeprint"
	"go.uber.org/zap"
)

const LogsFolder = "logs"

type PipelineInfo struct {
	Pipeline           *pipeline.Pipeline
	RunningForAnAsset  bool
	RunDownstreamTasks bool
}

var (
	yesterday        = time.Now().AddDate(0, 0, -1)
	defaultStartDate = time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
	defaultEndDate   = time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 0, time.UTC)

	startDateFlag = &cli.StringFlag{
		Name:        "start-date",
		Usage:       "the start date of the range the pipeline will run for in YYYY-MM-DD, YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff format",
		DefaultText: "beginning of yesterday, e.g. " + defaultStartDate.Format("2006-01-02 15:04:05.000000"),
		Value:       defaultStartDate.Format("2006-01-02 15:04:05.000000"),
		EnvVars:     []string{"BRUIN_START_DATE"},
	}
	endDateFlag = &cli.StringFlag{
		Name:        "end-date",
		Usage:       "the end date of the range the pipeline will run for in YYYY-MM-DD, YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff format",
		DefaultText: "end of yesterday, e.g. " + defaultEndDate.Format("2006-01-02 15:04:05") + ".999999",
		Value:       defaultEndDate.Format("2006-01-02 15:04:05") + ".999999",
		EnvVars:     []string{"BRUIN_END_DATE"},
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
			&cli.StringFlag{
				Name:        "sensor-mode",
				DefaultText: "'once' (default), 'skip', 'wait'",
				Usage:       "Set sensor mode: 'skip' to bypass, 'once' to run once, or 'wait' to loop until expected result",
			},
			&cli.BoolFlag{
				Name:    "full-refresh",
				Aliases: []string{"r"},
				Usage:   "truncate the table before running",
				EnvVars: []string{"BRUIN_FULL_REFRESH"},
			},
			&cli.BoolFlag{
				Name:  "use-pip",
				Usage: "use pip for managing Python dependencies",
			},
			&cli.BoolFlag{
				Name:  "continue",
				Usage: "use continue to run the pipeline from the last failed asset",
			},
			&cli.StringFlag{
				Name:    "tag",
				Aliases: []string{"t"},
				Usage:   "pick the assets with the given tag",
			},
			&cli.StringFlag{
				Name:    "exclude-tag",
				Aliases: []string{"x"},
				Usage:   "exclude the assets with given tag",
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
			&cli.StringFlag{
				Name:  "debug-ingestr-src",
				Usage: "Use ingestr from the given path instead of the builtin version.",
			},
			&cli.StringFlag{
				Name:    "config-file",
				EnvVars: []string{"BRUIN_CONFIG_FILE"},
				Usage:   "the path to the .bruin.yml file",
			},
			&cli.BoolFlag{
				Name:  "no-validation",
				Usage: "skip validation for this run.",
			},
			&cli.BoolFlag{
				Name:  "no-timestamp",
				Usage: "skip logging timestamps for this run.",
			},
			&cli.BoolFlag{
				Name:  "no-color",
				Usage: "plain log output for this run.",
			},
			&cli.BoolFlag{
				Name:   "minimal-logs",
				Usage:  "skip initial pipeline analysis logs for this run",
				Hidden: true,
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
			// Initialize runConfig with values from cli.Context
			runConfig := &scheduler.RunConfig{
				Downstream:        c.Bool("downstream"),
				StartDate:         c.String("start-date"),
				EndDate:           c.String("end-date"),
				Workers:           c.Int("workers"),
				Environment:       c.String("environment"),
				Force:             c.Bool("force"),
				PushMetadata:      c.Bool("push-metadata"),
				NoLogFile:         c.Bool("no-log-file"),
				FullRefresh:       c.Bool("full-refresh"),
				UsePip:            c.Bool("use-pip"),
				Tag:               c.String("tag"),
				ExcludeTag:        c.String("exclude-tag"),
				Only:              c.StringSlice("only"),
				Output:            c.String("output"),
				ExpUseWingetForUv: c.Bool("exp-use-winget-for-uv"),
				ConfigFilePath:    c.String("config-file"),
				SensorMode:        c.String("sensor-mode"),
			}

			var startDate, endDate time.Time

			var err error
			startDate, endDate, inputPath, err := ValidateRunConfig(runConfig, c.Args().Get(0), logger)
			if err != nil {
				return err
			}

			repoRoot, err := git.FindRepoFromPath(inputPath)
			if err != nil {
				errorPrinter.Printf("Failed to find the git repository root: %v\n", err)
				return cli.Exit("", 1)
			}

			configFilePath := runConfig.ConfigFilePath
			if configFilePath == "" {
				configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
			}
			cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
			if err != nil {
				errorPrinter.Printf("Failed to load the config file at '%s': %v\n", configFilePath, err)
				return cli.Exit("", 1)
			}

			err = switchEnvironment(runConfig.Environment, runConfig.Force, cm, os.Stdin)
			if err != nil {
				return err
			}

			if cm.SelectedEnvironment.SchemaPrefix != "" {
				// schema prefix implies a developer environment being configured where different assets within this
				// execution will be built under prefixed schemas. This requires not just modifying the queries,
				// but also modifying the asset names so that quality checks actually run against the tables in the new schema.
				// Since we change the asset names, we need to also prefix the upstream since their names would be changed as well.
				DefaultPipelineBuilder.AddMutator(func(ctx context.Context, asset *pipeline.Asset, foundPipeline *pipeline.Pipeline) (*pipeline.Asset, error) {
					asset.PrefixSchema(cm.SelectedEnvironment.SchemaPrefix)
					asset.PrefixUpstreams(cm.SelectedEnvironment.SchemaPrefix)
					return asset, nil
				})
			}

			pipelineInfo, err := GetPipeline(c.Context, inputPath, runConfig, logger)
			if err != nil {
				return err
			}

			var task *pipeline.Asset
			if pipelineInfo.RunningForAnAsset {
				task, err = DefaultPipelineBuilder.CreateAssetFromFile(inputPath, pipelineInfo.Pipeline)
				if err != nil {
					errorPrinter.Printf("Failed to build asset: %v\n", err)
					return cli.Exit("", 1)
				}
				task, err = DefaultPipelineBuilder.MutateAsset(c.Context, task, nil)
				if err != nil {
					errorPrinter.Printf("Failed to mutate asset: %v\n", err)
					return cli.Exit("", 1)
				}
			}

			// handle log files
			runID := time.Now().Format("2006_01_02_15_04_05")
			if os.Getenv("BRUIN_RUN_ID") != "" {
				runID = os.Getenv("BRUIN_RUN_ID")
			}
			executionStartLog := "Starting execution..."
			if !c.Bool("minimal-logs") {
				infoPrinter.Printf("Analyzed the pipeline '%s' with %d assets.\n", pipelineInfo.Pipeline.Name, len(pipelineInfo.Pipeline.Assets))

				if pipelineInfo.RunningForAnAsset {
					infoPrinter.Printf("Running only the asset '%s'\n", task.Name)
				}
				executionStartLog = "Starting the pipeline execution..."
			}

			shouldValidate := !pipelineInfo.RunningForAnAsset && !c.Bool("no-validation")
			if shouldValidate {
				if err := CheckLint(pipelineInfo.Pipeline, inputPath, logger, nil); err != nil {
					return err
				}
			}

			statePath := filepath.Join(repoRoot.Path, "logs/runs", pipelineInfo.Pipeline.Name)
			err = git.EnsureGivenPatternIsInGitignore(afero.NewOsFs(), repoRoot.Path, "logs/runs")
			if err != nil {
				errorPrinter.Printf("Failed to add the run state folder to .gitignore: %v\n", err)
				return cli.Exit("", 1)
			}

			filter := &Filter{
				IncludeTag:        runConfig.Tag,
				OnlyTaskTypes:     runConfig.Only,
				IncludeDownstream: pipelineInfo.RunDownstreamTasks,
				PushMetaData:      runConfig.PushMetadata,
				SingleTask:        task,
				ExcludeTag:        runConfig.ExcludeTag,
			}
			var pipelineState *scheduler.PipelineState
			if c.Bool("continue") {
				pipelineState, err = ReadState(afero.NewOsFs(), statePath, filter)
				if err != nil {
					errorPrinter.Printf("Failed to restore state: %v\n", err)
					return err
				}

				runConfig = &pipelineState.Parameters

				parsedStartDate, parsedEndDate, err := ParseDate(runConfig.StartDate, runConfig.EndDate, logger)
				if err != nil {
					return cli.Exit("", 1)
				}
				startDate = parsedStartDate
				endDate = parsedEndDate
			}

			foundPipeline := pipelineInfo.Pipeline

			if runConfig.Downstream {
				infoPrinter.Println("The downstream tasks will be executed as well.")
				pipelineInfo.RunDownstreamTasks = true
			}

			if !runConfig.NoLogFile {
				logFileName := fmt.Sprintf("%s__%s", runID, foundPipeline.Name)
				if pipelineInfo.RunningForAnAsset {
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

			if filter.PushMetaData {
				foundPipeline.MetadataPush.Global = true
			}

			connectionManager, errs := connection.NewManagerFromConfig(cm)
			if len(errs) > 0 {
				printErrors(errs, runConfig.Output, "Failed to register connections")
				return cli.Exit("", 1)
			}

			s := scheduler.NewScheduler(logger, foundPipeline, runID)

			if c.Bool("continue") {
				if err := s.RestoreState(pipelineState); err != nil {
					errorPrinter.Printf("Failed to restore state: %v\n", err)
					return cli.Exit("", 1)
				}
			}

			if !c.Bool("continue") {
				// Apply the filter to mark assets based on include/exclude tags
				if err := ApplyAllFilters(context.Background(), filter, s, foundPipeline); err != nil {
					errorPrinter.Printf("Failed to filter assets: %v\n", err)
					return cli.Exit("", 1)
				}
			}

			if s.InstanceCountByStatus(scheduler.Pending) == 0 {
				warningPrinter.Println("No tasks to run.")
				return nil
			}
			sendTelemetry(s, c)
			infoPrinter.Printf("\n%s\n", executionStartLog)
			infoPrinter.Println()
			if runConfig.SensorMode != "" {
				if !(runConfig.SensorMode == "skip" || runConfig.SensorMode == "once" || runConfig.SensorMode == "wait") {
					errorPrinter.Printf("invalid value for '--mode' flag: '%s', valid options are --skip ,--once, --wait", runConfig.SensorMode)
					return cli.Exit("", 1)
				}
			}

			var parser *sqlparser.SQLParser
			if cm.SelectedEnvironment.SchemaPrefix != "" {
				// we use the sql parser to rename the tables for dev mode
				parser, err = sqlparser.NewSQLParser(false)
				if err != nil {
					printError(err, c.String("output"), "Could not initialize sql parser")
				}

				go func() {
					err := parser.Start()
					if err != nil {
						printError(err, c.String("output"), "Could not start sql parser")
					}
				}()
			}

			mainExecutors, err := setupExecutors(s, cm, connectionManager, startDate, endDate, foundPipeline.Name, runID, runConfig.FullRefresh, runConfig.UsePip, runConfig.SensorMode, parser)
			if err != nil {
				errorPrinter.Println(err.Error())
				return cli.Exit("", 1)
			}
			formatOpts := executor.FormattingOptions{
				DoNotLogTimestamp: c.Bool("no-timestamp"),
				NoColor:           c.Bool("no-color"),
			}

			ex, err := executor.NewConcurrent(logger, mainExecutors, c.Int("workers"), formatOpts)
			if err != nil {
				errorPrinter.Printf("Failed to create executor: %v\n", err)
				return cli.Exit("", 1)
			}

			runCtx := context.Background()
			runCtx = context.WithValue(runCtx, pipeline.RunConfigFullRefresh, runConfig.FullRefresh)
			runCtx = context.WithValue(runCtx, pipeline.RunConfigStartDate, startDate)
			runCtx = context.WithValue(runCtx, pipeline.RunConfigEndDate, endDate)
			runCtx = context.WithValue(runCtx, executor.KeyIsDebug, isDebug)
			runCtx = context.WithValue(runCtx, python.CtxUseWingetForUv, runConfig.ExpUseWingetForUv) //nolint:staticcheck
			runCtx = context.WithValue(runCtx, python.LocalIngestr, c.String("debug-ingestr-src"))
			runCtx = context.WithValue(runCtx, config.EnvironmentContextKey, cm.SelectedEnvironment)

			exeCtx, cancel := signal.NotifyContext(runCtx, syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			ex.Start(exeCtx, s.WorkQueue, s.Results)

			start := time.Now()
			results := s.Run(runCtx)
			duration := time.Since(start)

			if err := s.SavePipelineState(afero.NewOsFs(), runConfig, runID, statePath); err != nil {
				logger.Error("failed to save pipeline state", zap.Error(err))
			}

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

func ReadState(fs afero.Fs, statePath string, filter *Filter) (*scheduler.PipelineState, error) {
	pipelineState, err := scheduler.ReadState(fs, statePath)
	if err != nil {
		errorPrinter.Printf("Failed to restore state: %v\n", err)
		return nil, err
	}
	filter.IncludeTag = pipelineState.Parameters.Tag
	filter.OnlyTaskTypes = pipelineState.Parameters.Only
	filter.PushMetaData = pipelineState.Parameters.PushMetadata
	filter.ExcludeTag = pipelineState.Parameters.ExcludeTag
	return pipelineState, nil
}

func GetPipeline(ctx context.Context, inputPath string, runConfig *scheduler.RunConfig, logger *zap.SugaredLogger) (*PipelineInfo, error) {
	pipelinePath := inputPath
	runningForAnAsset := isPathReferencingAsset(inputPath)
	if runningForAnAsset && runConfig.Tag != "" {
		errorPrinter.Printf("You cannot use the '--tag' flag when running a single asset.\n")
		return nil, errors.New("you cannot use the '--tag' flag when running a single asset")
	}

	var task *pipeline.Asset
	var err error
	runDownstreamTasks := false

	if runningForAnAsset {
		pipelinePath, err = path.GetPipelineRootFromTask(inputPath, pipelineDefinitionFiles)
		if err != nil {
			errorPrinter.Printf("Failed to find the pipeline this task belongs to: '%s'\n", inputPath)
			return &PipelineInfo{
				RunningForAnAsset:  runningForAnAsset,
				RunDownstreamTasks: runDownstreamTasks,
			}, err
		}
	}

	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate())
	if err != nil {
		errorPrinter.Println("failed to build pipeline, are you sure you have referred the right path?")
		errorPrinter.Println("\nHint: You need to run this command with a path to either the pipeline directory or the asset file itself directly.")

		return &PipelineInfo{
			Pipeline:           foundPipeline,
			RunningForAnAsset:  runningForAnAsset,
			RunDownstreamTasks: runDownstreamTasks,
		}, err
	}

	if runningForAnAsset {
		task, err = DefaultPipelineBuilder.CreateAssetFromFile(inputPath, foundPipeline)
		if err != nil {
			errorPrinter.Printf("Failed to build asset: %v. Are you sure you used the correct path?\n", err.Error())
			return &PipelineInfo{
				RunningForAnAsset:  runningForAnAsset,
				RunDownstreamTasks: runDownstreamTasks,
				Pipeline:           foundPipeline,
			}, err
		}

		task, err = DefaultPipelineBuilder.MutateAsset(ctx, task, foundPipeline)
		if err != nil {
			errorPrinter.Printf("Failed to mutate asset: %v\n", err)
			return &PipelineInfo{
				RunningForAnAsset:  runningForAnAsset,
				RunDownstreamTasks: runDownstreamTasks,
				Pipeline:           foundPipeline,
			}, err
		}
		if task == nil {
			errorPrinter.Printf("The given file path doesn't seem to be a Bruin task definition: '%s'\n", inputPath)
			return &PipelineInfo{
				RunningForAnAsset:  runningForAnAsset,
				RunDownstreamTasks: runDownstreamTasks,
				Pipeline:           foundPipeline,
			}, err
		}
	}

	return &PipelineInfo{
		Pipeline:           foundPipeline,
		RunningForAnAsset:  runningForAnAsset,
		RunDownstreamTasks: runConfig.Downstream,
	}, nil
}

func ParseDate(startDateStr, endDateStr string, logger *zap.SugaredLogger) (time.Time, time.Time, error) {
	startDate, err := date.ParseTime(startDateStr)
	logger.Debug("given start date: ", startDate)
	if err != nil {
		errorPrinter.Printf("Please give a valid start date: bruin run --start-date <start date>)\n")
		errorPrinter.Printf("A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats. \n")
		errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
		errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
		return time.Time{}, time.Time{}, err
	}

	endDate, err := date.ParseTime(endDateStr)
	logger.Debug("given end date: ", endDate)
	if err != nil {
		errorPrinter.Printf("Please give a valid end date: bruin run --start-date <start date>)\n")
		errorPrinter.Printf("A valid start date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats. \n")
		errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
		errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
		return time.Time{}, time.Time{}, err
	}

	return startDate, endDate, nil
}

func ValidateRunConfig(runConfig *scheduler.RunConfig, inputPath string, logger *zap.SugaredLogger) (time.Time, time.Time, string, error) {
	if inputPath == "" {
		inputPath = "."
	}

	startDate, endDate, err := ParseDate(runConfig.StartDate, runConfig.EndDate, logger)
	if err != nil {
		return time.Now(), time.Now(), "", err
	}

	return startDate, endDate, inputPath, nil
}

func CheckLint(foundPipeline *pipeline.Pipeline, pipelinePath string, logger *zap.SugaredLogger, parser *sqlparser.SQLParser) error {
	rules, err := lint.GetRules(fs, &git.RepoFinder{}, true, parser, true)
	if err != nil {
		errorPrinter.Printf("An error occurred while linting the pipelines: %v\n", err)
		return err
	}

	rules = lint.FilterRulesBySpeed(rules, true)

	linter := lint.NewLinter(path.GetPipelinePaths, DefaultPipelineBuilder, rules, logger)
	res, err := linter.LintPipelines([]*pipeline.Pipeline{foundPipeline})
	err = reportLintErrors(res, err, lint.Printer{RootCheckPath: pipelinePath}, "")
	if err != nil {
		return err
	}

	return nil
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
	usePipForPython bool,
	sensorMode string,
	parser *sqlparser.SQLParser,
) (map[pipeline.AssetType]executor.Config, error) {
	mainExecutors := executor.DefaultExecutorsV2

	// this is a heuristic we apply to find what might be the most common type of custom check in the pipeline
	// this should go away once we incorporate URIs into the assets
	estimateCustomCheckType := s.FindMajorityOfTypes(pipeline.AssetTypeBigqueryQuery)

	seedOperator, err := ingestr.NewSeedOperator(conn)
	if err != nil {
		return nil, err
	}

	if s.WillRunTaskOfType(pipeline.AssetTypePython) {
		jinjaVariables := jinja.PythonEnvVariables(&startDate, &endDate, pipelineName, runID, fullRefresh)
		if usePipForPython {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeMain] = python.NewLocalOperator(config, jinjaVariables)
		} else {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeMain] = python.NewLocalOperatorWithUv(config, conn, jinjaVariables)
		}
	}

	renderer := jinja.NewRendererWithStartEndDates(&startDate, &endDate, pipelineName, runID)
	wholeFileExtractor := &query.WholeFileExtractor{
		Fs:       fs,
		Renderer: renderer,
	}

	customCheckRunner := ansisql.NewCustomCheckOperator(conn, renderer)

	if s.WillRunTaskOfType(pipeline.AssetTypeBigqueryQuery) || estimateCustomCheckType == pipeline.AssetTypeBigqueryQuery || s.WillRunTaskOfType(pipeline.AssetTypeBigquerySeed) || s.WillRunTaskOfType(pipeline.AssetTypeBigqueryQuerySensor) || s.WillRunTaskOfType(pipeline.AssetTypeBigqueryTableSensor) {
		bqOperator := bigquery.NewBasicOperator(conn, wholeFileExtractor, bigquery.NewMaterializer(fullRefresh))
		bqCheckRunner, err := bigquery.NewColumnCheckOperator(conn)
		if err != nil {
			return nil, err
		}

		metadataPushOperator := bigquery.NewMetadataPushOperator(conn)
		bqQuerySensor := bigquery.NewQuerySensor(conn, renderer, sensorMode)
		bqTableSensor := bigquery.NewTableSensor(conn, sensorMode)

		mainExecutors[pipeline.AssetTypeBigqueryQuery][scheduler.TaskInstanceTypeMain] = bqOperator
		mainExecutors[pipeline.AssetTypeBigqueryQuery][scheduler.TaskInstanceTypeColumnCheck] = bqCheckRunner
		mainExecutors[pipeline.AssetTypeBigqueryQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		mainExecutors[pipeline.AssetTypeBigqueryQuery][scheduler.TaskInstanceTypeMetadataPush] = metadataPushOperator

		mainExecutors[pipeline.AssetTypeBigquerySource][scheduler.TaskInstanceTypeMetadataPush] = metadataPushOperator
		mainExecutors[pipeline.AssetTypeBigquerySource][scheduler.TaskInstanceTypeColumnCheck] = bqCheckRunner
		mainExecutors[pipeline.AssetTypeBigquerySource][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeBigqueryTableSensor][scheduler.TaskInstanceTypeMain] = bqTableSensor
		mainExecutors[pipeline.AssetTypeBigqueryTableSensor][scheduler.TaskInstanceTypeMetadataPush] = metadataPushOperator
		mainExecutors[pipeline.AssetTypeBigqueryTableSensor][scheduler.TaskInstanceTypeColumnCheck] = bqCheckRunner
		mainExecutors[pipeline.AssetTypeBigqueryTableSensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeBigqueryQuerySensor][scheduler.TaskInstanceTypeMain] = bqQuerySensor
		mainExecutors[pipeline.AssetTypeBigqueryQuerySensor][scheduler.TaskInstanceTypeMetadataPush] = metadataPushOperator
		mainExecutors[pipeline.AssetTypeBigqueryQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = bqCheckRunner
		mainExecutors[pipeline.AssetTypeBigqueryQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeBigquerySeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeBigquerySeed][scheduler.TaskInstanceTypeColumnCheck] = bqCheckRunner
		mainExecutors[pipeline.AssetTypeBigquerySeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		mainExecutors[pipeline.AssetTypeBigquerySeed][scheduler.TaskInstanceTypeMetadataPush] = metadataPushOperator
		// we set the Python runners to run the checks on BigQuery assuming that there won't be many usecases where a user has both BQ and Snowflake
		if estimateCustomCheckType == pipeline.AssetTypeBigqueryQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = bqCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeMetadataPush] = metadataPushOperator
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypePostgresQuery) || estimateCustomCheckType == pipeline.AssetTypePostgresQuery ||
		s.WillRunTaskOfType(pipeline.AssetTypeRedshiftQuery) || estimateCustomCheckType == pipeline.AssetTypeRedshiftQuery || s.WillRunTaskOfType(pipeline.AssetTypeRedshiftSeed) || s.WillRunTaskOfType(pipeline.AssetTypePostgresSeed) {
		pgCheckRunner := postgres.NewColumnCheckOperator(conn)
		pgOperator := postgres.NewBasicOperator(conn, wholeFileExtractor, postgres.NewMaterializer(fullRefresh), parser)

		mainExecutors[pipeline.AssetTypeRedshiftQuery][scheduler.TaskInstanceTypeMain] = pgOperator
		mainExecutors[pipeline.AssetTypeRedshiftQuery][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypeRedshiftQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypePostgresQuery][scheduler.TaskInstanceTypeMain] = pgOperator
		mainExecutors[pipeline.AssetTypePostgresQuery][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypePostgresQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypePostgresSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypePostgresSeed][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypePostgresSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeRedshiftSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeRedshiftSeed][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypeRedshiftSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		// we set the Python runners to run the checks on Snowflake assuming that there won't be many usecases where a user has both BQ and Snowflake
		if estimateCustomCheckType == pipeline.AssetTypePostgresQuery || estimateCustomCheckType == pipeline.AssetTypeRedshiftQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	shouldInitiateSnowflake := s.WillRunTaskOfType(pipeline.AssetTypeSnowflakeQuery) || s.WillRunTaskOfType(pipeline.AssetTypeSnowflakeQuerySensor) || estimateCustomCheckType == pipeline.AssetTypeSnowflakeQuery || s.WillRunTaskOfType(pipeline.AssetTypeSnowflakeSeed)
	if shouldInitiateSnowflake {
		sfOperator := snowflake.NewBasicOperator(conn, wholeFileExtractor, snowflake.NewMaterializer(fullRefresh))

		sfCheckRunner := snowflake.NewColumnCheckOperator(conn)

		sfQuerySensor := snowflake.NewQuerySensor(conn, renderer, 30)

		sfMetadataPushOperator := snowflake.NewMetadataPushOperator(conn)

		mainExecutors[pipeline.AssetTypeSnowflakeQuery][scheduler.TaskInstanceTypeMain] = sfOperator
		mainExecutors[pipeline.AssetTypeSnowflakeQuery][scheduler.TaskInstanceTypeColumnCheck] = sfCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeQuerySensor][scheduler.TaskInstanceTypeMain] = sfQuerySensor
		mainExecutors[pipeline.AssetTypeSnowflakeQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = sfCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeQuery][scheduler.TaskInstanceTypeMetadataPush] = sfMetadataPushOperator

		mainExecutors[pipeline.AssetTypeSnowflakeSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeSnowflakeSeed][scheduler.TaskInstanceTypeColumnCheck] = sfCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeSeed][scheduler.TaskInstanceTypeMetadataPush] = sfMetadataPushOperator

		// we set the Python runners to run the checks on Snowflake assuming that there won't be many usecases where a user has both BQ and Snowflake
		if estimateCustomCheckType == pipeline.AssetTypeSnowflakeQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = sfCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeMetadataPush] = sfMetadataPushOperator
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeMsSQLQuery) || estimateCustomCheckType == pipeline.AssetTypeMsSQLQuery ||
		s.WillRunTaskOfType(pipeline.AssetTypeSynapseQuery) || estimateCustomCheckType == pipeline.AssetTypeSynapseQuery || s.WillRunTaskOfType(pipeline.AssetTypeMsSQLSeed) || s.WillRunTaskOfType(pipeline.AssetTypeSynapseSeed) {
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

		mainExecutors[pipeline.AssetTypeMsSQLSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeMsSQLSeed][scheduler.TaskInstanceTypeColumnCheck] = msCheckRunner
		mainExecutors[pipeline.AssetTypeMsSQLSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeSynapseSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeSynapseSeed][scheduler.TaskInstanceTypeColumnCheck] = synapseCheckRunner
		mainExecutors[pipeline.AssetTypeSynapseSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		// we set the Python runners to run the checks on MsSQL
		if estimateCustomCheckType == pipeline.AssetTypeMsSQLQuery || estimateCustomCheckType == pipeline.AssetTypeSynapseQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = msCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeDatabricksQuery) || estimateCustomCheckType == pipeline.AssetTypeDatabricksQuery || s.WillRunTaskOfType(pipeline.AssetTypeDatabricksSeed) {
		databricksOperator := databricks.NewBasicOperator(conn, wholeFileExtractor, databricks.NewMaterializer(fullRefresh))
		databricksCheckRunner := databricks.NewColumnCheckOperator(conn)

		mainExecutors[pipeline.AssetTypeDatabricksQuery][scheduler.TaskInstanceTypeMain] = databricksOperator
		mainExecutors[pipeline.AssetTypeDatabricksQuery][scheduler.TaskInstanceTypeColumnCheck] = databricksCheckRunner
		mainExecutors[pipeline.AssetTypeDatabricksQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeDatabricksSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeDatabricksSeed][scheduler.TaskInstanceTypeColumnCheck] = databricksCheckRunner
		mainExecutors[pipeline.AssetTypeDatabricksSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

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

	if s.WillRunTaskOfType(pipeline.AssetTypeAthenaQuery) || estimateCustomCheckType == pipeline.AssetTypeAthenaQuery || s.WillRunTaskOfType(pipeline.AssetTypeAthenaSeed) {
		athenaOperator := athena.NewBasicOperator(conn, wholeFileExtractor, athena.NewMaterializer(fullRefresh))
		athenaCheckRunner := athena.NewColumnCheckOperator(conn)

		mainExecutors[pipeline.AssetTypeAthenaQuery][scheduler.TaskInstanceTypeMain] = athenaOperator
		mainExecutors[pipeline.AssetTypeAthenaQuery][scheduler.TaskInstanceTypeColumnCheck] = athenaCheckRunner
		mainExecutors[pipeline.AssetTypeAthenaQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeAthenaSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeAthenaSeed][scheduler.TaskInstanceTypeColumnCheck] = athenaCheckRunner
		mainExecutors[pipeline.AssetTypeAthenaSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		if estimateCustomCheckType == pipeline.AssetTypeAthenaQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = athenaCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeDuckDBQuery) || estimateCustomCheckType == pipeline.AssetTypeDuckDBQuery || s.WillRunTaskOfType(pipeline.AssetTypeDuckDBSeed) {
		duckDBOperator := duck.NewBasicOperator(conn, wholeFileExtractor, duck.NewMaterializer(fullRefresh))
		duckDBCheckRunner := duck.NewColumnCheckOperator(conn)

		mainExecutors[pipeline.AssetTypeDuckDBQuery][scheduler.TaskInstanceTypeMain] = duckDBOperator
		mainExecutors[pipeline.AssetTypeDuckDBQuery][scheduler.TaskInstanceTypeColumnCheck] = duckDBCheckRunner
		mainExecutors[pipeline.AssetTypeDuckDBQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeDuckDBSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeDuckDBSeed][scheduler.TaskInstanceTypeColumnCheck] = duckDBCheckRunner
		mainExecutors[pipeline.AssetTypeDuckDBSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		if estimateCustomCheckType == pipeline.AssetTypeDuckDBQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = duckDBCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	// ClickHouse
	if s.WillRunTaskOfType(pipeline.AssetTypeClickHouse) || estimateCustomCheckType == pipeline.AssetTypeClickHouse || s.WillRunTaskOfType(pipeline.AssetTypeClickHouseSeed) {
		clickHouseOperator := clickhouse.NewBasicOperator(conn, wholeFileExtractor, clickhouse.NewMaterializer(fullRefresh))
		checkRunner := clickhouse.NewColumnCheckOperator(conn)

		mainExecutors[pipeline.AssetTypeClickHouse][scheduler.TaskInstanceTypeMain] = clickHouseOperator
		mainExecutors[pipeline.AssetTypeClickHouse][scheduler.TaskInstanceTypeColumnCheck] = checkRunner
		mainExecutors[pipeline.AssetTypeClickHouse][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeClickHouseSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeClickHouseSeed][scheduler.TaskInstanceTypeColumnCheck] = checkRunner
		mainExecutors[pipeline.AssetTypeClickHouseSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		if estimateCustomCheckType == pipeline.AssetTypeClickHouse {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = checkRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeEMRServerlessSpark) {
		emrServerlessOperator, err := emr_serverless.NewBasicOperator(config)
		if err != nil {
			return nil, err
		}
		mainExecutors[pipeline.AssetTypeEMRServerlessSpark][scheduler.TaskInstanceTypeMain] = emrServerlessOperator
	}

	return mainExecutors, nil
}

func isPathReferencingAsset(p string) bool {
	// Check if the path matches any of the pipeline definition file names
	for _, pipelineDefinitionfile := range pipelineDefinitionFiles {
		if strings.HasSuffix(p, pipelineDefinitionfile) {
			return false
		}
	}

	// Return true only if it's not a directory
	return !isDir(p)
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

type Filter struct {
	IncludeTag        string   // Tag to include assets (from `--tag`)
	OnlyTaskTypes     []string // Task types to include (from `--only`)
	IncludeDownstream bool     // Whether to include downstream tasks (from `--downstream`)
	PushMetaData      bool
	SingleTask        *pipeline.Asset
	ExcludeTag        string
}

func HandleSingleTask(ctx context.Context, f *Filter, s *scheduler.Scheduler, p *pipeline.Pipeline) error {
	if f.SingleTask == nil {
		if f.IncludeDownstream {
			return errors.New("cannot use the --downstream flag when running the whole pipeline")
		}
		return nil
	}
	s.MarkAll(scheduler.Skipped)
	s.MarkAsset(f.SingleTask, scheduler.Pending, f.IncludeDownstream)
	if f.IncludeTag != "" {
		return errors.New("you cannot use the '--tag' flag when running a single asset")
	}
	if f.ExcludeTag != "" {
		if !f.IncludeDownstream {
			return errors.New("when running a single asset with '--exclude-tag', you must also use the '--downstream' flag")
		}
		excludedAssets := p.GetAssetsByTag(f.ExcludeTag)
		if len(excludedAssets) == 0 {
			return fmt.Errorf("no assets found with exclude tag '%s'", f.ExcludeTag)
		}
		s.MarkByTag(f.ExcludeTag, scheduler.Skipped, false)
	}
	return nil
}

func HandleIncludeTags(ctx context.Context, f *Filter, s *scheduler.Scheduler, p *pipeline.Pipeline) error {
	if f.IncludeTag == "" {
		return nil
	}
	s.MarkAll(scheduler.Skipped)
	includedAssets := p.GetAssetsByTag(f.IncludeTag)
	if len(includedAssets) == 0 {
		return fmt.Errorf("no assets found with include tag '%s'", f.IncludeTag)
	}
	s.MarkByTag(f.IncludeTag, scheduler.Pending, false)

	return nil
}

func HandleExcludeTags(ctx context.Context, f *Filter, s *scheduler.Scheduler, p *pipeline.Pipeline) error {
	if f.SingleTask != nil {
		return nil
	}
	if f.ExcludeTag == "" {
		return nil
	}
	excludedAssets := p.GetAssetsByTag(f.ExcludeTag)
	if len(excludedAssets) == 0 {
		return fmt.Errorf("no assets found with exclude tag '%s'", f.ExcludeTag)
	}
	s.MarkByTag(f.ExcludeTag, scheduler.Skipped, false)

	return nil
}

func FilterTaskTypes(ctx context.Context, f *Filter, s *scheduler.Scheduler, p *pipeline.Pipeline) error {
	if f.PushMetaData {
		p.MetadataPush.Global = true
	}
	if len(f.OnlyTaskTypes) > 0 {
		for _, taskType := range f.OnlyTaskTypes {
			if taskType != "main" && taskType != "checks" && taskType != "push-metadata" {
				return fmt.Errorf("invalid value for '--only' flag: '%s', available values are 'main', 'checks', and 'push-metadata'", taskType)
			}
		}

		runMain := slices.Contains(f.OnlyTaskTypes, "main")
		runChecks := slices.Contains(f.OnlyTaskTypes, "checks")
		runPushMetadata := slices.Contains(f.OnlyTaskTypes, "push-metadata")

		if !runMain {
			s.MarkPendingInstancesByType(scheduler.TaskInstanceTypeMain, scheduler.Skipped)
		}
		if !runChecks {
			s.MarkPendingInstancesByType(scheduler.TaskInstanceTypeColumnCheck, scheduler.Skipped)
			s.MarkPendingInstancesByType(scheduler.TaskInstanceTypeCustomCheck, scheduler.Skipped)
		}
		if !runPushMetadata {
			s.MarkPendingInstancesByType(scheduler.TaskInstanceTypeMetadataPush, scheduler.Skipped)
		}
	}
	return nil
}

type FilterMutator func(ctx context.Context, f *Filter, s *scheduler.Scheduler, p *pipeline.Pipeline) error

func ApplyAllFilters(ctx context.Context, f *Filter, s *scheduler.Scheduler, p *pipeline.Pipeline) error {
	s.MarkAll(scheduler.Pending)

	funcs := []FilterMutator{
		HandleSingleTask,
		HandleIncludeTags,
		HandleExcludeTags,
		FilterTaskTypes,
	}

	for _, filterFunc := range funcs {
		if err := filterFunc(ctx, f, s, p); err != nil {
			return err
		}
	}
	return nil
}
