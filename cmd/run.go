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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/athena"
	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/claudecode"
	"github.com/bruin-data/bruin/pkg/clickhouse"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/databricks"
	dataprocserverless "github.com/bruin-data/bruin/pkg/dataproc_serverless"
	"github.com/bruin-data/bruin/pkg/date"
	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/emr_serverless"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/fabric_warehouse"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/ingestr"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/mysql"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/r"
	"github.com/bruin-data/bruin/pkg/redshift"
	"github.com/bruin-data/bruin/pkg/s3"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/secrets"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/bruin-data/bruin/pkg/synapse"
	"github.com/bruin-data/bruin/pkg/tableau"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/bruin-data/bruin/pkg/trino"
	"github.com/fatih/color"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
	"github.com/xlab/treeprint"
	"go.uber.org/zap"
)

const (
	LogsFolder      = "logs"
	defaultGongPath = "/home/bruin/.local/bin/gong/gong"
)

var pythonCacheGitignorePatterns = []string{
	"__pycache__/",
	"*.py[cod]",
}

type PipelineInfo struct {
	Pipeline           *pipeline.Pipeline
	RunningForAnAsset  bool
	RunDownstreamTasks bool
}

type ExecutionSummary struct {
	TotalTasks      int
	SuccessfulTasks int
	FailedTasks     int
	SkippedTasks    int

	Assets       TaskTypeStats
	ColumnChecks TaskTypeStats
	CustomChecks TaskTypeStats
	MetadataPush TaskTypeStats

	Duration time.Duration
}

type TaskTypeStats struct {
	Total             int
	Succeeded         int
	Failed            int // Failed in main execution
	FailedDueToChecks int // Failed only due to checks (main execution succeeded)
	Skipped           int
}

func (s TaskTypeStats) HasAny() bool {
	return s.Total > 0
}

func (s TaskTypeStats) SuccessRate() float64 {
	if s.Total == 0 {
		return 0
	}
	return float64(s.Succeeded) / float64(s.Total) * 100
}

func printExecutionTable(results []*scheduler.TaskExecutionResult, s *scheduler.Scheduler) {
	// Group results by asset
	assetResults := make(map[string]map[string]*scheduler.TaskExecutionResult)
	assetOrder := make([]string, 0)

	for _, result := range results {
		assetName := result.Instance.GetAsset().Name
		if _, exists := assetResults[assetName]; !exists {
			assetResults[assetName] = make(map[string]*scheduler.TaskExecutionResult)
			assetOrder = append(assetOrder, assetName)
		}

		switch instance := result.Instance.(type) {
		case *scheduler.AssetInstance:
			assetResults[assetName]["main"] = result
		case *scheduler.ColumnCheckInstance:
			key := fmt.Sprintf("column:%s:%s", instance.Column.Name, instance.Check.Name)
			assetResults[assetName][key] = result
		case *scheduler.CustomCheckInstance:
			key := "custom:" + instance.Check.Name
			assetResults[assetName][key] = result
		}
	}

	// Only add upstream failed assets (skip the "skipped" ones entirely)
	upstreamFailedTasks := s.GetTaskInstancesByStatus(scheduler.UpstreamFailed)

	for _, task := range upstreamFailedTasks {
		assetName := task.GetAsset().Name
		if _, exists := assetResults[assetName]; !exists {
			assetResults[assetName] = make(map[string]*scheduler.TaskExecutionResult)
			assetOrder = append(assetOrder, assetName)
		}

		// Create a fake result for upstream failed task
		upstreamFailedResult := &scheduler.TaskExecutionResult{
			Instance: task,
			Error:    nil, // We'll use nil to indicate upstream failed
		}

		switch instance := task.(type) {
		case *scheduler.AssetInstance:
			assetResults[assetName]["main"] = upstreamFailedResult
		case *scheduler.ColumnCheckInstance:
			key := fmt.Sprintf("column:%s:%s", instance.Column.Name, instance.Check.Name)
			assetResults[assetName][key] = upstreamFailedResult
		case *scheduler.CustomCheckInstance:
			key := "custom:" + instance.Check.Name
			assetResults[assetName][key] = upstreamFailedResult
		}
	}

	if len(assetOrder) == 0 {
		return
	}

	fmt.Println("\n" + strings.Repeat("=", 50) + "\n")

	for _, assetName := range assetOrder {
		results := assetResults[assetName]
		mainResult := results["main"]

		// Asset name with status
		var assetStatus string
		var assetColor *color.Color

		if mainResult == nil { // nolint:gocritic
			// Asset not executed
			assetStatus = "SKIP"
			assetColor = color.New(color.Faint)
		} else if mainResult.Error == nil {
			// Check if this is upstream failed
			_, isUpstreamFailed := find(upstreamFailedTasks, mainResult.Instance)

			if isUpstreamFailed {
				assetStatus = "UPSTREAM FAILED"
				assetColor = color.New(color.FgYellow)
			} else {
				assetStatus = "PASS"
				assetColor = color.New(color.FgGreen)
			}
		} else {
			assetStatus = "FAIL"
			assetColor = color.New(color.FgRed)
		}

		fmt.Printf("%s %s ", assetColor.Sprint(assetStatus), assetName)

		// Print dots for quality checks
		checkCount := 0
		for key, result := range results {
			if key == "main" {
				continue
			}

			checkCount++
			if result == nil { // nolint:gocritic
				fmt.Print(faint("."))
			} else if result.Error == nil {
				// Check if upstream failed
				_, isUpstreamFailed := find(upstreamFailedTasks, result.Instance)

				if isUpstreamFailed {
					fmt.Print(color.New(color.FgYellow).Sprint("U"))
				} else {
					fmt.Print(color.New(color.FgGreen).Sprint("."))
				}
			} else {
				fmt.Print(color.New(color.FgRed).Sprint("F"))
			}
		}

		fmt.Println()
	}
}

func find(slice []scheduler.TaskInstance, item scheduler.TaskInstance) (int, bool) { // nolint:unparam
	for i, v := range slice {
		if v == item {
			return i, true
		}
	}
	return -1, false
}

func printExecutionSummary(results []*scheduler.TaskExecutionResult, s *scheduler.Scheduler, duration time.Duration, _ int) {
	summary := analyzeResults(results, s)
	summary.Duration = duration

	// Print execution table first
	printExecutionTable(results, s)

	// Determine overall status
	hasFailures := summary.FailedTasks > 0

	// Header with status and task count
	if hasFailures {
		summaryPrinter.Printf("\n\nbruin run completed with %s in %s\n\n",
			color.New(color.FgRed).Sprint("failures"),
			duration.Truncate(time.Millisecond).String())
	} else {
		summaryPrinter.Printf("\n\nbruin run completed %s in %s\n\n",
			color.New(color.FgGreen).Sprint("successfully"),
			duration.Truncate(time.Millisecond).String())
	}

	// Assets executed (only actual assets, not including quality checks)
	if summary.Assets.HasAny() {
		if summary.Assets.Failed > 0 || summary.Assets.FailedDueToChecks > 0 || summary.Assets.Skipped > 0 {
			summaryPrinter.Printf(" %s Assets executed      %s\n",
				color.New(color.FgRed).Sprint("✗"),
				formatCountWithSkipped(summary.Assets.Total, summary.Assets.Failed, summary.Assets.FailedDueToChecks, summary.Assets.Skipped))
		} else {
			summaryPrinter.Printf(" %s Assets executed      %s\n",
				color.New(color.FgGreen).Sprint("✓"),
				color.New(color.FgGreen).Sprintf("%d succeeded", summary.Assets.Succeeded))
		}
	}

	// Quality checks
	totalChecks := summary.ColumnChecks.Total + summary.CustomChecks.Total
	totalCheckFailures := summary.ColumnChecks.Failed + summary.CustomChecks.Failed
	totalCheckSkipped := summary.ColumnChecks.Skipped + summary.CustomChecks.Skipped
	if totalChecks > 0 {
		if totalCheckFailures > 0 || totalCheckSkipped > 0 {
			summaryPrinter.Printf(" %s Quality checks       %s\n",
				color.New(color.FgRed).Sprint("✗"),
				formatCountWithSkipped(totalChecks, totalCheckFailures, 0, totalCheckSkipped))
		} else {
			summaryPrinter.Printf(" %s Quality checks       %s\n",
				color.New(color.FgGreen).Sprint("✓"),
				color.New(color.FgGreen).Sprintf("%d succeeded", summary.ColumnChecks.Succeeded+summary.CustomChecks.Succeeded))
		}
	}

	// Metadata push
	if summary.MetadataPush.HasAny() {
		metadataExecuted := summary.MetadataPush.Succeeded + summary.MetadataPush.Failed
		if summary.MetadataPush.Failed > 0 {
			summaryPrinter.Printf(" %s Metadata pushed      %s\n",
				color.New(color.FgRed).Sprint("✗"),
				formatCount(metadataExecuted, summary.MetadataPush.Failed))
		} else {
			summaryPrinter.Printf(" %s Metadata pushed      %d\n",
				color.New(color.FgGreen).Sprint("✓"), metadataExecuted)
		}
	}
}

func formatCount(total, failed int) string {
	if failed == 0 {
		return strconv.Itoa(total)
	}
	succeeded := total - failed
	return fmt.Sprintf("%s / %s",
		color.New(color.FgRed).Sprintf("%d failed", failed),
		color.New(color.FgGreen).Sprintf("%d succeeded", succeeded))
}

func formatCountWithSkipped(total, failed, failedDueToChecks, skipped int) string {
	succeeded := total - failed - failedDueToChecks - skipped

	var parts []string
	if failed > 0 {
		parts = append(parts, color.New(color.FgRed).Sprintf("%d failed", failed))
	}
	if failedDueToChecks > 0 {
		parts = append(parts, color.New(color.FgYellow).Sprintf("%d failed due to checks", failedDueToChecks))
	}
	if succeeded > 0 {
		parts = append(parts, color.New(color.FgGreen).Sprintf("%d succeeded", succeeded))
	}
	if skipped > 0 {
		parts = append(parts, color.New(color.Faint).Sprintf("%d skipped", skipped))
	}

	if len(parts) == 0 {
		return "0"
	}
	if len(parts) == 1 && failed == 0 && failedDueToChecks == 0 && skipped == 0 {
		return strconv.Itoa(succeeded)
	}

	return strings.Join(parts, " / ")
}

func analyzeResults(results []*scheduler.TaskExecutionResult, s *scheduler.Scheduler) ExecutionSummary {
	summary := ExecutionSummary{}

	// Track asset status by asset name
	assetMainStatus := make(map[string]bool)       // true if main execution succeeded
	assetHasCheckFailures := make(map[string]bool) // true if any check failed
	assetNames := make(map[string]bool)            // all assets seen

	// Count all tasks by type and status
	for _, result := range results {
		summary.TotalTasks++

		// Determine if task succeeded
		succeeded := result.Error == nil
		if succeeded {
			summary.SuccessfulTasks++
		} else {
			summary.FailedTasks++
		}

		// Categorize by task type
		switch instance := result.Instance.(type) {
		case *scheduler.AssetInstance:
			assetName := instance.GetAsset().Name
			assetNames[assetName] = true
			assetMainStatus[assetName] = succeeded

		case *scheduler.ColumnCheckInstance:
			assetName := instance.GetAsset().Name
			assetNames[assetName] = true
			if !succeeded {
				assetHasCheckFailures[assetName] = true
			}
			summary.ColumnChecks.Total++
			if succeeded {
				summary.ColumnChecks.Succeeded++
			} else {
				summary.ColumnChecks.Failed++
			}
		case *scheduler.CustomCheckInstance:
			assetName := instance.GetAsset().Name
			assetNames[assetName] = true
			if !succeeded {
				assetHasCheckFailures[assetName] = true
			}
			summary.CustomChecks.Total++
			if succeeded {
				summary.CustomChecks.Succeeded++
			} else {
				summary.CustomChecks.Failed++
			}
		case *scheduler.MetadataPushInstance:
			summary.MetadataPush.Total++
			if succeeded {
				summary.MetadataPush.Succeeded++
			} else {
				summary.MetadataPush.Failed++
			}
		}
	}

	// Analyze asset-level results
	for assetName := range assetNames {
		mainSucceeded, mainSeen := assetMainStatus[assetName]
		if !mainSeen {
			continue
		}

		summary.Assets.Total++
		hasCheckFailures := assetHasCheckFailures[assetName]

		if mainSucceeded && !hasCheckFailures { // nolint:gocritic
			summary.Assets.Succeeded++
		} else if !mainSucceeded {
			summary.Assets.Failed++ // Failed in main execution
		} else if mainSucceeded && hasCheckFailures {
			summary.Assets.FailedDueToChecks++ // Main succeeded but checks failed
		}
	}

	// Don't count truly skipped tasks (those filtered out) in the summary

	// Count upstream failed tasks (they should be shown as skipped in summary)
	upstreamFailedTasks := s.GetTaskInstancesByStatus(scheduler.UpstreamFailed)
	upstreamFailedAssets := make(map[string]bool)
	for _, t := range upstreamFailedTasks {
		summary.SkippedTasks++

		assetName := t.GetAsset().Name
		if !upstreamFailedAssets[assetName] {
			upstreamFailedAssets[assetName] = true
			summary.Assets.Total++
			summary.Assets.Skipped++
		}

		// Also count individual check tasks as skipped
		switch t.(type) {
		case *scheduler.ColumnCheckInstance:
			summary.ColumnChecks.Total++
			summary.ColumnChecks.Skipped++
		case *scheduler.CustomCheckInstance:
			summary.CustomChecks.Total++
			summary.CustomChecks.Skipped++
		case *scheduler.MetadataPushInstance:
			summary.MetadataPush.Total++
			summary.MetadataPush.Skipped++
		}
	}

	// Update total tasks to include skipped ones
	summary.TotalTasks += summary.SkippedTasks

	return summary
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
		Sources:     cli.EnvVars("BRUIN_START_DATE"),
	}
	endDateFlag = &cli.StringFlag{
		Name:        "end-date",
		Usage:       "the end date of the range the pipeline will run for in YYYY-MM-DD, YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff format",
		DefaultText: "end of yesterday, e.g. " + defaultEndDate.Format("2006-01-02 15:04:05") + ".999999",
		Value:       defaultEndDate.Format("2006-01-02 15:04:05") + ".999999",
		Sources:     cli.EnvVars("BRUIN_END_DATE"),
	}
)

func setApplyIntervalModifiers(c *cli.Command) bool {
	fullRefresh := c.Bool("full-refresh")
	applyIntervalModifiers := c.Bool("apply-interval-modifiers")

	if fullRefresh && applyIntervalModifiers {
		warningPrinter.Println("Warning: --apply-interval-modifiers flag is ignored when --full-refresh is enabled.")
		return false
	}

	return applyIntervalModifiers
}

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
				Sources: cli.EnvVars("BRUIN_FULL_REFRESH"),
			},
			&cli.BoolFlag{
				Name:        "apply-interval-modifiers",
				Usage:       "apply interval modifiers",
				DefaultText: "false",
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
				Name:  "single-check",
				Usage: "runs a single column or custom check by ID",
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
			&cli.BoolFlag{
				Name:   "use-gong",
				Usage:  "use gong binary for execution",
				Hidden: true,
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
			&cli.StringFlag{
				Name:    "secrets-backend",
				Sources: cli.EnvVars("BRUIN_SECRETS_BACKEND"),
				Usage:   "the source of secrets if different from .bruin.yml. Possible values: 'vault', 'doppler', 'aws'",
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
				Name:  "verbose",
				Usage: "print verbose output including SQL queries",
			},
			&cli.BoolFlag{
				Name:   "minimal-logs",
				Usage:  "skip initial pipeline analysis logs for this run",
				Hidden: true,
			},
			&cli.StringSliceFlag{
				Name:    "var",
				Usage:   "override pipeline variables with custom values",
				Sources: cli.EnvVars("BRUIN_VARS"),
			},
			&cli.IntFlag{
				Name:  "timeout",
				Usage: "timeout for the entire pipeline run in seconds",
				Value: 604800, // 7 days default
			},
			&cli.StringFlag{
				Name:  "query-annotations",
				Usage: fmt.Sprintf("JSON string containing annotations to be added as comments to queries. Use '%s' to only include default annotations.", ansisql.DefaultQueryAnnotations),
			},
		},
		DisableSliceFlagSeparator: true,
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()

			logger := makeLogger(*isDebug)
			fullRefresh := c.Bool("full-refresh")
			applyIntervalModifiers := setApplyIntervalModifiers(c)

			useGong := c.Bool("use-gong")

			// When using gong, the path must exist and be executable
			if useGong {
				info, err := os.Stat(defaultGongPath)
				if err != nil {
					if os.IsNotExist(err) {
						return cli.Exit("gong binary not found at path: "+defaultGongPath, 1)
					}
					return cli.Exit(fmt.Sprintf("failed to access gong binary at path '%s': %v", defaultGongPath, err), 1)
				}
				if info.Mode()&0o111 == 0 {
					return cli.Exit(fmt.Sprintf("gong binary at path '%s' is not executable", defaultGongPath), 1)
				}
			}

			runConfig := &scheduler.RunConfig{
				Downstream:             c.Bool("downstream"),
				StartDate:              c.String("start-date"),
				EndDate:                c.String("end-date"),
				Workers:                c.Int("workers"),
				Environment:            c.String("environment"),
				Force:                  c.Bool("force"),
				PushMetadata:           c.Bool("push-metadata"),
				NoLogFile:              c.Bool("no-log-file"),
				FullRefresh:            fullRefresh,
				UsePip:                 c.Bool("use-pip"),
				Tag:                    c.String("tag"),
				ExcludeTag:             c.String("exclude-tag"),
				Only:                   c.StringSlice("only"),
				Output:                 c.String("output"),
				ExpUseWingetForUv:      c.Bool("exp-use-winget-for-uv"),
				ConfigFilePath:         c.String("config-file"),
				SensorMode:             c.String("sensor-mode"),
				ApplyIntervalModifiers: applyIntervalModifiers,
				Annotations:            c.String("query-annotations"),
				UseGong:                useGong,
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
				DefaultPipelineBuilder.AddAssetMutator(func(ctx context.Context, asset *pipeline.Asset, foundPipeline *pipeline.Pipeline) (*pipeline.Asset, error) {
					asset.PrefixSchema(cm.SelectedEnvironment.SchemaPrefix)
					asset.PrefixUpstreams(cm.SelectedEnvironment.SchemaPrefix)
					return asset, nil
				})
			}

			if vars := c.StringSlice("var"); len(vars) > 0 {
				DefaultPipelineBuilder.AddPipelineMutator(variableOverridesMutator(vars))
			}

			runID := NewRunID()
			runCtx := context.WithValue(ctx, pipeline.RunConfigFullRefresh, runConfig.FullRefresh)
			runCtx = context.WithValue(runCtx, pipeline.RunConfigStartDate, startDate)
			runCtx = context.WithValue(runCtx, pipeline.RunConfigEndDate, endDate)
			runCtx = context.WithValue(runCtx, pipeline.RunConfigApplyIntervalModifiers, applyIntervalModifiers)
			runCtx = context.WithValue(runCtx, executor.KeyIsDebug, isDebug)
			runCtx = context.WithValue(runCtx, executor.KeyVerbose, c.Bool("verbose"))
			runCtx = context.WithValue(runCtx, python.CtxUseWingetForUv, runConfig.ExpUseWingetForUv) //nolint:staticcheck
			runCtx = context.WithValue(runCtx, python.LocalIngestr, c.String("debug-ingestr-src"))
			runCtx = context.WithValue(runCtx, config.EnvironmentContextKey, cm.SelectedEnvironment)
			runCtx = context.WithValue(runCtx, pipeline.RunConfigRunID, runID)
			runCtx = context.WithValue(runCtx, pipeline.RunConfigFullRefresh, runConfig.FullRefresh)
			runCtx = context.WithValue(runCtx, pipeline.RunConfigQueryAnnotations, runConfig.Annotations)

			preview, err := GetPipeline(runCtx, inputPath, runConfig, logger, pipeline.WithOnlyPipeline())
			if err != nil {
				return err
			}

			var task *pipeline.Asset
			if preview.RunningForAnAsset && c.Args().Len() == 1 {
				task, err = DefaultPipelineBuilder.CreateAssetFromFile(inputPath, preview.Pipeline)
				if err != nil {
					errorPrinter.Printf("Failed to build asset: %v\n", err)
					return cli.Exit("", 1)
				}
				task, err = DefaultPipelineBuilder.MutateAsset(runCtx, task, preview.Pipeline)
				if err != nil {
					errorPrinter.Printf("Failed to mutate asset: %v\n", err)
					return cli.Exit("", 1)
				}
				if task == nil {
					errorPrinter.Printf("Failed to create asset from file '%s'\n", inputPath)
					return cli.Exit("", 1)
				}
			}

			statePath := filepath.Join(repoRoot.Path, "logs/runs", preview.Pipeline.Name)
			err = git.EnsureGivenPatternIsInGitignore(afero.NewOsFs(), repoRoot.Path, "logs/runs")
			if err != nil {
				errorPrinter.Printf("Failed to add the run state folder to .gitignore: %v\n", err)
				return cli.Exit("", 1)
			}

			// Initialize selectedAssets early (will be populated later if multiple assets are specified)
			var selectedAssets []*pipeline.Asset

			singleCheckID := c.String("single-check")
			filter := &Filter{
				IncludeTag:        runConfig.Tag,
				OnlyTaskTypes:     runConfig.Only,
				IncludeDownstream: preview.RunDownstreamTasks,
				PushMetaData:      runConfig.PushMetadata,
				SingleTask:        task,
				SelectedAssets:    selectedAssets,
				ExcludeTag:        runConfig.ExcludeTag,
				singleCheckID: scheduler.CheckUniqueID{
					ID:    singleCheckID,
					Asset: task,
				},
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

			// Set gong context after --continue restores RunConfig to ensure consistency
			if runConfig.UseGong {
				runCtx = context.WithValue(runCtx, python.CtxGongPath, defaultGongPath)
			}

			// Load macros from the pipeline's macros directory
			macroContent, err := jinja.LoadMacros(fs, preview.Pipeline.MacrosPath)
			if err != nil {
				logger.Error("Failed to load macros", "error", err)
				return cli.Exit("", 1)
			}

			renderer := jinja.NewRendererWithStartEndDatesAndMacros(&startDate, &endDate, preview.Pipeline.Name, runID, nil, macroContent)
			DefaultPipelineBuilder.AddAssetMutator(renderAssetParamsMutator(renderer))

			pipelineInfo, err := GetPipeline(runCtx, inputPath, runConfig, logger)
			if err != nil {
				return err
			}

			// Load assets from positional arguments
			// This allows: bruin run asset1.sql asset2.sql asset3.sql
			// Pipeline is inferred from the first asset file
			var assetPaths []string
			if c.Args().Len() > 1 && pipelineInfo.RunningForAnAsset {
				// Cannot use --single-check with multiple assets
				if singleCheckID != "" {
					errorPrinter.Printf("Cannot use --single-check flag when running multiple assets. --single-check can only be used with a single asset.\n")
					return cli.Exit("", 1)
				}
				assetPaths = c.Args().Slice()
				preview.RunningForAnAsset = false
				pipelineInfo.RunningForAnAsset = false
				task = nil
				filter.SingleTask = nil
				filter.singleCheckID = scheduler.CheckUniqueID{}
				// Clear local variable to prevent incorrect output formatting
				singleCheckID = ""
			}
			if len(assetPaths) > 0 {
				// Cannot use --single-check with multiple assets
				if singleCheckID != "" {
					errorPrinter.Printf("Cannot use --single-check flag when running multiple assets. --single-check can only be used with a single asset.\n")
					return cli.Exit("", 1)
				}
				// Get current working directory for path resolution
				cwd, err := os.Getwd()
				if err != nil {
					cwd = repoRoot.Path // Fallback to repo root
				}
				selectedAssets, err = loadAssetsFromPaths(assetPaths, pipelineInfo.Pipeline, repoRoot.Path, cwd)
				if err != nil {
					errorPrinter.Printf("Failed to load assets: %v\n", err)
					return cli.Exit("", 1)
				}

				if len(selectedAssets) == 0 {
					errorPrinter.Printf("No valid assets found from the provided paths\n")
					return cli.Exit("", 1)
				}

				// Update filter with loaded assets
				filter.SelectedAssets = selectedAssets
				// Clear local variable to prevent incorrect output formatting
				singleCheckID = ""
			}

			// Re-determine start date based on pipeline configuration and full-refresh flag
			startDate, err = DetermineStartDate(runConfig.StartDate, pipelineInfo.Pipeline, runConfig.FullRefresh, logger)
			if err != nil {
				return err
			}

			// Parse end date directly from CLI
			endDate, err = date.ParseTime(runConfig.EndDate)
			if err != nil {
				return err
			}

			// Validate date range
			if err := ValidateDateRange(startDate, endDate); err != nil {
				return err
			}

			// Update renderer with the finalized start/end dates
			renderer = jinja.NewRendererWithStartEndDatesAndMacros(&startDate, &endDate, pipelineInfo.Pipeline.Name, runID, nil, macroContent)
			DefaultPipelineBuilder.AddAssetMutator(renderAssetParamsMutator(renderer))

			// Update context with the finalized dates
			runCtx = context.WithValue(runCtx, pipeline.RunConfigStartDate, startDate)
			runCtx = context.WithValue(runCtx, pipeline.RunConfigEndDate, endDate)

			// handle log files
			executionStartLog := "Starting execution..."
			if !c.Bool("minimal-logs") {
				infoPrinter.Printf("Analyzed the pipeline '%s' with %d assets.\n", pipelineInfo.Pipeline.Name, len(pipelineInfo.Pipeline.Assets))

				if pipelineInfo.RunningForAnAsset {
					infoPrinter.Printf("Running only the asset '%s'\n", task.Name)
				} else if len(filter.SelectedAssets) > 0 {
					assetNames := make([]string, len(filter.SelectedAssets))
					for i, asset := range filter.SelectedAssets {
						assetNames[i] = asset.Name
					}
					infoPrinter.Printf("Running selected assets: %s\n", strings.Join(assetNames, ", "))
				}
				executionStartLog = "Starting the pipeline execution..."
			}

			var connectionManager config.ConnectionAndDetailsGetter
			var errs []error

			secretsBackend := c.String("secrets-backend")
			switch secretsBackend {
			case "vault":
				connectionManager, err = secrets.NewVaultClientFromEnv(logger) //nolint:contextcheck
				if err != nil {
					errs = append(errs, errors.Wrap(err, "failed to initialize vault client"))
				}
			case "doppler":
				connectionManager, err = secrets.NewDopplerClientFromEnv(logger)
				if err != nil {
					errs = append(errs, errors.Wrap(err, "failed to initialize doppler client"))
				}
			case "aws":
				connectionManager, err = secrets.NewAWSSecretsManagerClientFromEnv(logger)
				if err != nil {
					errs = append(errs, errors.Wrap(err, "failed to initialize AWS Secrets Manager client"))
				}
			default:
				connectionManager, errs = connection.NewManagerFromConfigWithContext(ctx, cm)
			}

			if len(errs) > 0 {
				printErrors(errs, runConfig.Output, "Errors occurred while initializing connection manager")
				return cli.Exit("", 1)
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
				} else if len(filter.SelectedAssets) > 0 {
					assetNames := make([]string, len(filter.SelectedAssets))
					for i, asset := range filter.SelectedAssets {
						assetNames[i] = asset.Name
					}
					logFileName = fmt.Sprintf("%s__%s__%s", runID, foundPipeline.Name, strings.Join(assetNames, "_"))
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

			err = ensurePythonCacheGitignore(afero.NewOsFs(), repoRoot.Path)
			if err != nil {
				errorPrinter.Printf("Failed to add Python cache patterns to .gitignore: %v\n", err)
				return cli.Exit("", 1)
			}

			if filter.PushMetaData {
				foundPipeline.MetadataPush.Global = true
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
				if err := ApplyAllFilters(context.Background(), filter, s, foundPipeline); err != nil { //nolint:contextcheck
					errorPrinter.Printf("Failed to filter assets: %v\n", err)
					return cli.Exit("", 1)
				}
			}

			if s.InstanceCountByStatus(scheduler.Pending) == 0 {
				warningPrinter.Println("No tasks to run.")
				return nil
			}

			shouldValidate := !c.Bool("no-validation")
			if err := Validate(shouldValidate, s, CheckLint, runCtx, pipelineInfo.Pipeline, inputPath, logger); err != nil {
				return err
			}

			sendTelemetry(s, c)
			if !c.Bool("minimal-logs") {
				infoPrinter.Printf("\nInterval: %s - %s\n", startDate.Format(time.RFC3339), endDate.Format(time.RFC3339))
				infoPrinter.Printf("\n%s\n\n", executionStartLog)
			}
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
				defer parser.Close()

				go func() {
					err := parser.Start()
					if err != nil {
						printError(err, c.String("output"), "Could not start sql parser")
					}
				}()
			}

			mainExecutors, err := SetupExecutors(s, connectionManager, startDate, endDate, foundPipeline.Name, runID, runConfig.FullRefresh, runConfig.UsePip, runConfig.SensorMode, renderer, parser)
			if err != nil {
				errorPrinter.Println(err.Error())
				return cli.Exit("", 1)
			}
			formatOpts := executor.FormattingOptions{
				DoNotLogTimestamp: c.Bool("no-timestamp"),
				DoNotLogTaskName:  preview.RunningForAnAsset,
				NoColor:           c.Bool("no-color"),
				MinimalLogs:       c.Bool("minimal-logs"),
			}

			ex, err := executor.NewConcurrent(logger, mainExecutors, c.Int("workers"), formatOpts)
			if err != nil {
				errorPrinter.Printf("Failed to create executor: %v\n", err)
				return cli.Exit("", 1)
			}

			// Create a context with timeout
			timeoutDuration := time.Duration(c.Int("timeout")) * time.Second
			timeoutCtx, timeoutCancel := context.WithTimeout(runCtx, timeoutDuration)
			defer timeoutCancel()

			// Combine timeout context with signal handling
			exeCtx, cancel := signal.NotifyContext(timeoutCtx, syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			// Check ADC credentials for BigQuery connections used by pending assets
			// This ensures credentials are available before any tasks begin running
			pendingAssets := getPendingAssets(s)
			if err := bigquery.CheckADCCredentialsForPipeline(runCtx, foundPipeline, pendingAssets, connectionManager); err != nil {
				errorPrinter.Printf("Failed to verify BigQuery ADC credentials: %v\n", err)
				return cli.Exit("", 1)
			}

			ex.Start(exeCtx, s.WorkQueue, s.Results)

			start := time.Now()
			results := s.Run(runCtx)
			duration := time.Since(start)

			if err := s.SavePipelineState(afero.NewOsFs(), runConfig, runID, statePath); err != nil {
				logger.Error("failed to save pipeline state", zap.Error(err))
			}

			errorsInTaskResults := make([]*scheduler.TaskExecutionResult, 0)
			for _, res := range results {
				if res.Error != nil {
					errorsInTaskResults = append(errorsInTaskResults, res)
				}
			}

			if len(errorsInTaskResults) > 0 {
				if singleCheckID != "" {
					printSingleCheckError(errorsInTaskResults[0])
				} else {
					if c.Bool("minimal-logs") {
						summaryPrinter.Printf("\n\nFailed %d tasks in %s\n", len(errorsInTaskResults), duration.Truncate(time.Millisecond).String())
						printErrorsMinimal(errorsInTaskResults)
					} else {
						printExecutionSummary(results, s, duration, len(results))
						printErrorsInResults(errorsInTaskResults, s)
					}
				}
				return cli.Exit("", 1)
			}

			// Print single check success if running a single check
			if singleCheckID != "" && len(results) > 0 {
				printSingleCheckSuccess(results[0])
				return nil
			}

			// Print execution summary (unless minimal-logs is enabled)
			minimalLogs := c.Bool("minimal-logs")
			if minimalLogs {
				summaryPrinter.Printf("\n\nExecuted %d tasks in %s\n", len(results), duration.Truncate(time.Millisecond).String())
			} else {
				printExecutionSummary(results, s, duration, len(results))
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

func GetPipeline(ctx context.Context, inputPath string, runConfig *scheduler.RunConfig, log logger.Logger, opts ...pipeline.CreatePipelineOption) (*PipelineInfo, error) {
	pipelinePath := inputPath
	runningForAnAsset := isPathReferencingAsset(inputPath)
	if runningForAnAsset && runConfig.Tag != "" {
		errorPrinter.Printf("You cannot use the '--tag' flag when running a single asset.\n")
		return nil, errors.New("you cannot use the '--tag' flag when running a single asset")
	}

	var err error
	runDownstreamTasks := false

	if runningForAnAsset {
		pipelinePath, err = path.GetPipelineRootFromTask(inputPath, PipelineDefinitionFiles)
		if err != nil {
			errorPrinter.Printf("Failed to find the pipeline this task belongs to: '%s'\n", inputPath)
			return &PipelineInfo{
				RunningForAnAsset:  runningForAnAsset,
				RunDownstreamTasks: runDownstreamTasks,
			}, err
		}
	}

	opts = append(opts, pipeline.WithMutate())
	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, opts...)
	if err != nil {
		errorPrinter.Println("Failed to build pipeline: ", err.Error())
		errorPrinter.Println("\nHint: You need to run this command with a path to either the pipeline directory or the asset file itself directly.")

		return &PipelineInfo{
			Pipeline:           foundPipeline,
			RunningForAnAsset:  runningForAnAsset,
			RunDownstreamTasks: runDownstreamTasks,
		}, err
	}

	return &PipelineInfo{
		Pipeline:           foundPipeline,
		RunningForAnAsset:  runningForAnAsset,
		RunDownstreamTasks: runConfig.Downstream,
	}, nil
}

func ParseDate(startDateStr, endDateStr string, logger logger.Logger) (time.Time, time.Time, error) {
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
		errorPrinter.Printf("Please give a valid end date: bruin run --end-date <end date>)\n")
		errorPrinter.Printf("A valid end date can be in the YYYY-MM-DD or YYYY-MM-DD HH:MM:SS formats. \n")
		errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
		errorPrinter.Printf("    e.g. %s  \n", time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
		return time.Time{}, time.Time{}, err
	}

	return startDate, endDate, nil
}

func DetermineStartDate(cliStartDate string, pipeline *pipeline.Pipeline, fullRefresh bool, logger logger.Logger) (time.Time, error) {
	var startDate time.Time
	var err error

	// Start date logic
	switch {
	case !fullRefresh:
		startDate, err = date.ParseTime(cliStartDate)
		if err != nil {
			return time.Time{}, err
		}
		logger.Debug("Using CLI start_date: ", cliStartDate)
	case pipeline == nil:
		startDate, err = date.ParseTime(cliStartDate)
		if err != nil {
			return time.Time{}, err
		}
		logger.Debug("Using CLI start_date: ", cliStartDate)
	case pipeline.StartDate == "":
		startDate, err = date.ParseTime(cliStartDate)
		if err != nil {
			return time.Time{}, err
		}
		logger.Debug("Using CLI start_date: ", cliStartDate)
	default:
		startDate, err = date.ParseTime(pipeline.StartDate)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid pipeline start_date '%s': %w", pipeline.StartDate, err)
		}
		logger.Debug("Using pipeline start_date: ", pipeline.StartDate)
	}

	return startDate, nil
}

func ValidateDateRange(startDate, endDate time.Time) error {
	if startDate.After(endDate) {
		errorPrinter.Printf("Start date cannot be after end date. Given start date: %s, end date: %s\n", startDate.Format("2006-01-02 15:04:05"), endDate.Format("2006-01-02 15:04:05"))
		return fmt.Errorf("start date %s is after end date %s", startDate.Format("2006-01-02 15:04:05"), endDate.Format("2006-01-02 15:04:05"))
	}
	return nil
}

func ValidateRunConfig(runConfig *scheduler.RunConfig, inputPath string, logger logger.Logger) (time.Time, time.Time, string, error) {
	if inputPath == "" {
		inputPath = "."
	}

	startDate, endDate, err := ParseDate(runConfig.StartDate, runConfig.EndDate, logger)
	if err != nil {
		return time.Now(), time.Now(), "", err
	}

	return startDate, endDate, inputPath, nil
}

func CheckLint(ctx context.Context, foundPipeline *pipeline.Pipeline, pipelinePath string, logger logger.Logger, validateOnlyAssetLevel bool) error {
	rules, err := lint.GetRules(fs, &git.RepoFinder{}, true, nil, true)
	if err != nil {
		errorPrinter.Printf("An error occurred while linting the pipelines: %v\n", err)
		return err
	}
	rules = append(rules, SeedAssetsValidator)

	rules = lint.FilterRulesBySpeed(rules, true)
	if validateOnlyAssetLevel {
		rules = lint.FilterRulesByLevel(rules, lint.LevelAsset)
	}

	linter := lint.NewLinter(path.GetPipelinePaths, DefaultPipelineBuilder, rules, logger, nil)
	res, err := linter.LintPipelines(ctx, []*pipeline.Pipeline{foundPipeline})
	err = reportLintErrors(res, err, lint.Printer{RootCheckPath: pipelinePath}, "")
	if err != nil {
		return err
	}

	return nil
}

func printErrorsInResults(errorsInTaskResults []*scheduler.TaskExecutionResult, s *scheduler.Scheduler) { // nolint:unparam
	data := make(map[string][]*scheduler.TaskExecutionResult, len(errorsInTaskResults))
	for _, result := range errorsInTaskResults {
		assetName := result.Instance.GetAsset().Name
		data[assetName] = append(data[assetName], result)
	}

	fmt.Println()
	tree := treeprint.NewWithRoot(color.New(color.FgRed).Sprintf("%d assets failed", len(data)))
	for assetName, results := range data {
		assetBranch := tree.AddBranch(color.New(color.FgYellow).Sprint(assetName))

		for _, result := range results {
			switch instance := result.Instance.(type) {
			case *scheduler.ColumnCheckInstance:
				assetBranch.AddNode(fmt.Sprintf("%s.%s - %s",
					color.New(color.FgCyan).Sprint(instance.Column.Name),
					color.New(color.FgMagenta).Sprint(instance.Check.Name),
					color.New(color.FgRed).Sprintf("%s", result.Error)))

			case *scheduler.CustomCheckInstance:
				assetBranch.AddNode(fmt.Sprintf("%s %s - %s",
					color.New(color.FgMagenta).Sprint(instance.Check.Name),
					faint("custom check"),
					color.New(color.FgRed).Sprintf("%s", result.Error)))

			default:
				assetBranch.AddNode(color.New(color.FgRed).Sprintf("%s", result.Error))
			}
		}
	}
	fmt.Println()
	fmt.Println(tree.String())
}

func printErrorsMinimal(errorsInTaskResults []*scheduler.TaskExecutionResult) {
	for _, result := range errorsInTaskResults {
		assetName := result.Instance.GetAsset().Name
		fmt.Printf("\n  %s: %s\n", assetName, result.Error)
	}
}

func printSingleCheckError(result *scheduler.TaskExecutionResult) {
	fmt.Println("\nCheck Failed")
	fmt.Println(strings.Repeat("-", 12))

	checkErr, ok := result.Error.(*ansisql.CheckError) //nolint:errorlint
	if ok {
		fmt.Printf("Error: %s\n\n", checkErr.Message)
		fmt.Printf("Result: %d (expected: %d)\n\n", checkErr.Result, checkErr.Expected)
		fmt.Println("Query:")
		fmt.Println(checkErr.Query + "\n")
	} else {
		fmt.Printf("Error: %s\n", result.Error)
	}
}

func printSingleCheckSuccess(result *scheduler.TaskExecutionResult) {
	fmt.Println("\nCheck Passed")
	fmt.Println(strings.Repeat("-", 12))

	switch instance := result.Instance.(type) {
	case *scheduler.ColumnCheckInstance:
		fmt.Printf("Check: %s.%s\n", instance.Column.Name, instance.Check.Name)
		fmt.Printf("Asset: %s\n\n", instance.GetAsset().Name)
		if instance.ExecutedQuery != "" {
			fmt.Println("Query:")
			fmt.Println(instance.ExecutedQuery)
		}
	case *scheduler.CustomCheckInstance:
		fmt.Printf("Check: %s (custom)\n", instance.Check.Name)
		fmt.Printf("Asset: %s\n\n", instance.GetAsset().Name)
		if instance.ExecutedQuery != "" {
			fmt.Println("Query:")
			fmt.Println(instance.ExecutedQuery)
		}
	}
}

//nolint:maintidx
func SetupExecutors(
	s *scheduler.Scheduler,
	conn config.ConnectionAndDetailsGetter,
	startDate,
	endDate time.Time,
	pipelineName string,
	runID string,
	fullRefresh bool,
	usePipForPython bool,
	sensorMode string,
	renderer *jinja.Renderer,
	parser *sqlparser.SQLParser,
) (map[pipeline.AssetType]executor.Config, error) {
	mainExecutors := executor.DefaultExecutorsV2

	// this is a heuristic we apply to find what might be the most common type of custom check in the pipeline
	// this should go away once we incorporate URIs into the assets
	estimateCustomCheckType := s.FindMajorityOfTypes(pipeline.AssetTypeBigqueryQuery)

	seedOperator, err := ingestr.NewSeedOperator(conn, renderer)
	if err != nil {
		return nil, err
	}

	jinjaVariables := jinja.PythonEnvVariables(&startDate, &endDate, pipelineName, runID, fullRefresh)
	if s.WillRunTaskOfType(pipeline.AssetTypePython) {
		if usePipForPython {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeMain] = python.NewLocalOperator(conn, jinjaVariables)
		} else {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeMain] = python.NewLocalOperatorWithUv(conn, jinjaVariables)
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeR) {
		rOperator, err := r.NewLocalOperator(conn, jinjaVariables)
		if err != nil {
			return nil, err
		}
		mainExecutors[pipeline.AssetTypeR][scheduler.TaskInstanceTypeMain] = rOperator
	}

	wholeFileExtractor := &query.WholeFileExtractor{
		Fs:       fs,
		Renderer: renderer,
	}
	customCheckRunner := ansisql.NewCustomCheckOperator(conn, renderer)
	if s.WillRunTaskOfType(pipeline.AssetTypeBigqueryQuery) || estimateCustomCheckType == pipeline.AssetTypeBigqueryQuery || s.WillRunTaskOfType(pipeline.AssetTypeBigquerySeed) || s.WillRunTaskOfType(pipeline.AssetTypeBigqueryQuerySensor) || s.WillRunTaskOfType(pipeline.AssetTypeBigqueryTableSensor) {
		bqOperator := bigquery.NewBasicOperator(conn, wholeFileExtractor, pipeline.HookWrapperMaterializer{
			Mat: bigquery.NewMaterializer(fullRefresh),
		}, parser)
		bqCheckRunner, err := bigquery.NewColumnCheckOperator(conn)
		if err != nil {
			return nil, err
		}

		metadataPushOperator := bigquery.NewMetadataPushOperator(conn)
		bqQuerySensor := bigquery.NewQuerySensor(conn, wholeFileExtractor, sensorMode)
		bqTableSensor := bigquery.NewTableSensor(conn, sensorMode, wholeFileExtractor)

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
		s.WillRunTaskOfType(pipeline.AssetTypeRedshiftQuery) || estimateCustomCheckType == pipeline.AssetTypeRedshiftQuery ||
		s.WillRunTaskOfType(pipeline.AssetTypeRedshiftSeed) || s.WillRunTaskOfType(pipeline.AssetTypePostgresSeed) ||
		s.WillRunTaskOfType(pipeline.AssetTypePostgresQuerySensor) || s.WillRunTaskOfType(pipeline.AssetTypeRedshiftQuerySensor) || s.WillRunTaskOfType(pipeline.AssetTypePostgresTableSensor) || s.WillRunTaskOfType(pipeline.AssetTypeRedshiftTableSensor) {
		pgCheckRunner := postgres.NewColumnCheckOperator(conn)
		pgOperator := postgres.NewBasicOperator(conn, wholeFileExtractor, pipeline.HookWrapperMaterializer{
			Mat: postgres.NewMaterializer(fullRefresh),
		}, parser)
		pgQuerySensor := ansisql.NewQuerySensor(conn, wholeFileExtractor, sensorMode)
		pgTableSensor := ansisql.NewTableSensor(conn, sensorMode, wholeFileExtractor)
		pgMetadataPushOperator := postgres.NewMetadataPushOperator(conn)
		rsTableSensor := redshift.NewTableSensor(conn, sensorMode, wholeFileExtractor)

		mainExecutors[pipeline.AssetTypeRedshiftQuery][scheduler.TaskInstanceTypeMain] = pgOperator
		mainExecutors[pipeline.AssetTypeRedshiftQuery][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypeRedshiftQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypePostgresQuery][scheduler.TaskInstanceTypeMain] = pgOperator
		mainExecutors[pipeline.AssetTypePostgresQuery][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypePostgresQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		mainExecutors[pipeline.AssetTypePostgresQuery][scheduler.TaskInstanceTypeMetadataPush] = pgMetadataPushOperator

		mainExecutors[pipeline.AssetTypePostgresSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypePostgresSeed][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypePostgresSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		mainExecutors[pipeline.AssetTypePostgresSeed][scheduler.TaskInstanceTypeMetadataPush] = pgMetadataPushOperator

		mainExecutors[pipeline.AssetTypeRedshiftSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeRedshiftSeed][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypeRedshiftSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypePostgresQuerySensor][scheduler.TaskInstanceTypeMain] = pgQuerySensor
		mainExecutors[pipeline.AssetTypePostgresQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypePostgresQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		mainExecutors[pipeline.AssetTypePostgresQuerySensor][scheduler.TaskInstanceTypeMetadataPush] = pgMetadataPushOperator

		mainExecutors[pipeline.AssetTypePostgresTableSensor][scheduler.TaskInstanceTypeMain] = pgTableSensor
		mainExecutors[pipeline.AssetTypePostgresTableSensor][scheduler.TaskInstanceTypeMetadataPush] = pgMetadataPushOperator
		mainExecutors[pipeline.AssetTypePostgresTableSensor][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypePostgresTableSensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeRedshiftQuerySensor][scheduler.TaskInstanceTypeMain] = pgQuerySensor
		mainExecutors[pipeline.AssetTypeRedshiftQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypeRedshiftQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeRedshiftTableSensor][scheduler.TaskInstanceTypeMain] = rsTableSensor
		mainExecutors[pipeline.AssetTypeRedshiftTableSensor][scheduler.TaskInstanceTypeMetadataPush] = pgMetadataPushOperator
		mainExecutors[pipeline.AssetTypeRedshiftTableSensor][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
		mainExecutors[pipeline.AssetTypeRedshiftTableSensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		// we set the Python runners to run the checks on Snowflake assuming that there won't be many usecases where a user has both BQ and Snowflake
		if estimateCustomCheckType == pipeline.AssetTypePostgresQuery || estimateCustomCheckType == pipeline.AssetTypeRedshiftQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = pgCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}
	if s.WillRunTaskOfType(pipeline.AssetTypeTrinoQuery) || estimateCustomCheckType == pipeline.AssetTypeTrinoQuery || s.WillRunTaskOfType(pipeline.AssetTypeTrinoQuerySensor) {
		trinoFileExtractor := &query.FileQuerySplitterExtractor{
			Fs:       fs,
			Renderer: renderer,
		}
		trinoOperator := trino.NewBasicOperator(conn, trinoFileExtractor, pipeline.HookWrapperMaterializer{
			Mat: trino.NewMaterializer(fullRefresh),
		})
		trinoCheckRunner := athena.NewColumnCheckOperator(conn)
		mainExecutors[pipeline.AssetTypeTrinoQuery][scheduler.TaskInstanceTypeMain] = trinoOperator
		mainExecutors[pipeline.AssetTypeTrinoQuery][scheduler.TaskInstanceTypeColumnCheck] = trinoCheckRunner
		mainExecutors[pipeline.AssetTypeTrinoQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		trinoQuerySensor := ansisql.NewQuerySensor(conn, wholeFileExtractor, sensorMode)
		mainExecutors[pipeline.AssetTypeTrinoQuerySensor][scheduler.TaskInstanceTypeMain] = trinoQuerySensor
		mainExecutors[pipeline.AssetTypeTrinoQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = trinoCheckRunner
		mainExecutors[pipeline.AssetTypeTrinoQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
	}
	shouldInitiateSnowflake := s.WillRunTaskOfType(pipeline.AssetTypeSnowflakeQuery) || s.WillRunTaskOfType(pipeline.AssetTypeSnowflakeQuerySensor) || estimateCustomCheckType == pipeline.AssetTypeSnowflakeQuery || s.WillRunTaskOfType(pipeline.AssetTypeSnowflakeSeed) || s.WillRunTaskOfType(pipeline.AssetTypeSnowflakeTableSensor)
	if shouldInitiateSnowflake {
		sfOperator := snowflake.NewBasicOperator(conn, wholeFileExtractor, pipeline.HookWrapperMaterializer{
			Mat: snowflake.NewMaterializer(fullRefresh),
		})

		sfCheckRunner := snowflake.NewColumnCheckOperator(conn)

		sfQuerySensor := snowflake.NewQuerySensor(conn, wholeFileExtractor, 30)
		sfTableSensor := ansisql.NewTableSensor(conn, sensorMode, wholeFileExtractor)

		sfMetadataPushOperator := snowflake.NewMetadataPushOperator(conn)

		mainExecutors[pipeline.AssetTypeSnowflakeQuery][scheduler.TaskInstanceTypeMain] = sfOperator
		mainExecutors[pipeline.AssetTypeSnowflakeQuery][scheduler.TaskInstanceTypeColumnCheck] = sfCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeQuerySensor][scheduler.TaskInstanceTypeMain] = sfQuerySensor
		mainExecutors[pipeline.AssetTypeSnowflakeQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = sfCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeQuery][scheduler.TaskInstanceTypeMetadataPush] = sfMetadataPushOperator

		mainExecutors[pipeline.AssetTypeSnowflakeTableSensor][scheduler.TaskInstanceTypeMain] = sfTableSensor
		mainExecutors[pipeline.AssetTypeSnowflakeTableSensor][scheduler.TaskInstanceTypeMetadataPush] = sfMetadataPushOperator
		mainExecutors[pipeline.AssetTypeSnowflakeTableSensor][scheduler.TaskInstanceTypeColumnCheck] = sfCheckRunner
		mainExecutors[pipeline.AssetTypeSnowflakeTableSensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

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

	//nolint: dupl
	if s.WillRunTaskOfType(pipeline.AssetTypeMsSQLQuery) || estimateCustomCheckType == pipeline.AssetTypeMsSQLQuery ||
		s.WillRunTaskOfType(pipeline.AssetTypeSynapseQuery) || estimateCustomCheckType == pipeline.AssetTypeSynapseQuery ||
		s.WillRunTaskOfType(pipeline.AssetTypeMsSQLSeed) || s.WillRunTaskOfType(pipeline.AssetTypeSynapseSeed) ||
		s.WillRunTaskOfType(pipeline.AssetTypeMsSQLQuerySensor) || s.WillRunTaskOfType(pipeline.AssetTypeSynapseQuerySensor) || s.WillRunTaskOfType(pipeline.AssetTypeMsSQLTableSensor) || s.WillRunTaskOfType(pipeline.AssetTypeSynapseTableSensor) {
		msOperator := mssql.NewBasicOperator(conn, wholeFileExtractor, pipeline.HookWrapperMaterializer{
			Mat: mssql.NewMaterializer(fullRefresh),
		})
		synapseOperator := synapse.NewBasicOperator(conn, wholeFileExtractor, pipeline.HookWrapperMaterializerList{
			Mat: synapse.NewMaterializer(fullRefresh),
		})

		msCheckRunner := mssql.NewColumnCheckOperator(conn)
		synapseCheckRunner := synapse.NewColumnCheckOperator(conn)

		msQuerySensor := ansisql.NewQuerySensor(conn, wholeFileExtractor, sensorMode)
		synapseQuerySensor := ansisql.NewQuerySensor(conn, wholeFileExtractor, sensorMode)

		msTableSensor := ansisql.NewTableSensor(conn, sensorMode, wholeFileExtractor)
		synapseTableSensor := ansisql.NewTableSensor(conn, sensorMode, wholeFileExtractor)

		mainExecutors[pipeline.AssetTypeMsSQLQuery][scheduler.TaskInstanceTypeMain] = msOperator
		mainExecutors[pipeline.AssetTypeMsSQLQuery][scheduler.TaskInstanceTypeColumnCheck] = msCheckRunner
		mainExecutors[pipeline.AssetTypeMsSQLQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeSynapseQuery][scheduler.TaskInstanceTypeMain] = synapseOperator
		mainExecutors[pipeline.AssetTypeSynapseQuery][scheduler.TaskInstanceTypeColumnCheck] = synapseCheckRunner
		mainExecutors[pipeline.AssetTypeSynapseQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeMsSQLSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeMsSQLSeed][scheduler.TaskInstanceTypeColumnCheck] = msCheckRunner
		mainExecutors[pipeline.AssetTypeMsSQLSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeMsSQLQuerySensor][scheduler.TaskInstanceTypeMain] = msQuerySensor
		mainExecutors[pipeline.AssetTypeMsSQLQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = msCheckRunner
		mainExecutors[pipeline.AssetTypeMsSQLQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeMsSQLTableSensor][scheduler.TaskInstanceTypeMain] = msTableSensor
		mainExecutors[pipeline.AssetTypeMsSQLTableSensor][scheduler.TaskInstanceTypeColumnCheck] = msCheckRunner
		mainExecutors[pipeline.AssetTypeMsSQLTableSensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeSynapseSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeSynapseSeed][scheduler.TaskInstanceTypeColumnCheck] = synapseCheckRunner
		mainExecutors[pipeline.AssetTypeSynapseSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeSynapseTableSensor][scheduler.TaskInstanceTypeMain] = synapseTableSensor
		mainExecutors[pipeline.AssetTypeSynapseTableSensor][scheduler.TaskInstanceTypeColumnCheck] = synapseCheckRunner
		mainExecutors[pipeline.AssetTypeSynapseTableSensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeSynapseSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeSynapseSeed][scheduler.TaskInstanceTypeColumnCheck] = synapseCheckRunner
		mainExecutors[pipeline.AssetTypeSynapseSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeSynapseQuerySensor][scheduler.TaskInstanceTypeMain] = synapseQuerySensor
		mainExecutors[pipeline.AssetTypeSynapseQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = synapseCheckRunner
		mainExecutors[pipeline.AssetTypeSynapseQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		// we set the Python runners to run the checks on MsSQL
		if estimateCustomCheckType == pipeline.AssetTypeMsSQLQuery || estimateCustomCheckType == pipeline.AssetTypeSynapseQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = msCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	//nolint: dupl
	if s.WillRunTaskOfType(pipeline.AssetTypeFabricWarehouseQuery) || estimateCustomCheckType == pipeline.AssetTypeFabricWarehouseQuery ||
		s.WillRunTaskOfType(pipeline.AssetTypeFabricWarehouseSeed) ||
		s.WillRunTaskOfType(pipeline.AssetTypeFabricWarehouseQuerySensor) || s.WillRunTaskOfType(pipeline.AssetTypeFabricWarehouseTableSensor) {
		fabricOperator := fabric_warehouse.NewBasicOperator(conn, wholeFileExtractor, pipeline.HookWrapperMaterializer{
			Mat: fabric_warehouse.NewMaterializer(fullRefresh),
		})

		fabricCheckRunner := fabric_warehouse.NewColumnCheckOperator(conn)

		fabricQuerySensor := ansisql.NewQuerySensor(conn, wholeFileExtractor, sensorMode)
		fabricTableSensor := ansisql.NewTableSensor(conn, sensorMode, wholeFileExtractor)

		mainExecutors[pipeline.AssetTypeFabricWarehouseQuery][scheduler.TaskInstanceTypeMain] = fabricOperator
		mainExecutors[pipeline.AssetTypeFabricWarehouseQuery][scheduler.TaskInstanceTypeColumnCheck] = fabricCheckRunner
		mainExecutors[pipeline.AssetTypeFabricWarehouseQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeFabricWarehouseSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeFabricWarehouseSeed][scheduler.TaskInstanceTypeColumnCheck] = fabricCheckRunner
		mainExecutors[pipeline.AssetTypeFabricWarehouseSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeFabricWarehouseQuerySensor][scheduler.TaskInstanceTypeMain] = fabricQuerySensor
		mainExecutors[pipeline.AssetTypeFabricWarehouseQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = fabricCheckRunner
		mainExecutors[pipeline.AssetTypeFabricWarehouseQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeFabricWarehouseTableSensor][scheduler.TaskInstanceTypeMain] = fabricTableSensor
		mainExecutors[pipeline.AssetTypeFabricWarehouseTableSensor][scheduler.TaskInstanceTypeColumnCheck] = fabricCheckRunner
		mainExecutors[pipeline.AssetTypeFabricWarehouseTableSensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		if estimateCustomCheckType == pipeline.AssetTypeFabricWarehouseQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = fabricCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	//nolint:dupl
	if s.WillRunTaskOfType(pipeline.AssetTypeDatabricksQuery) || estimateCustomCheckType == pipeline.AssetTypeDatabricksQuery ||
		s.WillRunTaskOfType(pipeline.AssetTypeDatabricksSeed) || s.WillRunTaskOfType(pipeline.AssetTypeDatabricksQuerySensor) || s.WillRunTaskOfType(pipeline.AssetTypeDatabricksTableSensor) {
		databricksOperator := databricks.NewBasicOperator(conn, wholeFileExtractor, pipeline.HookWrapperMaterializerList{
			Mat: databricks.NewMaterializer(fullRefresh),
		})
		databricksCheckRunner := databricks.NewColumnCheckOperator(conn)
		databricksQuerySensor := ansisql.NewQuerySensor(conn, wholeFileExtractor, sensorMode)
		databricksTableSensor := ansisql.NewTableSensor(conn, sensorMode, wholeFileExtractor)

		mainExecutors[pipeline.AssetTypeDatabricksQuery][scheduler.TaskInstanceTypeMain] = databricksOperator
		mainExecutors[pipeline.AssetTypeDatabricksQuery][scheduler.TaskInstanceTypeColumnCheck] = databricksCheckRunner
		mainExecutors[pipeline.AssetTypeDatabricksQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeDatabricksSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeDatabricksSeed][scheduler.TaskInstanceTypeColumnCheck] = databricksCheckRunner
		mainExecutors[pipeline.AssetTypeDatabricksSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeDatabricksQuerySensor][scheduler.TaskInstanceTypeMain] = databricksQuerySensor
		mainExecutors[pipeline.AssetTypeDatabricksQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = databricksCheckRunner
		mainExecutors[pipeline.AssetTypeDatabricksQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeDatabricksTableSensor][scheduler.TaskInstanceTypeMain] = databricksTableSensor
		mainExecutors[pipeline.AssetTypeDatabricksQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = databricksCheckRunner
		mainExecutors[pipeline.AssetTypeDatabricksQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		// we set the Python runners to run the checks on MsSQL
		if estimateCustomCheckType == pipeline.AssetTypeDatabricksQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = databricksOperator
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeIngestr) || estimateCustomCheckType == pipeline.AssetTypeIngestr {
		ingestrOperator, err := ingestr.NewBasicOperator(conn, renderer)
		if err != nil {
			return nil, err
		}
		ingestrCheckRunner := ingestr.NewColumnCheckOperator(&mainExecutors)
		ingestrCustomCheckRunner := ingestr.NewCustomCheckOperator(&mainExecutors)

		mainExecutors[pipeline.AssetTypeIngestr][scheduler.TaskInstanceTypeMain] = ingestrOperator
		mainExecutors[pipeline.AssetTypeIngestr][scheduler.TaskInstanceTypeColumnCheck] = ingestrCheckRunner
		mainExecutors[pipeline.AssetTypeIngestr][scheduler.TaskInstanceTypeCustomCheck] = ingestrCustomCheckRunner
	}

	//nolint:dupl
	if s.WillRunTaskOfType(pipeline.AssetTypeAthenaQuery) || estimateCustomCheckType == pipeline.AssetTypeAthenaQuery || s.WillRunTaskOfType(pipeline.AssetTypeAthenaSeed) {
		athenaOperator := athena.NewBasicOperator(conn, wholeFileExtractor, pipeline.HookWrapperMaterializerListWithLocation{
			Mat: athena.NewMaterializer(fullRefresh),
		})
		athenaCheckRunner := athena.NewColumnCheckOperator(conn)
		athenaTableSensor := ansisql.NewTableSensor(conn, sensorMode, wholeFileExtractor)

		mainExecutors[pipeline.AssetTypeAthenaQuery][scheduler.TaskInstanceTypeMain] = athenaOperator
		mainExecutors[pipeline.AssetTypeAthenaQuery][scheduler.TaskInstanceTypeColumnCheck] = athenaCheckRunner
		mainExecutors[pipeline.AssetTypeAthenaQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeAthenaSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeAthenaSeed][scheduler.TaskInstanceTypeColumnCheck] = athenaCheckRunner
		mainExecutors[pipeline.AssetTypeAthenaSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeAthenaTableSensor][scheduler.TaskInstanceTypeMain] = athenaTableSensor
		mainExecutors[pipeline.AssetTypeAthenaTableSensor][scheduler.TaskInstanceTypeColumnCheck] = athenaCheckRunner
		mainExecutors[pipeline.AssetTypeAthenaTableSensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		if estimateCustomCheckType == pipeline.AssetTypeAthenaQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = athenaCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	//nolint:dupl
	if s.WillRunTaskOfType(pipeline.AssetTypeDuckDBQuery) || estimateCustomCheckType == pipeline.AssetTypeDuckDBQuery ||
		s.WillRunTaskOfType(pipeline.AssetTypeDuckDBSeed) || s.WillRunTaskOfType(pipeline.AssetTypeDuckDBQuerySensor) {
		duckDBOperator := duck.NewBasicOperator(conn, wholeFileExtractor, pipeline.HookWrapperMaterializer{
			Mat: duck.NewMaterializer(fullRefresh),
		})
		duckDBCheckRunner := duck.NewColumnCheckOperator(conn)
		duckDBQuerySensor := ansisql.NewQuerySensor(conn, wholeFileExtractor, sensorMode)

		mainExecutors[pipeline.AssetTypeDuckDBQuery][scheduler.TaskInstanceTypeMain] = duckDBOperator
		mainExecutors[pipeline.AssetTypeDuckDBQuery][scheduler.TaskInstanceTypeColumnCheck] = duckDBCheckRunner
		mainExecutors[pipeline.AssetTypeDuckDBQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeDuckDBSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeDuckDBSeed][scheduler.TaskInstanceTypeColumnCheck] = duckDBCheckRunner
		mainExecutors[pipeline.AssetTypeDuckDBSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeDuckDBQuerySensor][scheduler.TaskInstanceTypeMain] = duckDBQuerySensor
		mainExecutors[pipeline.AssetTypeDuckDBQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = duckDBCheckRunner
		mainExecutors[pipeline.AssetTypeDuckDBQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		if estimateCustomCheckType == pipeline.AssetTypeDuckDBQuery {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = duckDBCheckRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	//nolint:dupl
	if s.WillRunTaskOfType(pipeline.AssetTypeClickHouse) || estimateCustomCheckType == pipeline.AssetTypeClickHouse ||
		s.WillRunTaskOfType(pipeline.AssetTypeClickHouseSeed) || s.WillRunTaskOfType(pipeline.AssetTypeClickHouseQuerySensor) || s.WillRunTaskOfType(pipeline.AssetTypeClickHouseTableSensor) {
		clickHouseOperator := clickhouse.NewBasicOperator(conn, wholeFileExtractor, pipeline.HookWrapperMaterializerList{
			Mat: clickhouse.NewMaterializer(fullRefresh),
		})
		checkRunner := clickhouse.NewColumnCheckOperator(conn)
		clickHouseQuerySensor := ansisql.NewQuerySensor(conn, wholeFileExtractor, sensorMode)
		clickHouseTableSensor := ansisql.NewTableSensor(conn, sensorMode, wholeFileExtractor)

		mainExecutors[pipeline.AssetTypeClickHouse][scheduler.TaskInstanceTypeMain] = clickHouseOperator
		mainExecutors[pipeline.AssetTypeClickHouse][scheduler.TaskInstanceTypeColumnCheck] = checkRunner
		mainExecutors[pipeline.AssetTypeClickHouse][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeClickHouseSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeClickHouseSeed][scheduler.TaskInstanceTypeColumnCheck] = checkRunner
		mainExecutors[pipeline.AssetTypeClickHouseSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeClickHouseQuerySensor][scheduler.TaskInstanceTypeMain] = clickHouseQuerySensor
		mainExecutors[pipeline.AssetTypeClickHouseQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = checkRunner
		mainExecutors[pipeline.AssetTypeClickHouseQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeClickHouseTableSensor][scheduler.TaskInstanceTypeMain] = clickHouseTableSensor
		mainExecutors[pipeline.AssetTypeClickHouseTableSensor][scheduler.TaskInstanceTypeColumnCheck] = checkRunner
		mainExecutors[pipeline.AssetTypeClickHouseTableSensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		if estimateCustomCheckType == pipeline.AssetTypeClickHouse {
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = checkRunner
			mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeTableauDatasource) {
		tableauOperator := tableau.NewBasicOperator(conn)
		mainExecutors[pipeline.AssetTypeTableauDatasource][scheduler.TaskInstanceTypeMain] = tableauOperator
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeTableauWorkbook) {
		tableauOperator := tableau.NewBasicOperator(conn)
		mainExecutors[pipeline.AssetTypeTableauWorkbook][scheduler.TaskInstanceTypeMain] = tableauOperator
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeTableau) {
		tableauOperator := tableau.NewBasicOperator(conn)
		mainExecutors[pipeline.AssetTypeTableau][scheduler.TaskInstanceTypeMain] = tableauOperator
	}

	emrServerlessAssetTypes := []pipeline.AssetType{
		pipeline.AssetTypeEMRServerlessSpark,
		pipeline.AssetTypeEMRServerlessPyspark,
	}

	for _, typ := range emrServerlessAssetTypes {
		if s.WillRunTaskOfType(typ) {
			emrServerlessOperator, err := emr_serverless.NewBasicOperator(conn, jinjaVariables)
			emrCheckRunner := emr_serverless.NewColumnCheckOperator(conn)
			emrCustomCheckRunner := emr_serverless.NewCustomCheckOperator(conn, renderer)
			if err != nil {
				return nil, err
			}
			mainExecutors[typ][scheduler.TaskInstanceTypeMain] = emrServerlessOperator
			mainExecutors[typ][scheduler.TaskInstanceTypeColumnCheck] = emrCheckRunner
			mainExecutors[typ][scheduler.TaskInstanceTypeCustomCheck] = emrCustomCheckRunner
		}
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeDataprocServerlessPyspark) {
		dataprocServerlessOperator, err := dataprocserverless.NewBasicOperator(conn, jinjaVariables)
		if err != nil {
			return nil, err
		}
		mainExecutors[pipeline.AssetTypeDataprocServerlessPyspark][scheduler.TaskInstanceTypeMain] = dataprocServerlessOperator
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeS3KeySensor) {
		s3KeySensor := s3.NewKeySensor(conn, sensorMode)
		mainExecutors[pipeline.AssetTypeS3KeySensor][scheduler.TaskInstanceTypeMain] = s3KeySensor
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeMySQLQuery) ||
		estimateCustomCheckType == pipeline.AssetTypeMySQLQuery ||
		s.WillRunTaskOfType(pipeline.AssetTypeMySQLSeed) ||
		s.WillRunTaskOfType(pipeline.AssetTypeMySQLQuerySensor) ||
		s.WillRunTaskOfType(pipeline.AssetTypeMySQLTableSensor) {
		mysqlOperator := mysql.NewBasicOperator(conn, wholeFileExtractor, pipeline.HookWrapperMaterializer{
			Mat: mysql.NewMaterializer(fullRefresh),
		}, parser)
		mysqlCheckRunner := mysql.NewColumnCheckOperator(conn)
		mysqlQuerySensor := ansisql.NewQuerySensor(conn, wholeFileExtractor, sensorMode)
		mysqlTableSensor := ansisql.NewTableSensor(conn, sensorMode, wholeFileExtractor)

		mainExecutors[pipeline.AssetTypeMySQLQuery][scheduler.TaskInstanceTypeMain] = mysqlOperator
		mainExecutors[pipeline.AssetTypeMySQLQuery][scheduler.TaskInstanceTypeColumnCheck] = mysqlCheckRunner
		mainExecutors[pipeline.AssetTypeMySQLQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeMySQLSeed][scheduler.TaskInstanceTypeMain] = seedOperator
		mainExecutors[pipeline.AssetTypeMySQLSeed][scheduler.TaskInstanceTypeColumnCheck] = mysqlCheckRunner
		mainExecutors[pipeline.AssetTypeMySQLSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeMySQLQuerySensor][scheduler.TaskInstanceTypeMain] = mysqlQuerySensor
		mainExecutors[pipeline.AssetTypeMySQLQuerySensor][scheduler.TaskInstanceTypeColumnCheck] = mysqlCheckRunner
		mainExecutors[pipeline.AssetTypeMySQLQuerySensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner

		mainExecutors[pipeline.AssetTypeMySQLTableSensor][scheduler.TaskInstanceTypeMain] = mysqlTableSensor
		mainExecutors[pipeline.AssetTypeMySQLTableSensor][scheduler.TaskInstanceTypeColumnCheck] = mysqlCheckRunner
		mainExecutors[pipeline.AssetTypeMySQLTableSensor][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeAgentClaudeCode) {
		claudeCodeOperator := claudecode.NewClaudeCodeOperator(renderer)
		mainExecutors[pipeline.AssetTypeAgentClaudeCode][scheduler.TaskInstanceTypeMain] = claudeCodeOperator
	}

	return mainExecutors, nil
}

func isPathReferencingAsset(p string) bool {
	// Check if the path matches any of the pipeline definition file names
	for _, pipelineDefinitionfile := range PipelineDefinitionFiles {
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

func sendTelemetry(s *scheduler.Scheduler, c *cli.Command) {
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
	SingleTask        *pipeline.Asset   // Single asset (from running asset file directly)
	SelectedAssets    []*pipeline.Asset // Multiple assets specified as positional arguments
	ExcludeTag        string
	singleCheckID     scheduler.CheckUniqueID // ID of the single check to run, if any
}

func SkipAllTasksIfSingleCheck(ctx context.Context, f *Filter, s *scheduler.Scheduler, p *pipeline.Pipeline) error {
	if f.singleCheckID.ID == "" {
		return nil
	}
	s.MarkAll(scheduler.Skipped)
	err := s.MarkCheckInstancesByID(f.singleCheckID.ID, f.singleCheckID.Asset, scheduler.Pending)
	if err != nil {
		return err
	}
	return nil
}

func HandleMultipleAssets(ctx context.Context, f *Filter, s *scheduler.Scheduler, p *pipeline.Pipeline) error {
	if len(f.SelectedAssets) == 0 {
		return nil
	}

	// Cannot use with single asset mode
	if f.SingleTask != nil {
		return errors.New("cannot specify multiple assets when running a single asset file directly")
	}

	// Cannot use with tags
	if f.IncludeTag != "" {
		return errors.New("cannot specify assets with --tag flag")
	}

	// Mark all as skipped first
	s.MarkAll(scheduler.Skipped)

	// Mark selected assets as pending
	for _, asset := range f.SelectedAssets {
		s.MarkAsset(asset, scheduler.Pending, f.IncludeDownstream)
	}

	// Handle exclude-tag if specified
	if f.ExcludeTag != "" {
		if !f.IncludeDownstream {
			return errors.New("when specifying assets with --exclude-tag, you must also use --downstream flag")
		}
		excludedAssets := p.GetAssetsByTag(f.ExcludeTag)
		if len(excludedAssets) == 0 {
			return fmt.Errorf("no assets found with exclude tag '%s'", f.ExcludeTag)
		}
		s.MarkByTag(f.ExcludeTag, scheduler.Skipped, false)
	}

	return nil
}

func HandleSingleTask(ctx context.Context, f *Filter, s *scheduler.Scheduler, p *pipeline.Pipeline) error {
	if f.SingleTask == nil {
		// Allow downstream with selected assets, but not for whole pipeline
		if f.IncludeDownstream && len(f.SelectedAssets) == 0 {
			return errors.New("cannot use the --downstream flag when running the whole pipeline")
		}
		return nil
	}

	// Cannot use with multiple assets
	if len(f.SelectedAssets) > 0 {
		return errors.New("cannot specify multiple assets when running a single asset file directly")
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

	// Cannot use with multiple assets
	if len(f.SelectedAssets) > 0 {
		return errors.New("cannot use --tag flag when specifying assets")
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
		HandleMultipleAssets, // Check for multiple assets first
		HandleSingleTask,     // Then single asset
		HandleIncludeTags,    // Then tags
		HandleExcludeTags,
		FilterTaskTypes,
		SkipAllTasksIfSingleCheck,
	}

	for _, filterFunc := range funcs {
		if err := filterFunc(ctx, f, s, p); err != nil {
			return err
		}
	}
	return nil
}

type assetCounter interface {
	GetAssetCountWithTasksPending() int
}

type lintChecker func(ctx context.Context, foundPipeline *pipeline.Pipeline, pipelinePath string, logger logger.Logger, validateOnlyAssetLevel bool) error

func Validate(shouldvalidate bool, assetCounter assetCounter, lintChecker lintChecker, ctx context.Context, foundPipeline *pipeline.Pipeline, pipelinePath string, logger logger.Logger) error {
	if !shouldvalidate {
		return nil
	}

	if assetCounter.GetAssetCountWithTasksPending() > 0 {
		return lintChecker(ctx, foundPipeline, pipelinePath, logger, false)
	}

	return lintChecker(ctx, foundPipeline, pipelinePath, logger, true)
}

// getPendingAssets extracts unique assets from pending task instances in the scheduler.
func getPendingAssets(s *scheduler.Scheduler) []*pipeline.Asset {
	pendingInstances := s.GetTaskInstancesByStatus(scheduler.Pending)
	seen := make(map[string]bool)
	assets := make([]*pipeline.Asset, 0)

	for _, instance := range pendingInstances {
		asset := instance.GetAsset()
		if seen[asset.Name] {
			continue
		}
		seen[asset.Name] = true
		assets = append(assets, asset)
	}

	return assets
}

func ensurePythonCacheGitignore(fs afero.Fs, repoRoot string) error {
	for _, pattern := range pythonCacheGitignorePatterns {
		if err := git.EnsureGivenPatternIsInGitignore(fs, repoRoot, pattern); err != nil {
			return fmt.Errorf("failed to add %s: %w", pattern, err)
		}
	}
	return nil
}

// loadAssetsFromPaths loads assets from a list of paths or names.
// It tries to find assets by path first, then by name if path lookup fails.
// All assets must belong to the same pipeline.
func loadAssetsFromPaths(assetPaths []string, p *pipeline.Pipeline, repoRoot, cwd string) ([]*pipeline.Asset, error) {
	assets := make([]*pipeline.Asset, 0, len(assetPaths))
	notFound := make([]string, 0)

	// Get pipeline directory for path resolution
	pipelineDir := filepath.Dir(p.DefinitionFile.Path)

	for _, assetPath := range assetPaths {
		if assetPath == "" {
			continue
		}

		var asset *pipeline.Asset

		// Try multiple base paths for relative paths
		basePaths := []string{cwd, repoRoot, pipelineDir}

		// First, try as absolute path
		if filepath.IsAbs(assetPath) {
			absPath, err := filepath.Abs(assetPath)
			if err == nil {
				asset = p.GetAssetByPath(absPath)
				if asset == nil {
					asset = findAssetByExecutablePath(p, absPath)
				}
			}
		}

		// If not found, try relative to each base path
		if asset == nil && !filepath.IsAbs(assetPath) {
			for _, base := range basePaths {
				relativePath := filepath.Join(base, assetPath)
				absPath, err := filepath.Abs(relativePath)
				if err == nil {
					asset = p.GetAssetByPath(absPath)
					if asset == nil {
						asset = findAssetByExecutablePath(p, absPath)
					}
					if asset != nil {
						break
					}
				}
			}
		}

		// If still not found, try by asset name
		if asset == nil {
			// Try with the path as-is (might be just the name)
			asset = p.GetAssetByName(assetPath)
		}

		// Try without file extension
		if asset == nil {
			nameWithoutExt := strings.TrimSuffix(filepath.Base(assetPath), filepath.Ext(assetPath))
			asset = p.GetAssetByName(nameWithoutExt)
		}

		// Try case-insensitive name lookup
		if asset == nil {
			asset = p.GetAssetByNameCaseInsensitive(assetPath)
		}

		// Try case-insensitive without extension
		if asset == nil {
			nameWithoutExt := strings.TrimSuffix(filepath.Base(assetPath), filepath.Ext(assetPath))
			asset = p.GetAssetByNameCaseInsensitive(nameWithoutExt)
		}

		if asset == nil {
			notFound = append(notFound, assetPath)
			continue
		}

		assets = append(assets, asset)
	}

	if len(notFound) > 0 {
		return nil, fmt.Errorf("assets not found: %s. Make sure the asset paths or names are correct and belong to the pipeline", strings.Join(notFound, ", "))
	}

	if len(assets) == 0 {
		return nil, errors.New("no valid assets found")
	}

	return assets, nil
}

// findAssetByExecutablePath finds an asset by its executable file path.
func findAssetByExecutablePath(p *pipeline.Pipeline, absPath string) *pipeline.Asset {
	absPath, err := filepath.Abs(absPath)
	if err != nil {
		return nil
	}

	for _, asset := range p.Assets {
		if asset.ExecutableFile.Path == absPath {
			return asset
		}
	}

	return nil
}
