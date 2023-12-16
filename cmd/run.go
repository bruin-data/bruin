package cmd

import (
	"context"
	"fmt"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"io"
	"log"
	"os"
	path2 "path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/date"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/fatih/color"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

const LogsFolder = "logs"

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
				Value: 8,
			},
			&cli.StringFlag{
				Name:        "start-date",
				Usage:       "the start date of the range the pipeline will run for in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format",
				DefaultText: fmt.Sprintf("yesterday, e.g. %s", time.Now().AddDate(0, 0, -1).Format("2006-01-02")),
				Value:       time.Now().AddDate(0, 0, -1).Format("2006-01-02"),
			},
			&cli.StringFlag{
				Name:        "end-date",
				Usage:       "the end date of the range the pipeline will run for in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format",
				DefaultText: fmt.Sprintf("today, e.g. %s", time.Now().Format("2006-01-02")),
				Value:       time.Now().Format("2006-01-02"),
			},
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"e", "env"},
				Usage:   "the environment to use",
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

			if !c.Bool("no-log-file") {
				runID := time.Now().Format("2006_01_02_15_04_05")
				logPath, err := filepath.Abs(fmt.Sprintf("%s/%s/%s.log", repoRoot.Path, LogsFolder, runID))
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

				err = git.EnsureGivenPatternIsInGitignore(afero.NewOsFs(), repoRoot.Path, fmt.Sprintf("%s/*.log", LogsFolder))
				if err != nil {
					errorPrinter.Printf("Failed to add the log file to .gitignore: %v\n", err)
					return cli.Exit("", 1)
				}
			}

			runningForAnAsset := isPathReferencingAsset(inputPath)
			var task *pipeline.Asset

			runDownstreamTasks := false
			if runningForAnAsset {
				task, err = builder.CreateAssetFromFile(inputPath)
				if err != nil {
					errorPrinter.Printf("Failed to build task: %v\n", err.Error())
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

			cm, err := config.LoadOrCreate(afero.NewOsFs(), path2.Join(repoRoot.Path, ".bruin.yml"))
			if err != nil {
				errorPrinter.Printf("Failed to load the config file: %v\n", err)
				return cli.Exit("", 1)
			}

			err = switchEnvironment(c, cm, os.Stdin)
			if err != nil {
				return err
			}

			connectionManager, err := connection.NewManagerFromConfig(cm)
			if err != nil {
				errorPrinter.Printf("Failed to register connections: %v\n", err)
				return cli.Exit("", 1)
			}

			foundPipeline, err := builder.CreatePipelineFromPath(pipelinePath)
			if err != nil {
				errorPrinter.Println("failed to build pipeline, are you sure you have referred the right path?")
				errorPrinter.Println("\nHint: You need to run this command with a path to either the pipeline directory or the asset file itself directly.")

				return cli.Exit("", 1)
			}

			infoPrinter.Printf("Analyzed the pipeline '%s' with %d assets.\n", foundPipeline.Name, len(foundPipeline.Assets))

			if !runningForAnAsset {
				rules, err := lint.GetRules(logger, fs)
				if err != nil {
					errorPrinter.Printf("An error occurred while linting the pipelines: %v\n", err)
					return cli.Exit("", 1)
				}

				linter := lint.NewLinter(path.GetPipelinePaths, builder, rules, logger)
				res, err := linter.LintPipelines([]*pipeline.Pipeline{foundPipeline})
				err = reportLintErrors(res, err, lint.Printer{RootCheckPath: pipelinePath})
				if err != nil {
					return cli.Exit("", 1)
				}
			} else {
				infoPrinter.Printf("Running only the asset '%s'\n", task.Name)
			}

			s := scheduler.NewScheduler(logger, foundPipeline)

			infoPrinter.Printf("\nStarting the pipeline execution...\n\n")

			if task != nil {
				logger.Debug("marking single task to run: ", task.Name)
				s.MarkAll(scheduler.Succeeded)
				s.MarkTask(task, scheduler.Pending, runDownstreamTasks)
			}

			mainExecutors, err := setupExecutors(s, cm, connectionManager, startDate, endDate)
			if err != nil {
				errorPrinter.Printf(err.Error())
				return cli.Exit("", 1)
			}

			ex, err := executor.NewConcurrent(logger, mainExecutors, c.Int("workers"))
			if err != nil {
				errorPrinter.Printf("Failed to create executor: %v\n", err)
				return cli.Exit("", 1)
			}

			ex.Start(s.WorkQueue, s.Results)

			start := time.Now()
			results := s.Run(context.Background())
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
			}

			return nil
		},
	}
}

func printErrorsInResults(errorsInTaskResults []*scheduler.TaskExecutionResult, s *scheduler.Scheduler) {
	errorPrinter.Printf("\nFailed tasks: %d\n", len(errorsInTaskResults))
	for _, t := range errorsInTaskResults {
		errorPrinter.Printf("  - %s\n", t.Instance.GetAsset().Name)
		errorPrinter.Printf("    └── %s\n\n", t.Error.Error())
	}

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

func setupExecutors(s *scheduler.Scheduler, config *config.Config, conn *connection.Manager, startDate, endDate time.Time) (map[pipeline.AssetType]executor.Config, error) {
	mainExecutors := executor.DefaultExecutorsV2
	if s.WillRunTaskOfType(pipeline.AssetTypePython) {
		mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeMain] = python.NewLocalOperator(config, map[string]string{})
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeBigqueryQuery) {
		wholeFileExtractor := &query.WholeFileExtractor{
			Fs:       fs,
			Renderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate),
		}

		bqOperator := bigquery.NewBasicOperator(conn, wholeFileExtractor, bigquery.Materializer{})

		bqTestRunner, err := bigquery.NewColumnCheckOperator(conn)
		if err != nil {
			return nil, err
		}

		mainExecutors[pipeline.AssetTypeBigqueryQuery][scheduler.TaskInstanceTypeMain] = bqOperator
		mainExecutors[pipeline.AssetTypeBigqueryQuery][scheduler.TaskInstanceTypeColumnCheck] = bqTestRunner

		// we set the Python runners to run the checks on BigQuery assuming that there won't be many usecases where a user has both BQ and Snowflake
		mainExecutors[pipeline.AssetTypePython][scheduler.TaskInstanceTypeColumnCheck] = bqTestRunner
	}

	if s.WillRunTaskOfType(pipeline.AssetTypeSnowflakeQuery) {
		wholeFileExtractor := &query.WholeFileExtractor{
			Fs:       fs,
			Renderer: jinja.NewRendererWithStartEndDates(&startDate, &endDate),
		}

		sfOperator := snowflake.NewBasicOperator(conn, wholeFileExtractor, snowflake.Materializer{})

		// sfTestRunner, err := snowflake.NewColumnCheckOperator(conn)
		// if err != nil {
		// 	return nil, err
		// }

		mainExecutors[pipeline.AssetTypeSnowflakeQuery][scheduler.TaskInstanceTypeMain] = sfOperator
		// mainExecutors[pipeline.AssetTypeSnowflakeQuery][scheduler.TaskInstanceTypeColumnCheck] = sfTestRunner
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
