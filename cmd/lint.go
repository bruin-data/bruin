package cmd

import (
	"fmt"
	"os"
	path2 "path"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func Lint(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "validate",
		Usage:     "validate the bruin pipeline configuration for all the pipelines in a given directory",
		ArgsUsage: "[path to pipelines]",
		Flags: []cli.Flag{
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
		},
		Action: func(c *cli.Context) error {
			fmt.Println()

			logger := makeLogger(*isDebug)

			rootPath := c.Args().Get(0)
			if rootPath == "" {
				rootPath = "."
			}

			logger.Debugf("using root path '%s'", rootPath)
			repoRoot, err := git.FindRepoFromPath(rootPath)
			if err != nil {
				errorPrinter.Printf("Failed to find the git repository root: %v\n", err)
				return cli.Exit("", 1)
			}
			logger.Debugf("found repo root '%s'", repoRoot.Path)

			cm, err := config.LoadOrCreate(afero.NewOsFs(), path2.Join(repoRoot.Path, ".bruin.yml"))
			if err != nil {
				errorPrinter.Printf("Failed to load the config file: %v\n", err)
				return cli.Exit("", 1)
			}

			logger.Debugf("loaded the config")

			err = switchEnvironment(c, cm, os.Stdin)
			if err != nil {
				return err
			}

			logger.Debugf("switched to the environment '%s'", cm.SelectedEnvironmentName)

			connectionManager, err := connection.NewManagerFromConfig(cm)
			if err != nil {
				errorPrinter.Printf("Failed to register connections: %v\n", err)
				return cli.Exit("", 1)
			}

			logger.Debugf("built the connection manager instance")

			rules, err := lint.GetRules(fs)
			if err != nil {
				errorPrinter.Printf("An error occurred while building the validation rules: %v\n", err)
				return cli.Exit("", 1)
			}

			logger.Debugf("successfully loaded %d rules", len(rules))

			yesterday := time.Now().AddDate(0, 0, -1)

			startDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
			endDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 0, time.UTC)
			renderer := jinja.NewRendererWithStartEndDates(&startDate, &endDate)

			if len(cm.SelectedEnvironment.Connections.GoogleCloudPlatform) > 0 {
				rules = append(rules, &lint.QueryValidatorRule{
					Identifier:  "bigquery-validator",
					TaskType:    pipeline.AssetTypeBigqueryQuery,
					Connections: connectionManager,
					Extractor: &query.WholeFileExtractor{
						Fs:       fs,
						Renderer: renderer,
					},
					WorkerCount: 32,
					Logger:      logger,
				})
			} else {
				logger.Debug("no GCP connections found, skipping BigQuery validation")
			}

			if len(cm.SelectedEnvironment.Connections.Snowflake) > 0 {
				rules = append(rules, &lint.QueryValidatorRule{
					Identifier:  "snowflake-validator",
					TaskType:    pipeline.AssetTypeSnowflakeQuery,
					Connections: connectionManager,
					Extractor: &query.WholeFileExtractor{
						Fs:       fs,
						Renderer: renderer,
					},
					WorkerCount: 32,
					Logger:      logger,
				})
			} else {
				logger.Debug("no Snowflake connections found, skipping Snowflake validation")
			}

			linter := lint.NewLinter(path.GetPipelinePaths, builder, rules, logger)

			infoPrinter.Printf("Validating pipelines in '%s' for '%s' environment...\n", rootPath, cm.SelectedEnvironmentName)
			result, err := linter.Lint(rootPath, pipelineDefinitionFile)

			printer := lint.Printer{RootCheckPath: rootPath}
			err = reportLintErrors(result, err, printer)
			if err != nil {
				return cli.Exit("", 1)
			}
			return nil
		},
	}
}

func reportLintErrors(result *lint.PipelineAnalysisResult, err error, printer lint.Printer) error {
	if err != nil {
		errorPrinter.Println("\nAn error occurred while linting the pipelines:")

		errorList := unwrapAllErrors(err)
		for i, e := range errorList {
			errorPrinter.Printf("%s└── %s\n", strings.Repeat("  ", i), e)
		}

		return err
	}

	printer.PrintIssues(result)

	// prepare the final message
	errorCount := result.ErrorCount()
	pipelineCount := len(result.Pipelines)
	pipelineStr := "pipeline"
	if pipelineCount > 1 {
		pipelineStr += "s"
	}

	if errorCount > 0 {
		issueStr := "issue"
		if errorCount > 1 {
			issueStr += "s"
		}

		errorPrinter.Printf("\n✘ Checked %d %s and found %d %s, please check above.\n", pipelineCount, pipelineStr, errorCount, issueStr)
		return errors.New("validation failed")
	}

	taskCount := 0
	for _, p := range result.Pipelines {
		taskCount += len(p.Pipeline.Assets)
	}

	successPrinter.Printf("\n✓ Successfully validated %d tasks across %d %s, all good.\n", taskCount, pipelineCount, pipelineStr)
	return nil
}

func unwrapAllErrors(err error) []string {
	if err == nil {
		return []string{}
	}

	errorItems := flattenErrors(err)
	count := len(errorItems)
	if count < 2 {
		return errorItems
	}

	cleanErrors := make([]string, count)
	cleanErrors[count-1] = errorItems[0]
	for i := range errorItems {
		if i == count-1 {
			break
		}

		rev := count - i - 1
		item := errorItems[rev]

		cleanMessage := strings.ReplaceAll(item, ": "+errorItems[rev-1], "")
		cleanErrors[i] = cleanMessage
	}

	return cleanErrors
}

func flattenErrors(err error) []string {
	if err == nil {
		return []string{}
	}

	unwrapped := errors.Unwrap(err)
	if unwrapped == nil {
		return []string{err.Error()}
	}

	for unwrapped != nil && err.Error() == unwrapped.Error() {
		unwrapped = errors.Unwrap(unwrapped)
	}

	var foundErrors []string
	allErrors := flattenErrors(unwrapped)
	foundErrors = append(foundErrors, allErrors...)
	foundErrors = append(foundErrors, err.Error())

	return foundErrors
}
