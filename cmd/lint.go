package cmd

import (
	"fmt"
	"io"
	"os"
	path2 "path"
	"strings"

	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/fatih/color"
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
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
			&cli.BoolFlag{
				Name:  "exclude-warnings",
				Usage: "exclude warning validations from the output",
			},
		},
		Action: func(c *cli.Context) error {
			// if the output is JSON then we intend to discard all the nicer pretty-print statements
			// and only print the JSON output directly to the stdout
			if c.String("output") == "json" {
				color.Output = io.Discard
			} else {
				fmt.Println()
			}

			logger := makeLogger(*isDebug)

			repoOrAsset := c.Args().Get(0)
			if repoOrAsset == "" {
				repoOrAsset = "."
			}
			rootPath := repoOrAsset
			asset := ""
			if isPathReferencingAsset(repoOrAsset) {
				asset = repoOrAsset
				pipelineRootFromAsset, err := path.GetPipelineRootFromTask(repoOrAsset, pipelineDefinitionFile)
				if err != nil {
					printError(err, c.String("output"), "Failed to find the pipeline root for the given asset")
					return cli.Exit("", 1)
				}
				rootPath = pipelineRootFromAsset
			}

			logger.Debugf("using root path '%s'", rootPath)
			repoRoot, err := git.FindRepoFromPath(rootPath)
			if err != nil {
				printError(err, c.String("output"), "Failed to find the git repository root")
				return cli.Exit("", 1)
			}
			logger.Debugf("found repo root '%s'", repoRoot.Path)

			configFilePath := path2.Join(repoRoot.Path, ".bruin.yml")
			cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
			if err != nil {
				printError(err, c.String("output"), fmt.Sprintf("Failed to load the config file at '%s'", configFilePath))
				return cli.Exit("", 1)
			}

			logger.Debugf("loaded the config from path '%s'", configFilePath)

			err = switchEnvironment(c, cm, os.Stdin)
			if err != nil {
				return err
			}

			logger.Debugf("switched to the environment '%s'", cm.SelectedEnvironmentName)

			connectionManager, errs := connection.NewManagerFromConfig(cm)
			if len(errs) > 0 {
				printErrors(errs, c.String("output"), "Failed to register connections")
				return cli.Exit("", 1)
			}

			logger.Debugf("built the connection manager instance")

			rules, err := lint.GetRules(fs, &git.RepoFinder{}, c.Bool("exclude-warnings"))
			if err != nil {
				printError(err, c.String("output"), "An error occurred while building the validation rules")

				return cli.Exit("", 1)
			}

			logger.Debugf("successfully loaded %d rules", len(rules))

			renderer := jinja.NewRendererWithYesterday("your-pipeline-name", "your-run-id")

			if len(cm.SelectedEnvironment.Connections.GoogleCloudPlatform) > 0 {
				rules = append(rules, &lint.QueryValidatorRule{
					Identifier:  "bigquery-validator",
					TaskType:    pipeline.AssetTypeBigqueryQuery,
					Connections: connectionManager,
					Extractor: &query.WholeFileExtractor{
						Fs:       fs,
						Renderer: renderer,
					},
					Materializer: bigquery.NewMaterializer(false),
					WorkerCount:  32,
					Logger:       logger,
				})
			} else {
				logger.Debug("no GCP connections found, skipping BigQuery validation")
			}

			if len(cm.SelectedEnvironment.Connections.Snowflake) > 0 {
				rules = append(rules, &lint.QueryValidatorRule{
					Identifier:  "snowflake-validator",
					TaskType:    pipeline.AssetTypeSnowflakeQuery,
					Connections: connectionManager,
					Extractor: &query.FileQuerySplitterExtractor{
						Fs:       fs,
						Renderer: renderer,
					},
					WorkerCount: 32,
					Logger:      logger,
				})
			} else {
				logger.Debug("no Snowflake connections found, skipping Snowflake validation")
			}

			var result *lint.PipelineAnalysisResult
			var errr error
			if asset == "" {
				linter := lint.NewLinter(path.GetPipelinePaths, DefaultPipelineBuilder, rules, logger)
				logger.Debugf("running %d rules for pipeline validation", len(rules))
				infoPrinter.Printf("Validating pipelines in '%s' for '%s' environment...\n", rootPath, cm.SelectedEnvironmentName)
				result, errr = linter.Lint(rootPath, pipelineDefinitionFile)
			} else {
				rules = lint.FilterRulesByLevel(rules, lint.LevelAsset)
				logger.Debugf("running %d rules for asset-only validation", len(rules))
				linter := lint.NewLinter(path.GetPipelinePaths, DefaultPipelineBuilder, rules, logger)
				result, errr = linter.LintAsset(rootPath, pipelineDefinitionFile, asset)
			}

			printer := lint.Printer{RootCheckPath: rootPath}

			if errr != nil || result == nil {
				printError(err, c.String("output"), "An error occurred")
				return cli.Exit("", 1)
			}

			if strings.ToLower(strings.TrimSpace(c.String("output"))) == "json" {
				err = printer.PrintJSON(result)
				if err != nil {
					printError(err, c.String("output"), "An error occurred")
					return cli.Exit("", 1)
				}
				return nil
			}

			err = reportLintErrors(result, err, printer, asset)
			if err != nil {
				printError(err, c.String("output"), "An error occurred")
				return cli.Exit("", 1)
			}
			return nil
		},
	}
}

func reportLintErrors(result *lint.PipelineAnalysisResult, err error, printer lint.Printer, asset string) error {
	if err != nil {
		errorPrinter.Println("\nAn error occurred while linting asset:")

		errorList := unwrapAllErrors(err)
		for i, e := range errorList {
			errorPrinter.Printf("%s└── %s\n", strings.Repeat("  ", i), e)
		}

		return err
	}

	printer.PrintIssues(result)

	// prepare the final message
	errorCount := result.ErrorCount()
	warningCount := result.WarningCount()
	pipelineCount := len(result.Pipelines)
	pipelineStr := "pipeline"
	if pipelineCount > 1 {
		pipelineStr += "s"
	}

	if errorCount > 0 || warningCount > 0 {
		issueStr := "issue"
		if errorCount > 1 {
			issueStr += "s"
		}

		warningStr := "warning"
		if warningCount > 1 {
			warningStr += "s"
		}

		foundMessage := "found"
		if errorCount > 0 {
			errorColoredMessage := color.New(color.FgRed).SprintFunc()
			foundMessage += errorColoredMessage(fmt.Sprintf(" %d %s", errorCount, issueStr))
		}

		if warningCount > 0 {
			if errorCount > 0 {
				foundMessage += " and"
			}
			warningColoredMessage := color.New(color.FgYellow).SprintFunc()
			foundMessage += warningColoredMessage(fmt.Sprintf(" %d %s", warningCount, warningStr))
		}

		if asset == "" {
			infoPrinter.Printf("\n✘ Checked %d %s and %s, please check above.\n", pipelineCount, pipelineStr, foundMessage)
		} else {
			infoPrinter.Printf("\n✘ Checked '%s' and found %s, please check above.\n", asset, foundMessage)
		}

		if errorCount > 0 {
			return errors.New("validation failed")
		}

		// warnings should not return failure
		return nil
	}

	taskCount := 0
	for _, p := range result.Pipelines {
		taskCount += len(p.Pipeline.Assets)
	}

	successPrinter.Printf("\n✓ Successfully validated %d assets across %d %s, all good.\n", taskCount, pipelineCount, pipelineStr)
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
