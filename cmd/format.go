package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/telemetry"
	errors2 "github.com/pkg/errors"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func Format(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "format",
		Usage:     "format given asset definition files",
		ArgsUsage: "[path to project root]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
			&cli.BoolFlag{
				Name:  "fail-if-changed",
				Usage: "exit with failure code if any file is modified",
				Value: false,
			},
		},
		Action: func(c *cli.Context) error {
			logger := makeLogger(*isDebug)

			repoOrAsset := c.Args().Get(0)
			if repoOrAsset == "" {
				repoOrAsset = "."
			}

			output := c.String("output")
			failIfChanged := c.Bool("fail-if-changed")

			if isPathReferencingAsset(repoOrAsset) {
				if failIfChanged {
					changed, err := hasFileChanged(repoOrAsset)
					if err != nil {
						if errors.Is(err, os.ErrNotExist) {
							printErrorForOutput(output, fmt.Errorf("the given file path '%s' does not seem to exist, are you sure you used the right path?", repoOrAsset))
						} else {
							printErrorForOutput(output, errors2.Wrap(err, "failed to check asset"))
						}
					}
					if changed {
						printErrorForOutput(output, fmt.Errorf("Failure:asset '%s' needs to be reformatted", repoOrAsset))
						return cli.Exit("", 1)
					} else {
						infoPrinter.Printf("Success:Asset '%s' doesn't need reformatting", repoOrAsset)
						return cli.Exit("", 0)
					}
				}
				asset, err := formatAsset(repoOrAsset)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						printErrorForOutput(output, fmt.Errorf("the given file path '%s' does not seem to exist, are you sure you used the right path?", repoOrAsset))
					} else {
						printErrorForOutput(output, errors2.Wrap(err, "failed to format asset"))
					}

					return cli.Exit("", 1)
				}

				if output != "json" {
					infoPrinter.Printf("Successfully formatted asset '%s' %s\n", asset.Name, faint(fmt.Sprintf("(%s)", asset.DefinitionFile.Path)))
				}
			}

			assetFinderPool := pool.New().WithMaxGoroutines(32)
			assetPaths := path.GetAllPossibleAssetPaths(repoOrAsset, assetsDirectoryNames, pipeline.SupportedFileSuffixes)

			if len(assetPaths) == 0 {
				infoPrinter.Println("No assets were found in the given path, aborting.")
				return cli.Exit("", 1)
			}

			logger.Debugf("found %d asset paths", len(assetPaths))

			errorList := make([]error, 0)
			changedAssetpaths := make([]string, 0)
			processedAssetCount := 0

			for _, assetPath := range assetPaths {
				assetFinderPool.Go(func() {
					if failIfChanged {
						changed, err := hasFileChanged(assetPath)
						if err != nil {
							logger.Debugf("failed to process path '%s': %v", assetPath, err)
							errorList = append(errorList, errors2.Wrapf(err, "failed to check '%s'", assetPath))
							return
						}
						if changed {
							changedAssetpaths = append(changedAssetpaths, assetPath)
						}

					} else {
						asset, err := DefaultPipelineBuilder.CreateAssetFromFile(assetPath)
						if err != nil {
							logger.Debugf("failed to process path '%s': %v", assetPath, err)
							return
						}

						if asset == nil {
							logger.Debugf("no asset found in path '%s': %v", assetPath, err)
							return
						}

						err = asset.Persist(afero.NewOsFs())
						if err != nil {
							logger.Debugf("failed to persist asset '%s': %v", assetPath, err)
							errorList = append(errorList, errors2.Wrapf(err, "failed to persist asset '%s'", assetPath))
						}

						processedAssetCount++
					}
				})
			}

			assetFinderPool.Wait()
			if failIfChanged && len(errorList) == 0 {
				if len(changedAssetpaths) == 0 {
					infoPrinter.Printf("Success: No Asset '%s' needs reformatting", repoOrAsset)
					return cli.Exit("", 0)
				}
				errorPrinter.Println("Failure: Some Assets needs reformatting:")
				for _, assetPath := range changedAssetpaths {
					infoPrinter.Printf("'%s'\n", assetPath)
				}
				return cli.Exit("", 1)
			} else {
				if output == "json" {
					return nil
				}

				if processedAssetCount == 0 {
					infoPrinter.Println("No actual assets were found in the given path, nothing has changed.")
					return nil
				}

				assetStr := "asset"
				if processedAssetCount > 1 {
					assetStr += "s"
				}
				infoPrinter.Printf("Successfully formatted %d %s.\n", processedAssetCount, assetStr)
				return nil
			}
			if output == "json" {
				jsMessage, err := json.Marshal(errorList)
				if err != nil {
					printErrorJSON(err)
				}
				fmt.Println(jsMessage)
				return cli.Exit("", 1)
			}

			errorPrinter.Println("Some errors occurred:")
			for _, err := range errorList {
				errorPrinter.Println("  - " + err.Error())
			}

			return cli.Exit("", 1)
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

func formatAsset(path string) (*pipeline.Asset, error) {
	asset, err := DefaultPipelineBuilder.CreateAssetFromFile(path)
	if err != nil {
		return nil, errors2.Wrap(err, "failed to build the asset")
	}

	return asset, asset.Persist(afero.NewOsFs())
}

func hasFileChanged(path string) (bool, error) {
	fs := afero.NewOsFs()
	// Read the original content from the file
	originalContent, err := afero.ReadFile(fs, path)
	if err != nil {
		return false, errors2.Wrap(err, "failed to read original content")
	}
	// Create the asset
	asset, err := DefaultPipelineBuilder.CreateAssetFromFile(path)
	if err != nil {
		return false, errors2.Wrap(err, "failed to build the asset")
	}
	// Persist the asset (modifies the file)
	err = asset.Persist(fs)
	if err != nil {
		return false, errors2.Wrap(err, "failed to persist the asset")
	}

	// Read the new content after persisting
	newContent, err := afero.ReadFile(fs, path)
	if err != nil {
		return false, errors2.Wrap(err, "failed to read new content")
	}

	// Restore the original content to the file
	err = afero.WriteFile(fs, path, originalContent, 0644)
	if err != nil {
		return false, errors2.Wrap(err, "failed to restore original content")
	}

	// Compare the original and new content
	return string(originalContent) != string(newContent), nil
}
