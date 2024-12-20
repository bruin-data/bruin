package cmd

import (
	"bytes"
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
				Name:  "check",
				Usage: "check if any file needs to be formatted",
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
			check := c.Bool("check")

			if isPathReferencingAsset(repoOrAsset) {
				if check {
					return checkChangesForSingleAsset(repoOrAsset, output)
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
					if check {
						changed, err := shouldFileChange(assetPath)
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
			if check && len(errorList) == 0 {
				if len(changedAssetpaths) == 0 {
					if output == "json" {
						return nil
					}
					infoPrinter.Printf("success: no asset in '%s' needs reformatting", repoOrAsset)
					return nil
				}
				errorPrinter.Println("failure: some Assets needs reformatting:")
				for _, assetPath := range changedAssetpaths {
					infoPrinter.Printf("'%s'\n", assetPath)
				}
				return cli.Exit("", 1)
			}
			if !check && len(errorList) == 0 {
				if output == "json" {
					return nil
				}

				if processedAssetCount == 0 {
					infoPrinter.Println("no actual assets were found in the given path, nothing has changed.")
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

func shouldFileChange(path string) (bool, error) {
	fs := afero.NewOsFs()
	// Read the original content from the file
	originalContent, err := afero.ReadFile(fs, path)
	if err != nil {
		return false, errors2.Wrap(err, "failed to read original content")
	}
	normalizedOriginalContent := normalizeLineEndings(originalContent)
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
	normalizedNewContent := normalizeLineEndings(newContent)

	// Restore the original content to the file
	err = afero.WriteFile(fs, path, originalContent, 0644)
	if err != nil {
		return false, errors2.Wrap(err, "failed to restore original content")
	}

	// Compare the normalized original and new content
	return string(normalizedOriginalContent) != string(normalizedNewContent), nil
}

func checkChangesForSingleAsset(repoOrAsset, output string) error {
	changed, err := shouldFileChange(repoOrAsset)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			printErrorForOutput(output, fmt.Errorf("the given file path '%s' does not seem to exist, are you sure you used the right path?", repoOrAsset))
		} else {
			printErrorForOutput(output, errors2.Wrap(err, "failed to format asset"))
		}
		return cli.Exit("", 1)
	}
	if changed {
		printErrorForOutput(output, fmt.Errorf("failure: asset '%s' needs to be reformatted", repoOrAsset))
		return cli.Exit("", 1)
	}
	if output == "json" {
		return nil
	}
	infoPrinter.Printf("success: Asset '%s' doesn't need reformatting", repoOrAsset)
	return nil
}

func normalizeLineEndings(content []byte) []byte {
	content = bytes.ReplaceAll(content, []byte("\r\n"), []byte("\n"))
	return bytes.ReplaceAll(content, []byte("\r"), []byte("\n"))
}
