package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
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
		},
		Action: func(c *cli.Context) error {
			logger := makeLogger(*isDebug)

			repoOrAsset := c.Args().Get(0)
			if repoOrAsset == "" {
				repoOrAsset = "."
			}

			output := c.String("output")

			if isPathReferencingAsset(repoOrAsset) {
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

			logger.Debugf("found %d asset paths", len(assetPaths))

			errorList := make([]error, 0)
			processedAssetCount := 0

			for _, assetPath := range assetPaths {
				assetFinderPool.Go(func() {
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
				})
			}

			assetFinderPool.Wait()
			if len(errorList) == 0 {
				infoPrinter.Printf("Successfully processed %d assets.\n", processedAssetCount)
			}

			return nil
		},
	}
}

func formatAsset(path string) (*pipeline.Asset, error) {
	asset, err := DefaultPipelineBuilder.CreateAssetFromFile(path)
	if err != nil {
		return nil, errors2.Wrap(err, "failed to build the asset")
	}

	return asset, asset.Persist(afero.NewOsFs())
}
