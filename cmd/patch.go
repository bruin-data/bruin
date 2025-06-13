package cmd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func updateAssetDependencies(ctx context.Context, asset *pipeline.Asset, p *pipeline.Pipeline, sp *sqlparser.SQLParser, renderer *jinja.Renderer) error {
	missingDeps, err := sp.GetMissingDependenciesForAsset(asset, p, renderer.CloneForAsset(ctx, p, asset))
	if err != nil {
		return fmt.Errorf("failed to get missing dependencies for asset '%s': %w", asset.Name, err)
	}

	if len(missingDeps) == 0 {
		return nil
	}

	for _, dep := range missingDeps {
		foundMissingUpstream := p.GetAssetByName(dep)
		if foundMissingUpstream == nil {
			continue
		}

		if foundMissingUpstream.Name == asset.Name {
			continue
		}

		asset.AddUpstream(foundMissingUpstream)
	}

	err = asset.Persist(afero.NewOsFs()) // Use afero.NewOsFs() for real file system operations
	if err != nil {
		return fmt.Errorf("failed to persist asset '%s': %w", asset.Name, err)
	}

	return nil
}

func Patch() *cli.Command {
	return &cli.Command{
		Name:      "patch",
		ArgsUsage: "[path to the asset or pipeline]",
		Subcommands: []*cli.Command{
			{
				Name:      "fill-asset-dependencies",
				Usage:     "Fills missing asset dependencies based on the query. Accepts a path to an asset file or a pipeline directory.",
				ArgsUsage: "[path to the asset or pipeline]",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Usage:   "Output format. Possible values: plain, json",
						Value:   "plain",
					},
				},
				Action: func(c *cli.Context) error {
					inputPath := c.Args().First()
					if inputPath == "" {
						fmt.Println("Please provide a path to an asset or a pipeline.")
						return nil
					}

					output := c.String("output")

					sqlParserInstance, err := sqlparser.NewSQLParser(false)
					if err != nil {
						printErrorForOutput(output, fmt.Errorf("failed to create sql parser: %w", err))
						return cli.Exit("", 1)
					}
					defer sqlParserInstance.Close()

					jinjaRenderer := jinja.NewRendererWithYesterday("test-pipeline", "test-run-id")

					ctx := context.Background()

					if isPathReferencingAsset(inputPath) { //nolint
						pipelinePath, err := path.GetPipelineRootFromTask(inputPath, []string{".bruin.yml", "pipeline.yml"}) // TODO: use shared constant
						if err != nil {
							printErrorForOutput(output, fmt.Errorf("failed to find the pipeline this asset belongs to: '%s': %w", inputPath, err))
							return cli.Exit("", 1)
						}

						foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate())
						if err != nil {
							printErrorForOutput(output, fmt.Errorf("failed to build pipeline at '%s': %w", pipelinePath, err))
							return cli.Exit("", 1)
						}

						asset, err := DefaultPipelineBuilder.CreateAssetFromFile(inputPath, foundPipeline)
						if err != nil {
							printErrorForOutput(output, fmt.Errorf("failed to build asset from file '%s': %w", inputPath, err))
							return cli.Exit("", 1)
						}

						asset, err = DefaultPipelineBuilder.MutateAsset(ctx, asset, foundPipeline)
						if err != nil {
							printErrorForOutput(output, fmt.Errorf("failed to mutate asset '%s': %w", asset.Name, err))
							return cli.Exit("", 1)
						}
						if asset == nil {
							printErrorForOutput(output, fmt.Errorf("the given file path doesn't seem to be a Bruin asset definition: '%s'", inputPath))
							return cli.Exit("", 1)
						}

						err = updateAssetDependencies(ctx, asset, foundPipeline, sqlParserInstance, jinjaRenderer)
						if err != nil {
							printErrorForOutput(output, fmt.Errorf("failed to update asset dependencies: %w", err))
							return cli.Exit("", 1)
						}

						if output == "json" {
							fmt.Println(`{"status": "success", "message": "Asset dependencies updated successfully"}`)
							return nil
						}

						successPrinter.Printf("Asset dependencies updated successfully for asset '%s'\n", asset.Name)
						return nil
					} else {
						// This is a pipeline path
						pipelinePath := inputPath
						foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate())
						if err != nil {
							printErrorForOutput(output, fmt.Errorf("failed to build pipeline at '%s': %w", pipelinePath, err))
							return cli.Exit("", 1)
						}

						failedAssets := make(map[string]error)
						processedAssets := 0
						successfulAssets := 0
						for _, asset := range foundPipeline.Assets {
							processedAssets++
							err := updateAssetDependencies(ctx, asset, foundPipeline, sqlParserInstance, jinjaRenderer)
							if err != nil {
								failedAssets[asset.Name] = err
							} else {
								successfulAssets++
							}
						}

						if len(failedAssets) == 0 {
							if output == "json" {
								fmt.Println(`{"status": "success", "message": "Asset dependencies updated successfully"}`)
								return nil
							} else {
								successPrinter.Printf("Successfully updated dependencies for %d assets in pipeline '%s'.\n", successfulAssets, foundPipeline.Name)
								return nil
							}
						}

						if successfulAssets == 0 {
							printErrorForOutput(output, fmt.Errorf("encountered errors while processing pipeline '%s' with %d assets", foundPipeline.Name, len(failedAssets)))
							return cli.Exit("", 1)
						}

						if output == "json" {
							resp := map[string]interface{}{
								"status":            "failed",
								"message":           fmt.Sprintf("Asset dependencies updated successfully for %d assets in pipeline '%s'", successfulAssets, foundPipeline.Name),
								"successful_assets": successfulAssets,
								"failed_assets":     len(failedAssets),
								"processed_assets":  processedAssets,
							}
							jsonResp, err := json.Marshal(resp)
							if err != nil {
								printErrorForOutput(output, fmt.Errorf("failed to marshal json: %w", err))
								return cli.Exit("", 1)
							}
							fmt.Println(string(jsonResp))
							return nil
						}
						errorPrinter.Println("Summary:")
						errorPrinter.Printf("Processed %d assets in pipeline '%s'\n", processedAssets, foundPipeline.Name)
						errorPrinter.Printf("Successfully updated dependencies for %d assets\n", successfulAssets)
						errorPrinter.Printf("Failed to update dependencies for %d assets\n", len(failedAssets))
						for assetName, err := range failedAssets {
							errorPrinter.Printf("- '%s': %s\n", assetName, err)
						}

						return nil
					}
				},
			},
		},
	}
}
