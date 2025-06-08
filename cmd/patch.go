package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
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

// Returns: status ("updated", "skipped", "failed"), error
func fillColumnsFromDB(pp *ppInfo, fs afero.Fs, environment string, fullRefresh bool) (string, error) {
	// If not full-refresh and asset already has columns, skip
	if !fullRefresh && len(pp.Asset.Columns) > 0 {
		return "skipped", nil
	}
	_, conn, err := getConnectionFromPipelineInfo(pp, environment)
	if err != nil {
		return "failed", fmt.Errorf("failed to get connection for asset '%s': %w", pp.Asset.Name, err)
	}
	if !pp.Asset.IsSQLAsset() {
		return "skipped", nil
	}
	querier, ok := conn.(interface {
		SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error)
	})
	if !ok {
		return "failed", fmt.Errorf("connection for asset '%s' does not support schema introspection", pp.Asset.Name)
	}
	tableName := pp.Asset.Name
	queryStr := fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName)
	q := &query.Query{Query: queryStr}
	result, err := querier.SelectWithSchema(context.Background(), q)
	if err != nil {
		return "failed", fmt.Errorf("failed to query columns for asset '%s': %w", pp.Asset.Name, err)
	}
	if len(result.Columns) == 0 {
		return "failed", fmt.Errorf("no columns found for asset '%s' (table may not exist)", pp.Asset.Name)
	}
	columns := make([]pipeline.Column, len(result.Columns))
	for i, colName := range result.Columns {
		columns[i] = pipeline.Column{
			Name:      colName,
			Type:      result.ColumnTypes[i],
			Checks:    []pipeline.ColumnCheck{},
			Upstreams: []*pipeline.UpstreamColumn{},
		}
	}
	// Only overwrite if fullRefresh or asset.Columns is empty (already checked above)
	pp.Asset.Columns = columns
	err = pp.Asset.Persist(fs)
	if err != nil {
		return "failed", fmt.Errorf("failed to persist asset '%s': %w", pp.Asset.Name, err)
	}
	return "updated", nil
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

						// Load config once for the pipeline
						repoRoot, err := git.FindRepoFromPath(pipelinePath)
						if err != nil {
							printErrorForOutput(output, fmt.Errorf("failed to find the git repository root: %w", err))
							return cli.Exit("", 1)
						}
						cm, err := config.LoadOrCreate(afero.NewOsFs(), filepath.Join(repoRoot.Path, ".bruin.yml"))
						if err != nil {
							printErrorForOutput(output, fmt.Errorf("failed to load the config file: %w", err))
							return cli.Exit("", 1)
						}

						updatedAssets := []string{}
						skippedAssets := []string{}
						failedAssets := map[string]error{}
						processedAssets := 0

						for _, asset := range foundPipeline.Assets {
							pp := &ppInfo{Pipeline: foundPipeline, Asset: asset, Config: cm}
							status, err := fillColumnsFromDB(pp, afero.NewOsFs(), "", false)
							processedAssets++
							switch status {
							case "updated":
								updatedAssets = append(updatedAssets, asset.Name)
							case "skipped":
								skippedAssets = append(skippedAssets, asset.Name)
							case "failed":
								failedAssets[asset.Name] = err
							}
						}

						if len(failedAssets) == 0 {
							if output == "json" {
								fmt.Println(`{"status": "success", "message": "Asset dependencies updated successfully"}`)
								return nil
							} else {
								successPrinter.Printf("Successfully updated dependencies for %d assets in pipeline '%s'.\n", len(updatedAssets), foundPipeline.Name)
								return nil
							}
						}

						if len(updatedAssets) == 0 {
							printErrorForOutput(output, fmt.Errorf("encountered errors while processing pipeline '%s' with %d assets", foundPipeline.Name, len(failedAssets)))
							return cli.Exit("", 1)
						}

						if output == "json" {
							resp := map[string]interface{}{
								"status":            "failed",
								"message":           fmt.Sprintf("Asset dependencies updated successfully for %d assets in pipeline '%s'", len(updatedAssets), foundPipeline.Name),
								"successful_assets": len(updatedAssets),
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
						errorPrinter.Printf("Successfully updated dependencies for %d assets\n", len(updatedAssets))
						if len(skippedAssets) > 0 {
							errorPrinter.Printf("Skipped %d non-SQL assets:\n", len(skippedAssets))
							for _, name := range skippedAssets {
								errorPrinter.Printf("- %s\n", name)
							}
						}
						if len(failedAssets) > 0 {
							errorPrinter.Printf("Failed to update dependencies for %d assets\n", len(failedAssets))
							for assetName, err := range failedAssets {
								errorPrinter.Printf("- '%s': %s\n", assetName, err)
							}
						}

						return nil
					}
				},
			},
			{
				Name:      "fill-from-db",
				Usage:     "Fills the asset's columns from the database schema. Accepts a path to an asset file or a pipeline directory.",
				ArgsUsage: "[path to the asset or pipeline]",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Usage:   "Output format. Possible values: plain, json",
						Value:   "plain",
					},
					&cli.StringFlag{
						Name:    "environment",
						Aliases: []string{"env"},
						Usage:   "Target environment name as defined in .bruin.yml.",
					},
					&cli.BoolFlag{
						Name:  "full-refresh",
						Usage: "If set, overwrite existing columns in the asset with columns from the DB.",
					},
				},
				Action: func(c *cli.Context) error {
					inputPath := c.Args().First()
					if inputPath == "" {
						return errors.New("please provide a path to an asset or a pipeline")
					}

					output := c.String("output")
					environment := c.String("environment")
					fs := afero.NewOsFs()
					ctx := context.Background()
					fullRefresh := c.Bool("full-refresh")

					if isPathReferencingAsset(inputPath) {
						// Single asset
						pp, err := GetPipelineAndAsset(ctx, inputPath, fs, "")
						if err != nil {
							printErrorForOutput(output, fmt.Errorf("failed to resolve asset: %w", err))
							return cli.Exit("", 1)
						}
						status, err := fillColumnsFromDB(pp, fs, environment, fullRefresh)
						if err != nil {
							printErrorForOutput(output, err)
							return cli.Exit("", 1)
						}
						if output == "json" {
							fmt.Printf(`{"status": "%s", "message": "Columns filled from DB for asset '%s'"}\n`, status, pp.Asset.Name)
							return nil
						}
						successPrinter.Printf("Columns filled from DB for asset '%s'\n", pp.Asset.Name)
						return nil
					} else {
						// Pipeline: fill for all assets
						pipelinePath := inputPath
						foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate())
						if err != nil {
							printErrorForOutput(output, fmt.Errorf("failed to build pipeline at '%s': %w", pipelinePath, err))
							return cli.Exit("", 1)
						}
						repoRoot, err := git.FindRepoFromPath(pipelinePath)
						if err != nil {
							printErrorForOutput(output, fmt.Errorf("failed to find the git repository root: %w", err))
							return cli.Exit("", 1)
						}
						cm, err := config.LoadOrCreate(fs, filepath.Join(repoRoot.Path, ".bruin.yml"))
						if err != nil {
							printErrorForOutput(output, fmt.Errorf("failed to load the config file: %w", err))
							return cli.Exit("", 1)
						}
						updatedAssets := []string{}
						skippedAssets := []string{}
						failedAssets := map[string]error{}
						processedAssets := 0

						for _, asset := range foundPipeline.Assets {
							pp := &ppInfo{Pipeline: foundPipeline, Asset: asset, Config: cm}
							status, err := fillColumnsFromDB(pp, fs, environment, fullRefresh)
							processedAssets++
							switch status {
							case "updated":
								updatedAssets = append(updatedAssets, asset.Name)
							case "skipped":
								skippedAssets = append(skippedAssets, asset.Name)
							case "failed":
								failedAssets[asset.Name] = err
							}
						}

						if len(failedAssets) == 0 {
							if len(updatedAssets) == 0 {
								// All skipped
								if output == "json" {
									fmt.Printf(`{"status": "skipped", "message": "All assets in pipeline '%s' were skipped (already have columns or not SQL assets)"}\n`, foundPipeline.Name)
									return nil
								}
								warningPrinter.Printf("All assets in pipeline '%s' were skipped (already have columns or not SQL assets)\n", foundPipeline.Name)
								return nil
							}
							if len(updatedAssets) == processedAssets {
								// All processed assets were updated
								if output == "json" {
									fmt.Printf(`{"status": "success", "message": "Columns filled from DB for all assets in pipeline '%s"}`+"\n", foundPipeline.Name)
									return nil
								}
								successPrinter.Printf("Columns filled from DB for all assets in pipeline '%s'\n", foundPipeline.Name)
								return nil
							}
							// Partial update, no failures
							if output == "json" {
								fmt.Printf(`{"status": "partial", "message": "Columns filled from DB for %d out of %d assets in pipeline '%s'"}\n`, len(updatedAssets), processedAssets, foundPipeline.Name)
								return nil
							}
							successPrinter.Printf("Columns filled from DB for %d out of %d assets in pipeline '%s'\n", len(updatedAssets), processedAssets, foundPipeline.Name)
							return nil
						}

						if len(updatedAssets) == 0 {
							printErrorForOutput(output, fmt.Errorf("encountered errors while processing pipeline '%s' with %d assets", foundPipeline.Name, len(failedAssets)))
							return cli.Exit("", 1)
						}

						if output == "json" {
							resp := map[string]interface{}{
								"status":            "failed",
								"message":           fmt.Sprintf("Asset dependencies updated successfully for %d assets in pipeline '%s'", len(updatedAssets), foundPipeline.Name),
								"successful_assets": len(updatedAssets),
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
						errorPrinter.Printf("Successfully updated dependencies for %d assets\n", len(updatedAssets))
						if len(skippedAssets) > 0 {
							errorPrinter.Printf("Skipped %d non-SQL assets:\n", len(skippedAssets))
							for _, name := range skippedAssets {
								errorPrinter.Printf("- %s\n", name)
							}
						}
						if len(failedAssets) > 0 {
							errorPrinter.Printf("Failed to update dependencies for %d assets\n", len(failedAssets))
							for assetName, err := range failedAssets {
								errorPrinter.Printf("- '%s': %s\n", assetName, err)
							}
						}

						return nil
					}
				},
			},
		},
	}
}
