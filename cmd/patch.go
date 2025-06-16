package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

const (
	fillStatusUpdated = "updated"
	fillStatusSkipped = "skipped"
	fillStatusFailed  = "failed"
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

// Returns: status ("updated", "skipped", "failed").
func fillColumnsFromDB(pp *ppInfo, fs afero.Fs, environment string) (string, error) {
	_, conn, err := getConnectionFromPipelineInfo(pp, environment)
	if err != nil {
		return fillStatusFailed, fmt.Errorf("failed to get connection for asset '%s': %w", pp.Asset.Name, err)
	}
	querier, ok := conn.(interface {
		SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error)
	})
	if !ok {
		return fillStatusFailed, fmt.Errorf("connection for asset '%s' does not support schema introspection", pp.Asset.Name)
	}
	tableName := pp.Asset.Name
	queryStr := fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName)
	q := &query.Query{Query: queryStr}
	result, err := querier.SelectWithSchema(context.Background(), q)
	if err != nil {
		return fillStatusFailed, fmt.Errorf("failed to query columns for asset '%s': %w", pp.Asset.Name, err)
	}
	if len(result.Columns) == 0 {
		return fillStatusFailed, fmt.Errorf("no columns found for asset '%s' (table may not exist)", pp.Asset.Name)
	}

	// Skip special column names
	skipColumns := map[string]bool{
		"_is_current":  true,
		"_valid_until": true,
		"_valid_from":  true,
	}

	existingColumns := make(map[string]pipeline.Column)
	for _, col := range pp.Asset.Columns {
		existingColumns[strings.ToLower(col.Name)] = col
	}

	if len(existingColumns) == 0 {
		columns := make([]pipeline.Column, 0, len(result.Columns))
		for i, colName := range result.Columns {
			if skipColumns[colName] {
				continue
			}
			columns = append(columns, pipeline.Column{
				Name:      colName,
				Type:      result.ColumnTypes[i],
				Checks:    []pipeline.ColumnCheck{},
				Upstreams: []*pipeline.UpstreamColumn{},
			})
		}
		pp.Asset.Columns = columns
		err = pp.Asset.Persist(fs)
		if err != nil {
			return fillStatusFailed, fmt.Errorf("failed to persist asset '%s': %w", pp.Asset.Name, err)
		}
		return fillStatusUpdated, nil
	}

	hasNewColumns := false
	for i, colName := range result.Columns {
		if skipColumns[colName] {
			continue
		}
		lowerColName := strings.ToLower(colName)
		if _, exists := existingColumns[lowerColName]; !exists {
			pp.Asset.Columns = append(pp.Asset.Columns, pipeline.Column{
				Name:      colName,
				Type:      result.ColumnTypes[i],
				Checks:    []pipeline.ColumnCheck{},
				Upstreams: []*pipeline.UpstreamColumn{},
			})
			hasNewColumns = true
		}
	}

	if !hasNewColumns {
		return fillStatusSkipped, nil
	}

	err = pp.Asset.Persist(fs)
	if err != nil {
		return fillStatusFailed, fmt.Errorf("failed to persist asset '%s': %w", pp.Asset.Name, err)
	}
	return fillStatusUpdated, nil
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
							status, err := fillColumnsFromDB(pp, afero.NewOsFs(), "")
							processedAssets++
							switch status {
							case fillStatusUpdated:
								updatedAssets = append(updatedAssets, asset.Name)
							case fillStatusSkipped:
								skippedAssets = append(skippedAssets, asset.Name)
							case fillStatusFailed:
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
				Name:      "fill-columns-from-db",
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

					if isPathReferencingAsset(inputPath) { //nolint:nestif
						// Single asset
						pp, err := GetPipelineAndAsset(ctx, inputPath, fs, "")
						if err != nil {
							printErrorForOutput(output, err)
							return cli.Exit("", 1)
						}
						status, err := fillColumnsFromDB(pp, fs, environment)
						if err != nil {
							printErrorForOutput(output, fmt.Errorf("failed to fill columns from DB for asset '%s': %w", pp.Asset.Name, err))
							return cli.Exit("", 1)
						}
						if output == "json" {
							fmt.Printf(`{"status": "%s", "asset Name": "%s'"}\n`, status, pp.Asset.Name)
							return nil
						}
						switch status {
						case fillStatusUpdated:
							successPrinter.Printf("Columns filled from DB for asset '%s'\n", pp.Asset.Name)
							return nil
						case fillStatusSkipped:
							warningPrinter.Printf("No changes needed for asset '%s'\n", pp.Asset.Name)
							return nil
						case fillStatusFailed:
							errorPrinter.Printf("Failed to fill columns from DB for asset '%s'\n", pp.Asset.Name)
							return cli.Exit("", 1)
						}
						return nil
					} else {
						// Pipeline path passed : fill for all assets
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
							status, err := fillColumnsFromDB(pp, fs, environment)
							processedAssets++
							switch status {
							case fillStatusUpdated:
								updatedAssets = append(updatedAssets, asset.Name)
							case fillStatusSkipped:
								skippedAssets = append(skippedAssets, asset.Name)
							case fillStatusFailed:
								failedAssets[asset.Name] = err
							}
						}

						if output == "json" {
							status := "success"
							if len(failedAssets) > 0 {
								status = "partial"
								if len(updatedAssets) == 0 {
									status = "failed"
								}
							} else if len(updatedAssets) == 0 {
								status = "skipped"
							}

							resp := map[string]interface{}{
								"status":           status,
								"skipped_assets":   len(skippedAssets),
								"updated_assets":   len(updatedAssets),
								"failed_assets":    len(failedAssets),
								"processed_assets": processedAssets,
							}
							jsonResp, _ := json.Marshal(resp)
							fmt.Println(string(jsonResp))
							if status == "failed" {
								return cli.Exit("", 1)
							}
							return nil
						}

						status := "success"
						if len(failedAssets) > 0 {
							status = "partial"
							if len(updatedAssets) == 0 {
								status = "failed"
							}
						} else if len(updatedAssets) == 0 {
							status = "skipped"
						}

						switch status {
						case "failed":
							errorPrinter.Printf("Failed to fill columns from DB for %d assets:\n", len(failedAssets))
							for assetName, _ := range failedAssets {
								errorPrinter.Printf("- '%s'\n", assetName)
							}
							if len(skippedAssets) > 0 {
								warningPrinter.Printf("\n No changes required for %d assets:\n", len(skippedAssets))
								for _, name := range skippedAssets {
									warningPrinter.Printf("- %s\n", name)
								}
							}
							return cli.Exit("", 1)
						case "skipped":
							warningPrinter.Printf("No changes needed for pipeline '%s': %d assets skipped\n", foundPipeline.Name, len(skippedAssets))
							return nil
						case "success":
							successPrinter.Printf("Successfully updated all %d assets in pipeline '%s'\n", len(updatedAssets), foundPipeline.Name)

							return nil
						case "partial":
							successPrinter.Printf("Partially updated pipeline '%s':\n", foundPipeline.Name)
							if len(updatedAssets) > 0 {
								successPrinter.Printf("Successfully updated %d assets:\n", len(updatedAssets))
								for _, name := range updatedAssets {
									successPrinter.Printf("- %s\n", name)
								}
							}
							if len(skippedAssets) > 0 {
								warningPrinter.Printf("\nSkipped %d assets:\n", len(skippedAssets))
								for _, name := range skippedAssets {
									warningPrinter.Printf("- %s\n", name)
								}
							}
							if len(failedAssets) > 0 {
								errorPrinter.Printf("\nFailed to update %d assets:\n", len(failedAssets))
								for assetName, err := range failedAssets {
									errorPrinter.Printf("- '%s': %s\n", assetName, err)
								}
							}
							return nil
						}
						return nil
					}
				},
			},
		},
	}
}
