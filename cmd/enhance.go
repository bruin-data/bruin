package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/enhance"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

const (
	enhanceStatusUpdated = "updated"
	enhanceStatusSkipped = "skipped"
	enhanceStatusFailed  = "failed"
)

func Enhance(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "enhance",
		Usage:     "Enhance asset definitions with AI-powered suggestions for metadata, quality checks, and descriptions",
		ArgsUsage: "[path to asset or pipeline]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "Output format: plain, json",
				Value:   "plain",
			},
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "Target environment name as defined in .bruin.yml",
			},
			&cli.BoolFlag{
				Name:  "auto-apply",
				Usage: "Automatically apply all suggestions without confirmation",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "skip-format",
				Usage: "Skip the initial formatting step",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "skip-fill-columns",
				Usage: "Skip filling columns from database",
				Value: false,
			},
			&cli.StringFlag{
				Name:  "model",
				Usage: "Claude model to use for AI suggestions",
				Value: "claude-sonnet-4-20250514",
			},
			&cli.BoolFlag{
				Name:  "dry-run",
				Usage: "Show suggestions without applying any changes",
				Value: false,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			return enhanceAction(ctx, c, isDebug)
		},
	}
}

func enhanceAction(ctx context.Context, c *cli.Command, isDebug *bool) error {
	logger := makeLogger(*isDebug)
	fs := afero.NewOsFs()
	output := c.String("output")

	inputPath := c.Args().Get(0)
	if inputPath == "" {
		inputPath = "."
	}

	// Check if it's a single asset or pipeline
	isSingleAsset := isPathReferencingAsset(inputPath)

	if isSingleAsset {
		return enhanceSingleAsset(ctx, c, inputPath, fs, output, logger)
	}

	return enhancePipeline(ctx, c, inputPath, fs, output, logger)
}

func enhanceSingleAsset(ctx context.Context, c *cli.Command, assetPath string, fs afero.Fs, output string, logger interface{}) error {
	// Step 1: Format (unless skipped)
	if !c.Bool("skip-format") {
		if output != "json" {
			infoPrinter.Println("Step 1/3: Formatting asset...")
		}
		_, err := formatAsset(assetPath)
		if err != nil {
			if output != "json" {
				warningPrinter.Printf("Warning: formatting failed: %v\n", err)
			}
			// Continue even if formatting fails
		}
	}

	// Step 2: Fill columns from DB (unless skipped)
	if !c.Bool("skip-fill-columns") {
		if output != "json" {
			infoPrinter.Println("Step 2/3: Filling columns from database...")
		}
		pp, err := GetPipelineAndAsset(ctx, assetPath, fs, "")
		if err == nil {
			status, fillErr := fillColumnsFromDB(pp, fs, c.String("environment"), nil)
			if fillErr != nil && output != "json" {
				warningPrinter.Printf("Warning: fill columns failed: %v\n", fillErr)
			} else if status == fillStatusUpdated && output != "json" {
				infoPrinter.Println("  Columns updated from database schema.")
			}
		} else if output != "json" {
			warningPrinter.Printf("Warning: could not load asset for column filling: %v\n", err)
		}
	}

	// Step 3: AI Enhancement
	if output != "json" {
		infoPrinter.Println("Step 3/3: Getting AI suggestions...")
	}

	// Reload asset after previous steps may have modified it
	pp, err := GetPipelineAndAsset(ctx, assetPath, fs, "")
	if err != nil {
		return printEnhanceError(output, errors.Wrap(err, "failed to load asset"))
	}

	enhancer := enhance.NewEnhancer(fs, c.String("model"))

	// Try to get API key from config
	if apiKey := getAnthropicAPIKey(fs, assetPath); apiKey != "" {
		enhancer.SetAPIKey(apiKey)
	}

	// Set repo root and environment for MCP database tools
	repoRoot, err := git.FindRepoFromPath(assetPath)
	if err == nil {
		enhancer.SetRepoRoot(repoRoot.Path)
		if output != "json" {
			infoPrinter.Printf("  MCP database tools enabled (repo: %s)\n", repoRoot.Path)
		}
	}
	if env := c.String("environment"); env != "" {
		enhancer.SetEnvironment(env)
		if output != "json" {
			infoPrinter.Printf("  Using environment: %s\n", env)
		}
	}

	// Check if Claude CLI is available
	if err := enhancer.EnsureClaudeCLI(); err != nil {
		return printEnhanceError(output, errors.Wrap(err, "Claude CLI not available"))
	}

	suggestions, err := enhancer.EnhanceAsset(ctx, pp.Asset, pp.Pipeline.Name)
	if err != nil {
		return printEnhanceError(output, errors.Wrap(err, "failed to get AI suggestions"))
	}

	if suggestions.IsEmpty() {
		if output == "json" {
			result := map[string]interface{}{
				"status":  "no_suggestions",
				"message": "No enhancements suggested for this asset",
				"asset":   pp.Asset.Name,
			}
			jsonBytes, _ := json.Marshal(result)
			fmt.Println(string(jsonBytes))
		} else {
			infoPrinter.Printf("No enhancements suggested for asset '%s'.\n", pp.Asset.Name)
		}
		return nil
	}

	// Handle dry-run mode
	if c.Bool("dry-run") {
		if output == "json" {
			result := map[string]interface{}{
				"status":      "dry_run",
				"asset":       pp.Asset.Name,
				"suggestions": suggestions,
			}
			jsonBytes, _ := json.Marshal(result)
			fmt.Println(string(jsonBytes))
		} else {
			enhance.DisplaySuggestions(os.Stdout, suggestions, pp.Asset.Name)
			fmt.Println("\nDry run complete. No changes applied.")
		}
		return nil
	}

	// Interactive confirmation or auto-apply
	var approved *enhance.EnhancementSuggestions
	if c.Bool("auto-apply") {
		approved = suggestions
	} else {
		if output == "json" {
			// In JSON mode, auto-apply since we can't do interactive
			approved = suggestions
		} else {
			confirmer := enhance.NewInteractiveConfirmer(os.Stdin, os.Stdout)
			approved, err = confirmer.ConfirmSuggestions(suggestions, pp.Asset.Name)
			if err != nil {
				return printEnhanceError(output, errors.Wrap(err, "confirmation failed"))
			}
		}
	}

	// Apply and persist
	if !approved.IsEmpty() {
		enhance.ApplySuggestions(pp.Asset, approved)
		if err := pp.Asset.Persist(fs); err != nil {
			return printEnhanceError(output, errors.Wrap(err, "failed to save asset"))
		}

		if output == "json" {
			result := map[string]interface{}{
				"status":  "success",
				"asset":   pp.Asset.Name,
				"applied": approved,
			}
			jsonBytes, _ := json.Marshal(result)
			fmt.Println(string(jsonBytes))
		} else {
			successPrinter.Printf("\nSuccessfully enhanced asset '%s'\n", pp.Asset.Name)
		}
	} else {
		if output == "json" {
			result := map[string]interface{}{
				"status":  "skipped",
				"asset":   pp.Asset.Name,
				"message": "No suggestions were approved",
			}
			jsonBytes, _ := json.Marshal(result)
			fmt.Println(string(jsonBytes))
		} else {
			infoPrinter.Println("No suggestions were approved.")
		}
	}

	return nil
}

func enhancePipeline(ctx context.Context, c *cli.Command, pipelinePath string, fs afero.Fs, output string, logger interface{}) error {
	// Step 1: Format all assets (unless skipped)
	if !c.Bool("skip-format") {
		if output != "json" {
			infoPrinter.Println("Step 1/3: Formatting assets...")
		}
		// Use the format command logic for the entire pipeline
		assetPaths := path.GetAllPossibleAssetPaths(pipelinePath, assetsDirectoryNames, pipeline.SupportedFileSuffixes)
		formattedCount := 0
		for _, assetPath := range assetPaths {
			if _, err := formatAsset(assetPath); err == nil {
				formattedCount++
			}
		}
		if output != "json" && formattedCount > 0 {
			infoPrinter.Printf("  Formatted %d assets.\n", formattedCount)
		}
	}

	// Load the pipeline
	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate())
	if err != nil {
		return printEnhanceError(output, errors.Wrapf(err, "failed to build pipeline at '%s'", pipelinePath))
	}

	// Step 2: Fill columns from DB for all assets (unless skipped)
	if !c.Bool("skip-fill-columns") {
		if output != "json" {
			infoPrinter.Println("Step 2/3: Filling columns from database...")
		}
		// Load config
		repoRoot, err := git.FindRepoFromPath(pipelinePath)
		if err == nil {
			cm, err := config.LoadOrCreate(fs, filepath.Join(repoRoot.Path, ".bruin.yml"))
			if err == nil {
				env := c.String("environment")
				if env != "" {
					_ = cm.SelectEnvironment(env)
				}

				filledCount := 0
				for _, asset := range foundPipeline.Assets {
					pp := &ppInfo{Pipeline: foundPipeline, Asset: asset, Config: cm}
					status, _ := fillColumnsFromDB(pp, fs, env, nil)
					if status == fillStatusUpdated {
						filledCount++
					}
				}
				if output != "json" && filledCount > 0 {
					infoPrinter.Printf("  Updated columns for %d assets.\n", filledCount)
				}
			}
		}
	}

	// Step 3: AI Enhancement for each asset
	if output != "json" {
		infoPrinter.Println("Step 3/3: Getting AI suggestions for assets...")
	}

	enhancer := enhance.NewEnhancer(fs, c.String("model"))

	// Try to get API key from config
	if apiKey := getAnthropicAPIKey(fs, pipelinePath); apiKey != "" {
		enhancer.SetAPIKey(apiKey)
	}

	// Set repo root and environment for MCP database tools
	pipelineRepoRoot, repoErr := git.FindRepoFromPath(pipelinePath)
	if repoErr == nil {
		enhancer.SetRepoRoot(pipelineRepoRoot.Path)
		if output != "json" {
			infoPrinter.Printf("  MCP database tools enabled (repo: %s)\n", pipelineRepoRoot.Path)
		}
	}
	if env := c.String("environment"); env != "" {
		enhancer.SetEnvironment(env)
		if output != "json" {
			infoPrinter.Printf("  Using environment: %s\n", env)
		}
	}

	if err := enhancer.EnsureClaudeCLI(); err != nil {
		return printEnhanceError(output, errors.Wrap(err, "Claude CLI not available"))
	}

	// Re-load pipeline to get updated assets
	foundPipeline, err = DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate())
	if err != nil {
		return printEnhanceError(output, errors.Wrapf(err, "failed to reload pipeline at '%s'", pipelinePath))
	}

	results := make([]map[string]interface{}, 0)
	updatedCount := 0
	skippedCount := 0
	failedCount := 0

	for _, asset := range foundPipeline.Assets {
		if output != "json" {
			infoPrinter.Printf("  Processing '%s'...\n", asset.Name)
		}

		suggestions, err := enhancer.EnhanceAsset(ctx, asset, foundPipeline.Name)
		if err != nil {
			failedCount++
			if output != "json" {
				warningPrinter.Printf("    Failed to get suggestions: %v\n", err)
			}
			results = append(results, map[string]interface{}{
				"asset":  asset.Name,
				"status": enhanceStatusFailed,
				"error":  err.Error(),
			})
			continue
		}

		if suggestions.IsEmpty() {
			skippedCount++
			results = append(results, map[string]interface{}{
				"asset":  asset.Name,
				"status": enhanceStatusSkipped,
			})
			continue
		}

		// In pipeline mode with auto-apply or JSON output, apply automatically
		// Otherwise, this would be too tedious to confirm each
		var approved *enhance.EnhancementSuggestions
		if c.Bool("auto-apply") || output == "json" || c.Bool("dry-run") {
			approved = suggestions
		} else {
			confirmer := enhance.NewInteractiveConfirmer(os.Stdin, os.Stdout)
			approved, _ = confirmer.ConfirmSuggestions(suggestions, asset.Name)
		}

		if c.Bool("dry-run") {
			results = append(results, map[string]interface{}{
				"asset":       asset.Name,
				"status":      "dry_run",
				"suggestions": suggestions,
			})
			continue
		}

		if !approved.IsEmpty() {
			enhance.ApplySuggestions(asset, approved)
			if err := asset.Persist(fs); err != nil {
				failedCount++
				results = append(results, map[string]interface{}{
					"asset":  asset.Name,
					"status": enhanceStatusFailed,
					"error":  err.Error(),
				})
				continue
			}
			updatedCount++
			results = append(results, map[string]interface{}{
				"asset":   asset.Name,
				"status":  enhanceStatusUpdated,
				"applied": approved,
			})
		} else {
			skippedCount++
			results = append(results, map[string]interface{}{
				"asset":  asset.Name,
				"status": enhanceStatusSkipped,
			})
		}
	}

	// Print summary
	if output == "json" {
		summary := map[string]interface{}{
			"status":   "completed",
			"pipeline": foundPipeline.Name,
			"summary": map[string]int{
				"updated": updatedCount,
				"skipped": skippedCount,
				"failed":  failedCount,
				"total":   len(foundPipeline.Assets),
			},
			"results": results,
		}
		jsonBytes, _ := json.Marshal(summary)
		fmt.Println(string(jsonBytes))
	} else {
		fmt.Println()
		infoPrinter.Printf("Enhancement Summary for pipeline '%s':\n", foundPipeline.Name)
		fmt.Printf("  Total assets: %d\n", len(foundPipeline.Assets))
		successPrinter.Printf("  Updated: %d\n", updatedCount)
		fmt.Printf("  Skipped (no suggestions): %d\n", skippedCount)
		if failedCount > 0 {
			errorPrinter.Printf("  Failed: %d\n", failedCount)
		}
	}

	return nil
}

func printEnhanceError(output string, err error) error {
	printErrorForOutput(output, err)
	return cli.Exit("", 1)
}

// getAnthropicAPIKey tries to get the Anthropic API key from the config file.
func getAnthropicAPIKey(fs afero.Fs, inputPath string) string {
	repoRoot, err := git.FindRepoFromPath(inputPath)
	if err != nil {
		return ""
	}

	cm, err := config.LoadOrCreate(fs, filepath.Join(repoRoot.Path, ".bruin.yml"))
	if err != nil {
		return ""
	}

	// Get the selected environment's connections
	env := cm.SelectedEnvironment
	if env == nil {
		return ""
	}

	// Return the first Anthropic connection's API key
	if len(env.Connections.Anthropic) > 0 {
		return env.Connections.Anthropic[0].APIKey
	}

	return ""
}
