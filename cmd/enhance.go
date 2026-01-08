package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/enhance"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

const (
	enhanceStatusUpdated = "updated"
	enhanceStatusSkipped = "skipped"
	enhanceStatusFailed  = "failed"
)

// enhanceCommand returns the enhance subcommand for the ai parent command.
func enhanceCommand(isDebug *bool) *cli.Command {
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
				Name:  "manual",
				Usage: "Manually confirm each suggestion before applying",
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
			&cli.BoolFlag{
				Name:  "debug",
				Usage: "Show debug information during enhancement",
				Value: false,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			// Check both global and local debug flags
			localDebug := c.Bool("debug")
			effectiveDebug := (isDebug != nil && *isDebug) || localDebug
			return enhanceAction(ctx, c, &effectiveDebug)
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
		return enhanceSingleAsset(ctx, c, inputPath, fs, output, logger, isDebug)
	}

	return enhancePipeline(ctx, c, inputPath, fs, output, logger, isDebug)
}

func enhanceSingleAsset(ctx context.Context, c *cli.Command, assetPath string, fs afero.Fs, output string, logger interface{}, isDebug *bool) error {
	absAssetPath, _ := filepath.Abs(assetPath)

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
		infoPrinter.Println("Step 3/3: Enhancing asset with AI...")
	}

	// Reload asset after previous steps may have modified it
	pp, err := GetPipelineAndAsset(ctx, assetPath, fs, "")
	if err != nil {
		return printEnhanceError(output, errors.Wrap(err, "failed to load asset"))
	}

	enhancer := enhance.NewEnhancer(fs, c.String("model"))

	// Enable debug mode if --debug flag is set
	isDebugMode := isDebug != nil && *isDebug
	enhancer.SetDebug(isDebugMode)

	// Try to get API key from config
	if apiKey := getAnthropicAPIKey(fs, assetPath); apiKey != "" {
		enhancer.SetAPIKey(apiKey)
	}

	// Set repo root and environment for MCP file tools
	repoRoot, err := git.FindRepoFromPath(assetPath)
	if err == nil {
		enhancer.SetRepoRoot(repoRoot.Path)
		if output != "json" {
			infoPrinter.Printf("  AI agent enabled with file editing tools\n")
		}
	}
	env := c.String("environment")
	if env != "" {
		enhancer.SetEnvironment(env)
		if output != "json" {
			infoPrinter.Printf("  Using environment: %s\n", env)
		}
	}

	// Check if Claude CLI is available
	if err := enhancer.EnsureClaudeCLI(); err != nil {
		return printEnhanceError(output, errors.Wrap(err, "Claude CLI not available"))
	}

	// Pre-fetch table statistics to minimize tool calls during enhancement
	var tableSummaryJSON string
	if pp.Asset.Materialization.Type != "" && pp.Asset.Name != "" {
		tableSummaryJSON = preFetchTableSummary(ctx, fs, assetPath, pp.Asset, env, output)
	}

	suggestions, err := enhancer.EnhanceAssetWithStats(ctx, pp.Asset, pp.Pipeline.Name, tableSummaryJSON)
	if err != nil {
		return printEnhanceError(output, errors.Wrap(err, "failed to enhance asset"))
	}

	// If suggestions is nil, Claude directly edited the file via MCP
	if suggestions == nil {
		if output == "json" {
			result := map[string]interface{}{
				"status": "success",
				"asset":  pp.Asset.Name,
			}
			jsonBytes, _ := json.Marshal(result)
			fmt.Println(string(jsonBytes))
		} else {
			successPrinter.Printf("\n✓ Enhanced '%s'\n", pp.Asset.Name)
			// Show git diff to display what changed
			showGitDiff(absAssetPath)
		}
		return nil
	}

	// Fallback mode: Claude returned JSON suggestions
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

	// Auto-apply by default, or interactive confirmation with --manual flag
	var approved *enhance.EnhancementSuggestions
	if c.Bool("manual") && output != "json" {
		confirmer := enhance.NewInteractiveConfirmer(os.Stdin, os.Stdout)
		approved, err = confirmer.ConfirmSuggestions(suggestions, pp.Asset.Name)
		if err != nil {
			return printEnhanceError(output, errors.Wrap(err, "confirmation failed"))
		}
	} else {
		approved = suggestions
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
			enhance.DisplayAppliedChanges(os.Stdout, approved, pp.Asset.Name)
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

func enhancePipeline(ctx context.Context, c *cli.Command, pipelinePath string, fs afero.Fs, output string, logger interface{}, isDebug *bool) error {
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
		infoPrinter.Println("Step 3/3: Enhancing assets with AI...")
	}

	enhancer := enhance.NewEnhancer(fs, c.String("model"))

	// Enable debug mode if --debug flag is set
	isDebugMode := isDebug != nil && *isDebug
	enhancer.SetDebug(isDebugMode)

	// Try to get API key from config
	if apiKey := getAnthropicAPIKey(fs, pipelinePath); apiKey != "" {
		enhancer.SetAPIKey(apiKey)
	}

	// Set repo root and environment for MCP file tools
	pipelineRepoRoot, repoErr := git.FindRepoFromPath(pipelinePath)
	if repoErr == nil {
		enhancer.SetRepoRoot(pipelineRepoRoot.Path)
		if output != "json" {
			infoPrinter.Printf("  AI agent enabled with file editing tools\n")
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

	env := c.String("environment")
	for _, asset := range foundPipeline.Assets {
		if output != "json" {
			infoPrinter.Printf("  Processing '%s'...\n", asset.Name)
		}

		// Pre-fetch table statistics to minimize tool calls during enhancement
		var tableSummaryJSON string
		if asset.Materialization.Type != "" && asset.Name != "" {
			tableSummaryJSON = preFetchTableSummary(ctx, fs, asset.DefinitionFile.Path, asset, env, output)
		}

		suggestions, err := enhancer.EnhanceAssetWithStats(ctx, asset, foundPipeline.Name, tableSummaryJSON)
		if err != nil {
			failedCount++
			if output != "json" {
				warningPrinter.Printf("    Failed to enhance: %v\n", err)
			}
			results = append(results, map[string]interface{}{
				"asset":  asset.Name,
				"status": enhanceStatusFailed,
				"error":  err.Error(),
			})
			continue
		}

		// If suggestions is nil, Claude directly edited the file via MCP
		if suggestions == nil {
			updatedCount++
			if output != "json" {
				successPrinter.Printf("    ✓ Enhanced\n")
			}
			results = append(results, map[string]interface{}{
				"asset":  asset.Name,
				"status": enhanceStatusUpdated,
			})
			continue
		}

		// Fallback mode: Claude returned JSON suggestions
		if suggestions.IsEmpty() {
			skippedCount++
			results = append(results, map[string]interface{}{
				"asset":  asset.Name,
				"status": enhanceStatusSkipped,
			})
			continue
		}

		// Auto-apply by default, or interactive confirmation with --manual flag
		var approved *enhance.EnhancementSuggestions
		if c.Bool("manual") && output != "json" && !c.Bool("dry-run") {
			confirmer := enhance.NewInteractiveConfirmer(os.Stdin, os.Stdout)
			approved, _ = confirmer.ConfirmSuggestions(suggestions, asset.Name)
		} else {
			approved = suggestions
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
			if output != "json" {
				enhance.DisplayAppliedChanges(os.Stdout, approved, asset.Name)
			}
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

		// Show git diff for the pipeline directory if any assets were updated
		if updatedCount > 0 {
			showGitDiff(pipelinePath)
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

// showGitDiff displays git diff output for the given file.
func showGitDiff(filePath string) {
	// Run git diff with --no-pager to print directly without interactive pager
	cmd := exec.Command("git", "--no-pager", "diff", "--color=always", filePath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Println("\nChanges:")
	_ = cmd.Run()
}

// preFetchTableSummary fetches table statistics before calling Claude to minimize tool calls.
// Returns JSON-serialized table summary or empty string if fetching fails.
func preFetchTableSummary(ctx context.Context, fs afero.Fs, assetPath string, asset *pipeline.Asset, environment, output string) string {
	// Get connection name from asset's upstream connection or materialization
	connectionName := getAssetConnectionName(asset)
	if connectionName == "" {
		return ""
	}

	// Get table name from asset
	tableName := asset.Name
	if tableName == "" {
		return ""
	}

	// Load connection manager
	repoRoot, err := git.FindRepoFromPath(assetPath)
	if err != nil {
		return ""
	}

	cm, err := config.LoadOrCreate(fs, filepath.Join(repoRoot.Path, ".bruin.yml"))
	if err != nil {
		return ""
	}

	if environment != "" {
		if err := cm.SelectEnvironment(environment); err != nil {
			return ""
		}
	}

	manager, errs := connection.NewManagerFromConfigWithContext(ctx, cm)
	if len(errs) > 0 {
		return ""
	}

	if output != "json" {
		infoPrinter.Printf("  Pre-fetching table statistics for '%s'...\n", tableName)
	}

	// Fetch table summary with sample values for enum-like columns
	summary := getTableSummaryWithManager(ctx, manager, connectionName, tableName)
	if summary.Error != "" {
		if output != "json" {
			warningPrinter.Printf("  Warning: could not fetch table statistics: %s\n", summary.Error)
		}
		return ""
	}

	// Serialize to JSON
	jsonBytes, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return ""
	}

	if output != "json" {
		infoPrinter.Printf("  Pre-fetched statistics for %d columns", len(summary.Columns))
		if len(summary.SampleValues) > 0 {
			infoPrinter.Printf(" (including sample values for %d enum-like columns)", len(summary.SampleValues))
		}
		infoPrinter.Println()
	}

	return string(jsonBytes)
}

// getAssetConnectionName extracts the connection name from an asset.
func getAssetConnectionName(asset *pipeline.Asset) string {
	// Check asset connection field first
	if asset.Connection != "" {
		return asset.Connection
	}

	// Check if there's a connection in parameters
	if conn, ok := asset.Parameters["connection"]; ok && conn != "" {
		return conn
	}

	return ""
}

// TableSummary represents comprehensive statistics for an entire table.
type TableSummary struct {
	TableName    string                   `json:"table_name"`
	Connection   string                   `json:"connection"`
	RowCount     int64                    `json:"row_count"`
	Columns      []ColumnSummary          `json:"columns"`
	SampleValues map[string][]interface{} `json:"sample_values,omitempty"`
	Error        string                   `json:"error,omitempty"`
}

// ColumnSummary represents statistics for a single column within a table summary.
type ColumnSummary struct {
	Name           string      `json:"name"`
	Type           string      `json:"type"`
	NormalizedType string      `json:"normalized_type"`
	Nullable       bool        `json:"nullable"`
	PrimaryKey     bool        `json:"primary_key"`
	Unique         bool        `json:"unique"`
	Stats          interface{} `json:"stats,omitempty"`
}

// Selector interface for querying databases.
type Selector interface {
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
}

// getTableSummaryWithManager retrieves comprehensive statistics using a provided connection manager.
func getTableSummaryWithManager(ctx context.Context, manager config.ConnectionAndDetailsGetter, connectionName, tableName string) *TableSummary {
	if manager == nil {
		return &TableSummary{TableName: tableName, Error: "connection manager is nil"}
	}

	conn := manager.GetConnection(connectionName)
	if conn == nil {
		return &TableSummary{TableName: tableName, Error: fmt.Sprintf("connection '%s' not found", connectionName)}
	}

	summarizer, ok := conn.(diff.TableSummarizer)
	if !ok {
		return &TableSummary{TableName: tableName, Error: fmt.Sprintf("connection '%s' does not support table summary", connectionName)}
	}

	result, err := summarizer.GetTableSummary(ctx, tableName, false)
	if err != nil {
		return &TableSummary{TableName: tableName, Connection: connectionName, Error: fmt.Sprintf("failed to get table summary: %v", err)}
	}

	summary := &TableSummary{
		TableName:    tableName,
		Connection:   connectionName,
		RowCount:     result.RowCount,
		Columns:      make([]ColumnSummary, 0, len(result.Table.Columns)),
		SampleValues: make(map[string][]interface{}),
	}

	for _, col := range result.Table.Columns {
		colSummary := ColumnSummary{
			Name:           col.Name,
			Type:           col.Type,
			NormalizedType: string(col.NormalizedType),
			Nullable:       col.Nullable,
			PrimaryKey:     col.PrimaryKey,
			Unique:         col.Unique,
		}

		// Convert statistics to a map for JSON serialization
		if col.Stats != nil {
			colSummary.Stats = convertStatsToMap(col.Stats)
		}

		summary.Columns = append(summary.Columns, colSummary)

		// Pre-fetch sample values for enum-like columns
		if isEnumLikeColumn(col.Name, string(col.NormalizedType)) {
			values := getSampleColumnValues(ctx, conn, tableName, col.Name, 20)
			if len(values) > 0 {
				summary.SampleValues[col.Name] = values
			}
		}
	}

	return summary
}

// convertStatsToMap converts diff.ColumnStatistics to a map for JSON serialization.
func convertStatsToMap(stats diff.ColumnStatistics) map[string]interface{} {
	result := make(map[string]interface{})
	result["type"] = stats.Type()

	switch s := stats.(type) {
	case *diff.NumericalStatistics:
		if s.Min != nil {
			result["min"] = *s.Min
		}
		if s.Max != nil {
			result["max"] = *s.Max
		}
		if s.Avg != nil {
			result["avg"] = *s.Avg
		}
		result["count"] = s.Count
		result["null_count"] = s.NullCount
		if s.StdDev != nil {
			result["stddev"] = *s.StdDev
		}
	case *diff.StringStatistics:
		result["distinct_count"] = s.DistinctCount
		result["min_length"] = s.MinLength
		result["max_length"] = s.MaxLength
		result["avg_length"] = s.AvgLength
		result["count"] = s.Count
		result["null_count"] = s.NullCount
		result["empty_count"] = s.EmptyCount
	case *diff.BooleanStatistics:
		result["true_count"] = s.TrueCount
		result["false_count"] = s.FalseCount
		result["count"] = s.Count
		result["null_count"] = s.NullCount
	case *diff.DateTimeStatistics:
		if !s.EarliestDate.IsZero() {
			result["earliest_date"] = s.EarliestDate.String()
		}
		if !s.LatestDate.IsZero() {
			result["latest_date"] = s.LatestDate.String()
		}
		result["unique_count"] = s.UniqueCount
		result["count"] = s.Count
		result["null_count"] = s.NullCount
	case *diff.JSONStatistics:
		result["count"] = s.Count
		result["null_count"] = s.NullCount
	}

	return result
}

// isEnumLikeColumn checks if a column name suggests it contains enum-like values.
func isEnumLikeColumn(name string, normalizedType string) bool {
	// Only check string columns
	if normalizedType != string(diff.CommonTypeString) {
		return false
	}

	lowerName := strings.ToLower(name)

	// Common enum-like column name patterns
	enumPatterns := []string{
		"status",
		"state",
		"type",
		"category",
		"kind",
		"level",
		"priority",
		"severity",
		"role",
		"gender",
		"country",
		"region",
		"currency",
		"language",
		"tier",
		"plan",
		"source",
		"channel",
		"platform",
		"device",
		"browser",
		"os",
		"method",
		"action",
		"event_type",
		"payment_method",
		"payment_status",
		"order_status",
		"subscription_status",
	}

	for _, pattern := range enumPatterns {
		if strings.Contains(lowerName, pattern) {
			return true
		}
	}

	// Check for suffix patterns
	suffixPatterns := []string{"_type", "_status", "_state", "_kind", "_category", "_level"}
	for _, suffix := range suffixPatterns {
		if strings.HasSuffix(lowerName, suffix) {
			return true
		}
	}

	return false
}

// getSampleColumnValues retrieves sample distinct values for a column.
func getSampleColumnValues(ctx context.Context, conn interface{}, tableName, columnName string, limit int) []interface{} {
	selector, ok := conn.(Selector)
	if !ok {
		return nil
	}

	// Query for distinct values
	sampleQuery := fmt.Sprintf(`
		SELECT DISTINCT %s
		FROM %s
		WHERE %s IS NOT NULL
		LIMIT %d
	`, columnName, tableName, columnName, limit)

	q := &query.Query{Query: sampleQuery}
	result, err := selector.Select(ctx, q)
	if err != nil {
		return nil
	}

	values := make([]interface{}, 0, len(result))
	for _, row := range result {
		if len(row) > 0 {
			values = append(values, row[0])
		}
	}

	return values
}

