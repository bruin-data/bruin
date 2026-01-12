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
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
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
				Usage: "AI model to use for suggestions",
				Value: "claude-sonnet-4-20250514",
			},
			&cli.BoolFlag{
				Name:  "claude",
				Usage: "Use Claude CLI for AI enhancement",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "opencode",
				Usage: "Use OpenCode CLI for AI enhancement",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "codex",
				Usage: "Use Codex CLI for AI enhancement",
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
	fs := afero.NewOsFs()
	output := c.String("output")

	inputPath := c.Args().Get(0)
	if inputPath == "" {
		return printEnhanceError(output, errors.New("please provide a path to an asset file"))
	}

	// Only single asset enhancement is supported
	if !isPathReferencingAsset(inputPath) {
		return printEnhanceError(output, errors.New("please provide a path to a single asset file (e.g., assets/my_asset.sql or assets/my_asset.asset.yml)"))
	}

	return enhanceSingleAsset(ctx, c, inputPath, fs, output, isDebug)
}

func enhanceSingleAsset(ctx context.Context, c *cli.Command, assetPath string, fs afero.Fs, output string, isDebug *bool) error {
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
			status, fillErr := fillColumnsFromDB(pp, fs, c.String("environment"), nil) //nolint:contextcheck
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

	// Determine provider from flags
	providerType := enhance.ProviderClaude // default

	// Check for mutually exclusive flags
	flagCount := 0
	if c.Bool("claude") {
		flagCount++
		providerType = enhance.ProviderClaude
	}
	if c.Bool("opencode") {
		flagCount++
		providerType = enhance.ProviderOpenCode
	}
	if c.Bool("codex") {
		flagCount++
		providerType = enhance.ProviderCodex
	}

	if flagCount > 1 {
		return printEnhanceError(output, errors.New("cannot specify multiple provider flags (--claude, --opencode, --codex)"))
	}

	enhancer := enhance.NewEnhancer(providerType, c.String("model"))

	// Enable debug mode if --debug flag is set
	isDebugMode := isDebug != nil && *isDebug
	enhancer.SetDebug(isDebugMode)

	// Try to get API key from config
	if apiKey := getAnthropicAPIKey(fs, assetPath); apiKey != "" {
		enhancer.SetAPIKey(apiKey)
	}

	env := c.String("environment")
	if env != "" && output != "json" {
		infoPrinter.Printf("  Using environment: %s\n", env)
	}

	// Check if the selected provider's CLI is available
	if err := enhancer.EnsureCLI(); err != nil {
		return printEnhanceError(output, err)
	}

	// Pre-fetch table statistics to minimize tool calls during enhancement
	var tableSummaryJSON string
	if pp.Asset.Materialization.Type != "" && pp.Asset.Name != "" {
		tableSummaryJSON = preFetchTableSummary(ctx, fs, assetPath, pp.Asset, env, output)
	}

	err = enhancer.EnhanceAsset(ctx, pp.Asset, pp.Pipeline.Name, tableSummaryJSON)
	if err != nil {
		return printEnhanceError(output, errors.Wrap(err, "failed to enhance asset"))
	}

	// Provider directly edited the file
	if output == "json" {
		result := struct {
			Status string `json:"status"`
			Asset  string `json:"asset"`
		}{
			Status: "success",
			Asset:  pp.Asset.Name,
		}
		jsonBytes, err := json.Marshal(result)
		if err != nil {
			return printEnhanceError(output, errors.Wrap(err, "failed to marshal result"))
		}
		fmt.Println(string(jsonBytes))
	} else {
		successPrinter.Printf("\n✓ Enhanced '%s'\n", pp.Asset.Name)
		// Show git diff to display what changed
		showGitDiff(absAssetPath)
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
		"cateimage.pngry",
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
