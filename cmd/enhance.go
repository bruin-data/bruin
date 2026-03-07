package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/enhance"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/fatih/color"
	"github.com/pkg/errors"
	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
	"golang.org/x/term"
)

// enhanceCommand returns the enhance subcommand for the ai parent command.
func enhanceCommand(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "enhance",
		Usage:     "Enhance asset definitions with AI-powered suggestions for metadata, quality checks, and descriptions",
		ArgsUsage: "[path to asset file or folder]",
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
			&cli.StringFlag{
				Name:  "model",
				Usage: "AI model to use for suggestions",
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
			&cli.IntFlag{
				Name:  "concurrency",
				Usage: "Number of assets to enhance in parallel when processing a folder",
				Value: 5,
			},
			&cli.StringFlag{
				Name:  "system-prompt",
				Usage: "Custom system prompt to append to the default enhancement instructions",
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
		return printEnhanceError(output, errors.New("please provide a path to an asset file or folder"))
	}

	isDebugMode := isDebug != nil && *isDebug
	useTUI := output != "json" && !isDebugMode && term.IsTerminal(int(os.Stderr.Fd()))

	if isPathReferencingAsset(inputPath) {
		if useTUI {
			return enhanceSingleAssetWithTUI(ctx, c, inputPath, fs, output, isDebug)
		}
		return enhanceSingleAsset(ctx, c, inputPath, fs, output, isDebug, nil)
	}

	if isDir(inputPath) {
		return enhanceFolder(ctx, c, inputPath, fs, output, isDebug, useTUI)
	}

	return printEnhanceError(output, errors.New("please provide a path to an asset file or a folder containing assets"))
}

func enhanceSingleAssetWithTUI(ctx context.Context, c *cli.Command, inputPath string, fs afero.Fs, output string, isDebug *bool) error {
	name := filepath.Base(inputPath)

	// Read original content before modifications (for diff display after TUI)
	absPath, err := filepath.Abs(inputPath)
	if err != nil {
		return printEnhanceError(output, errors.Wrap(err, "failed to get absolute path"))
	}
	originalContent, err := afero.ReadFile(fs, absPath)
	if err != nil {
		return printEnhanceError(output, errors.Wrap(err, "failed to read original file content"))
	}

	// Save the real terminal and redirect all output so only the TUI writes to the terminal.
	// color.Output/color.Error are reassigned intentionally to suppress color printer output.
	realTerminal := os.Stderr
	devNull, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	savedStdout := os.Stdout
	savedColorOutput := color.Output
	savedColorError := color.Error //nolint:reassign
	os.Stdout = devNull
	os.Stderr = devNull
	color.Output = devNull //nolint:reassign
	color.Error = devNull  //nolint:reassign

	tui := NewEnhanceTUI(realTerminal, []string{name})
	tui.Start()
	tui.MarkRunning(name)

	enhErr := enhanceSingleAsset(ctx, c, inputPath, fs, output, isDebug, tui)

	if enhErr != nil {
		tui.MarkFailed(name)
	} else {
		tui.MarkDone(name)
	}

	// Brief pause so the user can see the final state
	time.Sleep(300 * time.Millisecond)
	tui.Stop()

	// Restore output after TUI is done
	os.Stdout = savedStdout
	os.Stderr = realTerminal
	color.Output = savedColorOutput //nolint:reassign
	color.Error = savedColorError   //nolint:reassign
	devNull.Close()

	if enhErr != nil {
		return printEnhanceError(output, enhErr)
	}

	// Show diff after TUI clears
	successPrinter.Printf("\n✓ Enhanced '%s'\n", name)
	showDiff(originalContent, absPath)
	return nil
}

func enhanceFolder(ctx context.Context, c *cli.Command, folderPath string, fs afero.Fs, output string, isDebug *bool, useTUI bool) error {
	concurrency := max(c.Int("concurrency"), 1)

	assetPaths := path.GetAllPossibleAssetPaths(folderPath, assetsDirectoryNames, pipeline.SupportedFileSuffixes)
	if len(assetPaths) == 0 {
		return printEnhanceError(output, errors.New("no assets found in the given folder"))
	}

	// Build asset names for the TUI
	assetNames := make([]string, len(assetPaths))
	for i, ap := range assetPaths {
		assetNames[i] = filepath.Base(ap)
	}

	var tui *EnhanceTUI
	var restoreOutput func()
	if useTUI {
		// Save the real terminal BEFORE redirecting output, same pattern as the run TUI.
		// This ensures only the TUI writes to the terminal — all other output (errorPrinter,
		// subprocess stderr, validation messages) goes to /dev/null.
		// We must also redirect color.Output and color.Error since the fatih/color package
		// caches its own writers at init time and doesn't follow os.Stdout/os.Stderr changes.
		realTerminal := os.Stderr
		devNull, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		savedStdout := os.Stdout
		savedColorOutput := color.Output
		savedColorError := color.Error //nolint:reassign
		os.Stdout = devNull
		os.Stderr = devNull
		color.Output = devNull //nolint:reassign
		color.Error = devNull  //nolint:reassign
		restoreOutput = func() {
			os.Stdout = savedStdout
			os.Stderr = realTerminal
			color.Output = savedColorOutput //nolint:reassign
			color.Error = savedColorError   //nolint:reassign
			devNull.Close()
		}

		tui = NewEnhanceTUI(realTerminal, assetNames)
		tui.Start()
	} else if output != "json" {
		infoPrinter.Printf("Found %d assets in '%s', enhancing with concurrency %d...\n\n", len(assetPaths), folderPath, concurrency)
	}

	type assetResult struct {
		name string
		err  error
	}

	var mu sync.Mutex
	results := make([]assetResult, 0, len(assetPaths))

	p := pool.New().WithMaxGoroutines(concurrency)
	for _, ap := range assetPaths {
		p.Go(func() {
			name := filepath.Base(ap)

			if tui != nil {
				tui.MarkRunning(name)
			} else if output != "json" {
				mu.Lock()
				infoPrinter.Printf("Starting enhancement for '%s'...\n", name)
				mu.Unlock()
			}

			err := enhanceSingleAsset(ctx, c, ap, fs, output, isDebug, tui)

			if tui != nil {
				if err != nil {
					tui.MarkFailed(name)
				} else {
					tui.MarkDone(name)
				}
			}

			mu.Lock()
			defer mu.Unlock()
			results = append(results, assetResult{name: name, err: err})
		})
	}

	p.Wait()

	if tui != nil {
		// Brief pause so the user can see the final state
		time.Sleep(500 * time.Millisecond)
		tui.Stop()
		restoreOutput()
	}

	// Count results
	var succeeded, failed int
	var enhanceErrors []string
	for _, r := range results {
		if r.err != nil {
			failed++
			enhanceErrors = append(enhanceErrors, fmt.Sprintf("%s: %v", r.name, r.err))
		} else {
			succeeded++
		}
	}

	if output == "json" {
		result := struct {
			Status    string   `json:"status"`
			Total     int      `json:"total"`
			Succeeded int      `json:"succeeded"`
			Failed    int      `json:"failed"`
			Errors    []string `json:"errors,omitempty"`
		}{
			Total:     len(assetPaths),
			Succeeded: succeeded,
			Failed:    failed,
			Errors:    enhanceErrors,
		}
		if failed > 0 {
			result.Status = "partial"
		} else {
			result.Status = "success"
		}
		jsonBytes, err := json.Marshal(result)
		if err != nil {
			return printEnhanceError(output, errors.Wrap(err, "failed to marshal result"))
		}
		fmt.Println(string(jsonBytes))
		if failed > 0 {
			return cli.Exit("", 1)
		}
		return nil
	}

	// Print per-asset results
	fmt.Println()
	for _, r := range results {
		if r.err != nil {
			errorPrinter.Printf("  ✗ %s\n", r.name)
		} else {
			successPrinter.Printf("  ✓ %s\n", r.name)
		}
	}
	fmt.Println()
	successPrinter.Printf("Enhancement complete: %d/%d assets succeeded", succeeded, len(assetPaths))
	if failed > 0 {
		fmt.Println()
		warningPrinter.Printf("\n%d assets failed:\n", failed)
		for _, e := range enhanceErrors {
			warningPrinter.Printf("  - %s\n", e)
		}
		return cli.Exit("", 1)
	}
	fmt.Println()

	return nil
}

func enhanceSingleAsset(ctx context.Context, c *cli.Command, assetPath string, fs afero.Fs, output string, isDebug *bool, tui *EnhanceTUI) error {
	quiet := tui != nil

	absAssetPath, err := filepath.Abs(assetPath)
	if err != nil {
		if quiet {
			return errors.Wrap(err, "failed to get absolute path")
		}
		return printEnhanceError(output, errors.Wrap(err, "failed to get absolute path"))
	}

	logPrefix := filepath.Base(assetPath)
	tuiKey := logPrefix // stable key for TUI lookups (always filepath.Base)

	// Read original file content before any modifications (for diff display)
	originalContent, err := afero.ReadFile(fs, absAssetPath)
	if err != nil {
		if quiet {
			return errors.Wrap(err, "failed to read original file content")
		}
		return printEnhanceError(output, errors.Wrap(err, "failed to read original file content"))
	}

	// Step 1: Fill columns from DB
	if tui != nil {
		tui.SetStep(tuiKey, "filling columns")
	} else if output != "json" {
		infoPrinter.Printf("[%s] Step 1/4: Filling columns from database...\n", logPrefix)
	}

	var pp *ppInfo
	pp, err = GetPipelineAndAsset(ctx, assetPath, fs, "")
	if err == nil {
		if pp.Asset.Name != "" {
			logPrefix = pp.Asset.Name
		}
		status, fillErr := fillColumnsFromDB(pp, fs, c.String("environment"), nil) //nolint:contextcheck
		if fillErr != nil && !quiet && output != "json" {
			warningPrinter.Printf("[%s] Warning: fill columns failed: %v\n", logPrefix, fillErr)
		} else if status == fillStatusUpdated && !quiet && output != "json" {
			infoPrinter.Printf("[%s]   Columns updated from database schema.\n", logPrefix)
		}
	} else if !quiet && output != "json" {
		warningPrinter.Printf("[%s] Warning: could not load asset for column filling: %v\n", logPrefix, err)
	}

	// Step 2: AI Enhancement
	if tui != nil {
		tui.SetStep(tuiKey, "enhancing...")
	} else if output != "json" {
		infoPrinter.Printf("[%s] Step 2/4: Enhancing asset with AI...\n", logPrefix)
	}

	pp, err = GetPipelineAndAsset(ctx, assetPath, fs, "")
	if err != nil {
		if quiet {
			return errors.Wrap(err, "failed to load asset")
		}
		return printEnhanceError(output, errors.Wrap(err, "failed to load asset"))
	}

	inferredName, nameErr := pp.Asset.GetNameIfItWasSetFromItsPath(pp.Pipeline)
	if nameErr != nil {
		if quiet {
			return errors.Wrap(nameErr, "failed to infer asset name from path")
		}
		return printEnhanceError(output, errors.Wrap(nameErr, "failed to infer asset name from path"))
	}
	if inferredName != "" && pp.Asset.Name == inferredName {
		pp.Asset.Name = inferredName
		if !quiet && output != "json" {
			infoPrinter.Printf("[%s]   Inferred asset name from path: %s\n", logPrefix, inferredName)
		}
		if persistErr := pp.Asset.Persist(fs); persistErr != nil {
			if quiet {
				return errors.Wrap(persistErr, "failed to persist asset with inferred name")
			}
			return printEnhanceError(output, errors.Wrap(persistErr, "failed to persist asset with inferred name"))
		}
	}

	providerType := enhance.ProviderClaude
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
		if quiet {
			return errors.New("cannot specify multiple provider flags (--claude, --opencode, --codex)")
		}
		return printEnhanceError(output, errors.New("cannot specify multiple provider flags (--claude, --opencode, --codex)"))
	}

	enhancer := enhance.NewEnhancer(providerType, c.String("model"))

	isDebugMode := isDebug != nil && *isDebug
	enhancer.SetDebug(isDebugMode)

	// Set TUI log writer for streaming output
	var logWriter *enhanceLogWriter
	if tui != nil && !isDebugMode {
		logWriter = tui.LogWriter(tuiKey)
		enhancer.SetOutput(logWriter)
	}

	if apiKey := getAnthropicAPIKey(fs, assetPath); apiKey != "" {
		enhancer.SetAPIKey(apiKey)
	}

	env := c.String("environment")
	if env != "" && !quiet && output != "json" {
		infoPrinter.Printf("[%s]   Using environment: %s\n", logPrefix, env)
	}

	if err := enhancer.EnsureCLI(); err != nil {
		if quiet {
			return err
		}
		return printEnhanceError(output, err)
	}

	// Pre-fetch table statistics
	if tui != nil {
		tui.SetStep(tuiKey, "fetching stats")
	}
	var tableSummaryJSON string
	fetchOutput := output
	if quiet {
		fetchOutput = "json" // suppress pre-fetch prints
	}
	if pp.Asset.Materialization.Type != "" && pp.Asset.Name != "" {
		tableSummaryJSON = preFetchTableSummary(ctx, fs, assetPath, pp.Asset, env, fetchOutput)
	}
	if tui != nil {
		tui.SetStep(tuiKey, "enhancing...")
	}

	customSystemPrompt := c.String("system-prompt")
	err = enhancer.EnhanceAsset(ctx, pp.Asset, pp.Pipeline.Name, tableSummaryJSON, customSystemPrompt)

	// Flush any remaining log output
	if logWriter != nil {
		logWriter.Flush()
	}

	if err != nil {
		if quiet {
			return errors.Wrap(err, "failed to enhance asset")
		}
		return printEnhanceError(output, errors.Wrap(err, "failed to enhance asset"))
	}

	// Step 3: Validate
	if tui != nil {
		tui.SetStep(tuiKey, "validating")
	} else if output != "json" {
		infoPrinter.Printf("[%s] Step 3/3: Validating asset...\n", logPrefix)
	}
	validateCmd := Lint(isDebug)
	args := []string{"validate", absAssetPath}
	if env := c.String("environment"); env != "" {
		args = append(args, "--environment", env)
	}

	// stdout/stderr are already redirected to /dev/null when TUI is active (set up in enhanceFolder/enhanceSingleAssetWithTUI)
	if err := validateCmd.Run(ctx, args); err != nil {
		if writeErr := afero.WriteFile(fs, absAssetPath, originalContent, 0o644); writeErr != nil {
			if quiet {
				return errors.Wrap(writeErr, fmt.Sprintf("validation failed and failed to restore original file: %v", err))
			}
			return printEnhanceError(output, errors.Wrap(writeErr, fmt.Sprintf("validation failed and failed to restore original file: %v", err)))
		}
		if quiet {
			return errors.Wrap(err, "validation failed, original file restored")
		}
		return printEnhanceError(output, errors.Wrap(err, "validation failed, original file restored"))
	}

	// Display results (only when not using TUI — TUI caller handles output)
	if !quiet {
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
			successPrinter.Printf("\n[%s] ✓ Enhanced '%s'\n", logPrefix, pp.Asset.Name)
			showDiff(originalContent, absAssetPath)
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

// showDiff displays a colored diff between the original content and the current file content.
func showDiff(originalContent []byte, filePath string) {
	newContent, err := os.ReadFile(filePath)
	if err != nil {
		warningPrinter.Printf("Warning: could not read file to display diff: %v\n", err)
		return
	}

	// If no changes, don't show anything
	if string(originalContent) == string(newContent) {
		fmt.Println("\nNo changes made.")
		return
	}

	fmt.Println("\nChanges:")
	printUnifiedDiff(string(originalContent), string(newContent))
}

// printUnifiedDiff prints a unified diff format with colors.
func printUnifiedDiff(original, modified string) {
	added := 0
	removed := 0

	dmp := diffmatchpatch.New()
	text1, text2, lineArray := dmp.DiffLinesToChars(original, modified)
	diffs := dmp.DiffMain(text1, text2, false)
	diffs = dmp.DiffCharsToLines(diffs, lineArray)

	for _, d := range diffs {
		text := d.Text
		if len(text) == 0 {
			continue
		}

		lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")

		for _, line := range lines {
			switch d.Type {
			case diffmatchpatch.DiffInsert:
				fmt.Printf("\033[32m+ %s\033[0m\n", line)
				added++
			case diffmatchpatch.DiffDelete:
				fmt.Printf("\033[31m- %s\033[0m\n", line)
				removed++
			case diffmatchpatch.DiffEqual:
				fmt.Printf("  %s\n", line)
			}
		}
	}

	fmt.Printf("\n\033[32m+%d additions\033[0m, \033[31m-%d deletions\033[0m\n", added, removed)
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

	ctx = context.WithValue(ctx, config.ConfigFilePathContextKey, filepath.Join(repoRoot.Path, ".bruin.yml"))
	ctx = context.WithValue(ctx, config.EnvironmentNameContextKey, cm.SelectedEnvironmentName)

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
		return &TableSummary{TableName: tableName, Error: config.NewConnectionNotFoundError(ctx, "", connectionName).Error()}
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
