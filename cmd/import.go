package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/oracle"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/telemetry"
	errors2 "github.com/pkg/errors"
	"github.com/spf13/afero"

	"github.com/urfave/cli/v3"
	datatransfer "cloud.google.com/go/bigquery/datatransfer/apiv1"
	"cloud.google.com/go/bigquery/datatransfer/apiv1/datatransferpb"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/bruin-data/bruin/pkg/bigquery"
)

func Import() *cli.Command {
	return &cli.Command{
		Name: "import",
		Commands: []*cli.Command{
			ImportDatabase(),
			ImportScheduledQueries(),
		},
	}
}

func ImportDatabase() *cli.Command {
	return &cli.Command{
		Name:      "database",
		Usage:     "Import database tables as Bruin assets",
		ArgsUsage: "[pipeline path]",
		Before:    telemetry.BeforeCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"c"},
				Usage:    "the name of the connection to use",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "schema",
				Aliases: []string{"s"},
				Usage:   "filter by specific schema name",
			},
			&cli.BoolFlag{
				Name:    "no-columns",
				Aliases: []string{"n"},
				Usage:   "skip filling column metadata from database schema",
			},
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "Target environment name as defined in .bruin.yml",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			pipelinePath := c.Args().Get(0)
			if pipelinePath == "" {
				return cli.Exit("pipeline path is required", 1)
			}

			connectionName := c.String("connection")
			schema := c.String("schema")
			noColumns := c.Bool("no-columns")
			environment := c.String("environment")
			configFile := c.String("config-file")

			return runImport(ctx, pipelinePath, connectionName, schema, !noColumns, environment, configFile)
		},
	}
}

func ImportScheduledQueries() *cli.Command {
	return &cli.Command{
		Name:      "scheduledqueries",
		Usage:     "Import BigQuery scheduled queries as Bruin assets",
		Description: `Import BigQuery scheduled queries from the Data Transfer Service as individual Bruin assets.

This command connects to BigQuery Data Transfer Service, lists all scheduled queries,
and presents them in an interactive terminal UI where you can:
- Navigate with arrow keys or j/k
- Select/deselect queries with space bar
- Toggle preview mode with 'p' to see query details in a dual-pane view
- Press Enter to import selected queries
- Press 'q' to quit without importing

Selected queries will be imported as .sql files in the current pipeline's assets folder.
Each imported asset will contain the original SQL query and appropriate metadata.

Example:
  bruin import scheduledqueries ./my-pipeline --connection my-bq-conn --env prod`,
		ArgsUsage: "[pipeline path]",
		Before:    telemetry.BeforeCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"c"},
				Usage:    "the name of the BigQuery connection to use",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "Target environment name as defined in .bruin.yml",
			},
			&cli.StringFlag{
				Name:    "config-file",
				EnvVars: []string{"BRUIN_CONFIG_FILE"},
				Usage:   "the path to the .bruin.yml file",
			},
			&cli.StringFlag{
				Name:    "project-id",
				Aliases: []string{"p"},
				Usage:   "BigQuery project ID (uses connection config if not specified)",
			},
			&cli.StringFlag{
				Name:    "location",
				Aliases: []string{"l"},
				Usage:   "BigQuery location/region (uses connection config if not specified)",
			},
		},
		Action: func(c *cli.Context) error {
			pipelinePath := c.Args().Get(0)
			if pipelinePath == "" {
				return cli.Exit("pipeline path is required", 1)
			}

			connectionName := c.String("connection")
			environment := c.String("environment")
			configFile := c.String("config-file")
			projectID := c.String("project-id")
			location := c.String("location")

			return runScheduledQueriesImport(c.Context, pipelinePath, connectionName, environment, configFile, projectID, location)
		},
	}
}

func runImport(ctx context.Context, pipelinePath, connectionName, schema string, fillColumns bool, environment, configFile string) error {
	fs := afero.NewOsFs()

	conn, err := getConnectionFromConfig(environment, connectionName, fs, configFile)
	if err != nil {
		return errors2.Wrap(err, "failed to get database connection")
	}

	summarizer, ok := conn.(interface {
		GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error)
	})
	if !ok {
		return fmt.Errorf("connection type '%s' does not support database summary", connectionName)
	}

	summary, err := summarizer.GetDatabaseSummary(ctx)
	if err != nil {
		return errors2.Wrap(err, "failed to retrieve database summary")
	}

	pathParts := strings.Split(pipelinePath, "/")
	if pathParts[len(pathParts)-1] == "pipeline.yml" || pathParts[len(pathParts)-1] == "pipeline.yaml" {
		pipelinePath = strings.Join(pathParts[:len(pathParts)-2], "/")
	}
	pipelineFound, err := GetPipelinefromPath(ctx, pipelinePath)
	if err != nil {
		return errors2.Wrap(err, "failed to get pipeline from path")
	}
	existingAssets := make(map[string]*pipeline.Asset, len(pipelineFound.Assets))
	for _, asset := range pipelineFound.Assets {
		existingAssets[asset.Name] = asset
	}

	assetsPath := filepath.Join(pipelinePath, "assets")
	assetType := determineAssetTypeFromConnection(connectionName, conn)
	totalTables := 0
	mergedTableCount := 0
	for _, schemaObj := range summary.Schemas {
		if schema != "" && !strings.EqualFold(schemaObj.Name, schema) {
			continue
		}
		for _, table := range schemaObj.Tables {
			createdAsset, err := createAsset(ctx, assetsPath, schemaObj.Name, table.Name, assetType, conn, fillColumns)
			if err != nil {
				return errors2.Wrapf(err, "failed to create asset for table %s.%s", schemaObj.Name, table.Name)
			}

			assetName := fmt.Sprintf("%s.%s", strings.ToLower(schemaObj.Name), strings.ToLower(table.Name))
			if existingAssets[assetName] == nil {
				schemaFolder := filepath.Join(assetsPath, strings.ToLower(schemaObj.Name))
				if err := fs.MkdirAll(schemaFolder, 0o755); err != nil {
					return errors2.Wrapf(err, "failed to create schema directory %s", schemaFolder)
				}

				err = createdAsset.Persist(fs)
				if err != nil {
					return err
				}
				existingAssets[assetName] = createdAsset
				totalTables++
			} else {
				existingAsset := existingAssets[assetName]
				existingColumns := make(map[string]pipeline.Column, len(existingAsset.Columns))
				for _, column := range existingAsset.Columns {
					existingColumns[column.Name] = column
				}
				for _, c := range createdAsset.Columns {
					if _, ok := existingColumns[c.Name]; !ok {
						existingAsset.Columns = append(existingAsset.Columns, c)
					}
				}
				err = existingAsset.Persist(fs)
				mergedTableCount++
				if err != nil {
					return err
				}
			}
		}
	}

	filterDesc := ""
	if schema != "" {
		filterDesc = fmt.Sprintf(" (schema: %s)", schema)
	}

	fmt.Printf("Imported %d tables and Merged %d from data warehouse '%s'%s into pipeline '%s'\n",
		totalTables, mergedTableCount, summary.Name, filterDesc, pipelinePath)

	return nil
}

func fillAssetColumnsFromDB(ctx context.Context, asset *pipeline.Asset, conn interface{}, schemaName, tableName string) error {
	// Check if connection supports schema introspection
	querier, ok := conn.(interface {
		SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error)
	})
	if !ok {
		return errors2.New("connection does not support schema introspection")
	}

	// Query to get column information
	queryStr := fmt.Sprintf("SELECT * FROM %s.%s WHERE 1=0 LIMIT 0", schemaName, tableName)

	if _, ok := conn.(*mssql.DB); ok {
		queryStr = "SELECT TOP 0 * FROM " + schemaName + "." + tableName
	} else if _, ok := conn.(*oracle.Client); ok {
		queryStr = "SELECT * FROM " + schemaName + "." + tableName + " WHERE 1=0"
	}
	q := &query.Query{Query: queryStr}
	result, err := querier.SelectWithSchema(ctx, q)
	if err != nil {
		return errors2.Wrapf(err, "failed to query columns for table %s.%s", schemaName, tableName)
	}

	if len(result.Columns) == 0 {
		return fmt.Errorf("no columns found for table %s.%s", schemaName, tableName)
	}

	// Skip special column names (from patch.go)
	skipColumns := map[string]bool{
		"_IS_CURRENT":  true,
		"_VALID_UNTIL": true,
		"_VALID_FROM":  true,
	}

	// Create column definitions
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

	asset.Columns = columns
	return nil
}

func createAsset(ctx context.Context, assetsPath, schemaName, tableName string, assetType pipeline.AssetType, conn interface{}, fillColumns bool) (*pipeline.Asset, error) {
	// Create schema subfolder
	schemaFolder := filepath.Join(assetsPath, strings.ToLower(schemaName))

	fileName := strings.ToLower(tableName) + ".asset.yml"
	filePath := filepath.Join(schemaFolder, fileName)
	asset := &pipeline.Asset{
		Type: assetType,
		ExecutableFile: pipeline.ExecutableFile{
			Name: fileName,
			Path: filePath,
		},
		Description: fmt.Sprintf("Imported table %s.%s", schemaName, tableName),
	}

	if fillColumns {
		err := fillAssetColumnsFromDB(ctx, asset, conn, schemaName, tableName)
		if err != nil {
			warningPrinter.Printf("Warning: Could not fill columns for %s.%s: %v\n", schemaName, tableName, err)
			if err != nil {
				return nil, err
			}
		}
	}

	return asset, nil
}

func determineAssetTypeFromConnection(connectionName string, conn interface{}) pipeline.AssetType {
	// First, try to determine from the actual connection type
	if _, ok := conn.(interface {
		GetDatabaseSummary(ctx context.Context) ([]string, error)
	}); ok {
		connType := fmt.Sprintf("%T", conn)
		if strings.Contains(connType, "snowflake") {
			return pipeline.AssetTypeSnowflakeSource
		}
		if strings.Contains(connType, "bigquery") {
			return pipeline.AssetTypeBigquerySource
		}
		if strings.Contains(connType, "postgres") {
			return pipeline.AssetTypePostgresSource
		}
		if strings.Contains(connType, "athena") {
			return pipeline.AssetTypeAthenaSource
		}
		if strings.Contains(connType, "databricks") {
			return pipeline.AssetTypeDatabricksSource
		}
		if strings.Contains(connType, "duckdb") {
			return pipeline.AssetTypeDuckDBSource
		}
		if strings.Contains(connType, "clickhouse") {
			return pipeline.AssetTypeClickHouseSource
		}
		if strings.Contains(connType, "oracle") {
			return pipeline.AssetTypeOracleSource
		}
	}

	// Fallback: try to detect the connection type from the connection name
	connectionLower := strings.ToLower(connectionName)

	if strings.Contains(connectionLower, "snowflake") || strings.Contains(connectionLower, "sf") {
		return pipeline.AssetTypeSnowflakeSource
	}
	if strings.Contains(connectionLower, "bigquery") || strings.Contains(connectionLower, "bq") {
		return pipeline.AssetTypeBigquerySource
	}
	if strings.Contains(connectionLower, "postgres") || strings.Contains(connectionLower, "pg") {
		return pipeline.AssetTypePostgresSource
	}
	if strings.Contains(connectionLower, "redshift") || strings.Contains(connectionLower, "rs") {
		return pipeline.AssetTypeRedshiftSource
	}
	if strings.Contains(connectionLower, "athena") {
		return pipeline.AssetTypeAthenaSource
	}
	if strings.Contains(connectionLower, "databricks") {
		return pipeline.AssetTypeDatabricksSource
	}
	if strings.Contains(connectionLower, "duckdb") {
		return pipeline.AssetTypeDuckDBSource
	}
	if strings.Contains(connectionLower, "clickhouse") {
		return pipeline.AssetTypeClickHouseSource
	}
	if strings.Contains(connectionLower, "synapse") {
		return pipeline.AssetTypeSynapseSource
	}
	if strings.Contains(connectionLower, "mssql") || strings.Contains(connectionLower, "sqlserver") {
		return pipeline.AssetTypeMsSQLSource
	}
	if strings.Contains(connectionLower, "oracle") {
		return pipeline.AssetTypeOracleSource
	}

	// Default to Snowflake if we can't determine the type
	return pipeline.AssetTypeSnowflakeSource
}

func GetPipelinefromPath(ctx context.Context, inputPath string) (*pipeline.Pipeline, error) {
	pipelinePath, err := path.GetPipelineRootFromTask(inputPath, PipelineDefinitionFiles)
	if err != nil {
		errorPrinter.Printf("Failed to find the pipeline this task belongs to: '%s'\n", inputPath)
		return nil, err
	}

	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate())
	if err != nil {
		errorPrinter.Println("failed to get the pipeline this asset belongs to, are you sure you have referred the right path?")
		errorPrinter.Println("\nHint: You need to run this command with a path to the asset file itself directly.")
		return nil, err
	}
	return foundPipeline, nil
}

// Scheduled query data structures
type ScheduledQuery struct {
	Name        string
	DisplayName string
	Query       string
	Schedule    string
	Dataset     string
	Config      *datatransferpb.TransferConfig
}

// Bubbletea model for scheduled query selection
type scheduledQueryModel struct {
	queries       []ScheduledQuery
	selected      map[int]bool
	cursor        int
	showPreview   bool
	windowWidth   int
	windowHeight  int
	quitting      bool
	err           error
}

func (m scheduledQueryModel) Init() tea.Cmd {
	return nil
}

func (m scheduledQueryModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.quitting = true
			return m, tea.Quit
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(m.queries)-1 {
				m.cursor++
			}
		case " ":
			// Toggle selection
			if m.selected == nil {
				m.selected = make(map[int]bool)
			}
			m.selected[m.cursor] = !m.selected[m.cursor]
		case "enter":
			// Finish selection
			return m, tea.Quit
		case "p":
			// Toggle preview mode
			m.showPreview = !m.showPreview
		}
	case tea.WindowSizeMsg:
		m.windowWidth = msg.Width
		m.windowHeight = msg.Height
	}
	return m, nil
}

func (m scheduledQueryModel) View() string {
	if m.quitting {
		return ""
	}

	if m.err != nil {
		return fmt.Sprintf("Error: %v\n", m.err)
	}

	if len(m.queries) == 0 {
		return "No scheduled queries found.\n"
	}

	var s strings.Builder
	s.WriteString("Select scheduled queries to import (Space to select, Enter to confirm, P to toggle preview, Q to quit):\n\n")

	if m.showPreview && m.windowWidth > 80 {
		// Dual pane view
		return m.dualPaneView()
	}

	// Single pane view
	for i, query := range m.queries {
		cursor := " "
		if m.cursor == i {
			cursor = ">"
		}

		checked := " "
		if m.selected[i] {
			checked = "✓"
		}

		displayText := query.DisplayName
		if displayText == "" {
			displayText = query.Name
		}
		if displayText == "" {
			displayText = fmt.Sprintf("Query %d", i+1)
		}

		s.WriteString(fmt.Sprintf("%s [%s] %s\n", cursor, checked, displayText))
		if query.Schedule != "" {
			s.WriteString(fmt.Sprintf("     Schedule: %s\n", query.Schedule))
		}
		if query.Dataset != "" {
			s.WriteString(fmt.Sprintf("     Dataset: %s\n", query.Dataset))
		}
		s.WriteString("\n")
	}

	s.WriteString("\nPress 'p' to toggle preview mode")
	return s.String()
}

func (m scheduledQueryModel) dualPaneView() string {
	var s strings.Builder
	
	leftWidth := m.windowWidth / 2 - 2
	rightWidth := m.windowWidth - leftWidth - 3

	s.WriteString("Select scheduled queries to import (Space to select, Enter to confirm, P to toggle preview, Q to quit):\n\n")

	// Split view
	lines := make([]string, 0)
	
	// Left pane - query list
	for i, query := range m.queries {
		cursor := " "
		if m.cursor == i {
			cursor = ">"
		}

		checked := " "
		if m.selected[i] {
			checked = "✓"
		}

		displayText := query.DisplayName
		if displayText == "" {
			displayText = query.Name
		}
		if displayText == "" {
			displayText = fmt.Sprintf("Query %d", i+1)
		}

		// Truncate if too long
		if len(displayText) > leftWidth-6 {
			displayText = displayText[:leftWidth-9] + "..."
		}

		lines = append(lines, fmt.Sprintf("%s [%s] %s", cursor, checked, displayText))
	}

	// Right pane - query details
	rightContent := ""
	if m.cursor < len(m.queries) {
		query := m.queries[m.cursor]
		rightContent = fmt.Sprintf("Query Details:\n\nDisplay Name: %s\nName: %s\nSchedule: %s\nDataset: %s\n\nQuery:\n%s",
			query.DisplayName, query.Name, query.Schedule, query.Dataset, query.Query)
	}

	rightLines := strings.Split(rightContent, "\n")

	maxLines := max(len(lines), len(rightLines))
	
	for i := 0; i < maxLines; i++ {
		leftLine := ""
		if i < len(lines) {
			leftLine = lines[i]
		}
		// Pad left line to left width
		if len(leftLine) < leftWidth {
			leftLine += strings.Repeat(" ", leftWidth-len(leftLine))
		}

		rightLine := ""
		if i < len(rightLines) {
			rightLine = rightLines[i]
			// Truncate right line if too long
			if len(rightLine) > rightWidth {
				rightLine = rightLine[:rightWidth-3] + "..."
			}
		}

		s.WriteString(fmt.Sprintf("%s | %s\n", leftLine, rightLine))
	}

	return s.String()
}

func runScheduledQueriesImport(ctx context.Context, pipelinePath, connectionName, environment, configFile, projectID, location string) error {
	fs := afero.NewOsFs()

	// Get BigQuery connection
	conn, err := getConnectionFromConfig(environment, connectionName, fs, configFile)
	if err != nil {
		return errors2.Wrap(err, "failed to get BigQuery connection")
	}

	// Ensure it's a BigQuery connection
	bqClient, ok := conn.(*bigquery.Client)
	if !ok {
		return fmt.Errorf("connection '%s' is not a BigQuery connection", connectionName)
	}

	// Use project ID from connection config if not provided
	if projectID == "" {
		// Access the project ID from the client's config
		if bqClient != nil {
			projectID = bqClient.ProjectID()
		}
		if projectID == "" {
			return fmt.Errorf("could not determine project ID from connection, please specify --project-id")
		}
	}

	// Create Data Transfer Service client using the same credentials as BigQuery
	dtClient, err := datatransfer.NewClient(ctx)
	if err != nil {
		return errors2.Wrap(err, "failed to create BigQuery Data Transfer Service client")
	}
	defer dtClient.Close()

	// List scheduled queries
	queries, err := listScheduledQueries(ctx, dtClient, projectID, location)
	if err != nil {
		return errors2.Wrap(err, "failed to list scheduled queries")
	}

	if len(queries) == 0 {
		fmt.Println("No scheduled queries found in the project.")
		return nil
	}

	// Show interactive selection UI
	selected, err := showScheduledQuerySelector(queries)
	if err != nil {
		return errors2.Wrap(err, "failed to show query selector")
	}

	if len(selected) == 0 {
		fmt.Println("No queries selected.")
		return nil
	}

	// Import selected queries
	return importSelectedQueries(ctx, pipelinePath, selected, fs)
}

func listScheduledQueries(ctx context.Context, client *datatransfer.Client, projectID, location string) ([]ScheduledQuery, error) {
	parent := fmt.Sprintf("projects/%s", projectID)
	if location != "" {
		parent = fmt.Sprintf("projects/%s/locations/%s", projectID, location)
	}

	req := &datatransferpb.ListTransferConfigsRequest{
		Parent: parent,
		DataSourceIds: []string{"scheduled_query"}, // Filter for scheduled queries only
	}

	var queries []ScheduledQuery
	it := client.ListTransferConfigs(ctx, req)
	
	for {
		config, err := it.Next()
		if err != nil {
			if err.Error() == "no more items in iterator" {
				break
			}
			return nil, err
		}

		// Extract query parameters
		queryText := ""
		if config.Params != nil && config.Params.Fields != nil {
			if queryField, ok := config.Params.Fields["query"]; ok {
				queryText = queryField.GetStringValue()
			}
		}

		query := ScheduledQuery{
			Name:        config.Name,
			DisplayName: config.DisplayName,
			Query:       queryText,
			Schedule:    config.Schedule,
			Dataset:     config.GetDestinationDatasetId(),
			Config:      config,
		}

		queries = append(queries, query)
	}

	return queries, nil
}

func showScheduledQuerySelector(queries []ScheduledQuery) ([]ScheduledQuery, error) {
	model := scheduledQueryModel{
		queries:  queries,
		selected: make(map[int]bool),
	}

	p := tea.NewProgram(model, tea.WithAltScreen())
	finalModel, err := p.Run()
	if err != nil {
		return nil, err
	}

	final := finalModel.(scheduledQueryModel)
	if final.quitting {
		var selected []ScheduledQuery
		for i, isSelected := range final.selected {
			if isSelected && i < len(queries) {
				selected = append(selected, queries[i])
			}
		}
		return selected, nil
	}

	return nil, nil
}

func importSelectedQueries(ctx context.Context, pipelinePath string, queries []ScheduledQuery, fs afero.Fs) error {
	// Ensure pipeline path and get pipeline info
	pathParts := strings.Split(pipelinePath, "/")
	if pathParts[len(pathParts)-1] == "pipeline.yml" || pathParts[len(pathParts)-1] == "pipeline.yaml" {
		pipelinePath = strings.Join(pathParts[:len(pathParts)-2], "/")
	}

	pipelineFound, err := GetPipelinefromPath(ctx, pipelinePath)
	if err != nil {
		return errors2.Wrap(err, "failed to get pipeline from path")
	}

	existingAssets := make(map[string]*pipeline.Asset, len(pipelineFound.Assets))
	for _, asset := range pipelineFound.Assets {
		existingAssets[asset.Name] = asset
	}

	assetsPath := filepath.Join(pipelinePath, "assets")
	importedCount := 0

	for _, query := range queries {
		// Create asset from scheduled query
		asset, err := createAssetFromScheduledQuery(query, assetsPath)
		if err != nil {
			return errors2.Wrapf(err, "failed to create asset for scheduled query '%s'", query.DisplayName)
		}

		// Check if asset already exists
		if existingAssets[asset.Name] != nil {
			fmt.Printf("Asset '%s' already exists, skipping...\n", asset.Name)
			continue
		}

		// Create directory for the asset
		assetDir := filepath.Dir(asset.ExecutableFile.Path)
		if err := fs.MkdirAll(assetDir, 0o755); err != nil {
			return errors2.Wrapf(err, "failed to create directory %s", assetDir)
		}

		// Save the asset
		err = asset.Persist(fs)
		if err != nil {
			return errors2.Wrapf(err, "failed to save asset '%s'", asset.Name)
		}

		importedCount++
		fmt.Printf("Imported scheduled query '%s' as asset '%s'\n", query.DisplayName, asset.Name)
	}

	fmt.Printf("\nSuccessfully imported %d scheduled queries into pipeline '%s'\n", importedCount, pipelinePath)
	return nil
}

func createAssetFromScheduledQuery(query ScheduledQuery, assetsPath string) (*pipeline.Asset, error) {
	// Generate a safe filename from the display name or use a default
	assetName := query.DisplayName
	if assetName == "" {
		assetName = "scheduled_query"
	}
	
	// Sanitize the asset name for filename use
	assetName = strings.ToLower(assetName)
	assetName = strings.ReplaceAll(assetName, " ", "_")
	assetName = strings.ReplaceAll(assetName, "-", "_")
	// Remove any characters that aren't alphanumeric or underscore
	var sanitized strings.Builder
	for _, r := range assetName {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			sanitized.WriteRune(r)
		}
	}
	assetName = sanitized.String()
	
	if assetName == "" {
		assetName = "scheduled_query"
	}

	fileName := assetName + ".sql"
	filePath := filepath.Join(assetsPath, fileName)

	// Create the asset
	asset := &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Name: fileName,
			Path: filePath,
			Content: query.Query, // Add the SQL query content
		},
		Description: fmt.Sprintf("Imported from scheduled query: %s", query.DisplayName),
	}

	// Set the materialization if we have dataset info
	if query.Dataset != "" {
		asset.Materialization = pipeline.Materialization{
			Type: pipeline.MaterializationTypeTable,
		}
	}

	return asset, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
