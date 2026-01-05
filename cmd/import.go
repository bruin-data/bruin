package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	datatransfer "cloud.google.com/go/bigquery/datatransfer/apiv1"
	"cloud.google.com/go/bigquery/datatransfer/apiv1/datatransferpb"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/oracle"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/tableau"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	errors2 "github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
	"google.golang.org/api/iterator"
)

func Import() *cli.Command {
	return &cli.Command{
		Name: "import",
		Commands: []*cli.Command{
			ImportDatabase(),
			ImportScheduledQueries(),
			ImportTableauDashboards(),
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
			&cli.StringSliceFlag{
				Name:  "schemas",
				Usage: "filter by multiple schema names, only supported for BigQuery (e.g., --schemas public --schemas analytics)",
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
			schemas := c.StringSlice("schemas")
			noColumns := c.Bool("no-columns")
			environment := c.String("environment")
			configFile := c.String("config-file")

			// Validate that both --schema and --schemas are not used together
			if schema != "" && len(schemas) > 0 {
				return cli.Exit("cannot use both --schema and --schemas flags together", 1)
			}

			return runImport(ctx, pipelinePath, connectionName, schema, schemas, !noColumns, environment, configFile)
		},
	}
}

func ImportScheduledQueries() *cli.Command {
	return &cli.Command{
		Name:  "bq-scheduled-queries",
		Usage: "Import BigQuery scheduled queries as Bruin assets",
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
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
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
		Action: func(ctx context.Context, c *cli.Command) error {
			pipelinePath := c.Args().Get(0)
			if pipelinePath == "" {
				return cli.Exit("pipeline path is required", 1)
			}

			connectionName := c.String("connection")
			environment := c.String("environment")
			configFile := c.String("config-file")
			projectID := c.String("project-id")
			location := c.String("location")

			return runScheduledQueriesImport(ctx, pipelinePath, connectionName, environment, configFile, projectID, location)
		},
	}
}

type importWarning struct {
	tableName string
	message   string
}

func runImport(ctx context.Context, pipelinePath, connectionName, schema string, schemas []string, fillColumns bool, environment, configFile string) error {
	fs := afero.NewOsFs()

	conn, err := getConnectionFromConfigWithContext(ctx, environment, connectionName, fs, configFile)
	if err != nil {
		return errors2.Wrap(err, "failed to get database connection")
	}

	var summary *ansisql.DBDatabase

	// Build schema list from --schema or --schemas flags
	schemaList := schemas
	if schema != "" {
		schemaList = []string{schema}
	}

	// If schema(s) specified, try to use GetDatabaseSummaryForSchemas if available
	if len(schemaList) > 0 {
		if schemaSummarizer, ok := conn.(interface {
			GetDatabaseSummaryForSchemas(ctx context.Context, schemas []string) (*ansisql.DBDatabase, error)
		}); ok {
			summary, err = schemaSummarizer.GetDatabaseSummaryForSchemas(ctx, schemaList)
			if err != nil {
				return errors2.Wrap(err, "failed to retrieve database summary for specified schemas")
			}
		}
	}

	// Fall back to GetDatabaseSummary if no schema specified or connection doesn't support filtered summary
	if summary == nil {
		summarizer, ok := conn.(interface {
			GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error)
		})
		if !ok {
			return fmt.Errorf("connection type '%s' does not support database summary", connectionName)
		}

		summary, err = summarizer.GetDatabaseSummary(ctx)
		if err != nil {
			return errors2.Wrap(err, "failed to retrieve database summary")
		}
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
		existingAssets[strings.ToLower(asset.Name)] = asset
	}

	assetsPath := filepath.Join(pipelinePath, "assets")
	assetType := determineAssetTypeFromConnection(connectionName, conn)
	totalTables := 0
	mergedTableCount := 0
	var warnings []importWarning

	for _, schemaObj := range summary.Schemas {
		if schema != "" && !strings.EqualFold(schemaObj.Name, schema) {
			continue
		}
		for _, table := range schemaObj.Tables {
			fullName := fmt.Sprintf("%s.%s", schemaObj.Name, table.Name)
			createdAsset, warning := createAsset(ctx, assetsPath, schemaObj.Name, table.Name, assetType, conn, fillColumns)
			if warning != "" {
				warnings = append(warnings, importWarning{tableName: fullName, message: warning})
			}

			if createdAsset == nil {
				continue
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
	} else if len(schemas) > 0 {
		filterDesc = fmt.Sprintf(" (schemas: %s)", strings.Join(schemas, ", "))
	}

	fmt.Printf("Imported %d tables and Merged %d from data warehouse '%s'%s into pipeline '%s'\n",
		totalTables, mergedTableCount, summary.Name, filterDesc, pipelinePath)

	if len(warnings) > 0 {
		fmt.Printf("\nWarnings encountered during import (%d tables affected):\n", len(warnings))
		for _, w := range warnings {
			warningPrinter.Printf("  - %s: %s\n", w.tableName, w.message)
		}
		fmt.Println()
	}

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

	// Build fully qualified table name
	fullTableName := schemaName + "." + tableName

	// For PostgreSQL, quote the schema and table names to handle case-sensitive names
	if _, ok := conn.(*postgres.Client); ok {
		fullTableName = postgres.QuoteIdentifier(fullTableName)
	}

	// For MSSQL, quote the schema and table names to handle special characters
	if _, ok := conn.(*mssql.DB); ok {
		fullTableName = mssql.QuoteIdentifier(fullTableName)
	}

	// Query to get column information
	queryStr := fmt.Sprintf("SELECT * FROM %s WHERE 1=0 LIMIT 0", fullTableName)

	if _, ok := conn.(*mssql.DB); ok {
		queryStr = "SELECT TOP 0 * FROM " + fullTableName
	} else if _, ok := conn.(*oracle.Client); ok {
		queryStr = "SELECT * FROM " + fullTableName + " WHERE 1=0"
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

func createAsset(ctx context.Context, assetsPath, schemaName, tableName string, assetType pipeline.AssetType, conn interface{}, fillColumns bool) (*pipeline.Asset, string) {
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
			return asset, fmt.Sprintf("Could not fill columns: %v", err)
		}
	}

	return asset, ""
}

// maxInt returns the larger of two integers.
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
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
		if strings.Contains(connType, "mssql") {
			return pipeline.AssetTypeMsSQLSource
		}
		if strings.Contains(connType, "synapse") {
			return pipeline.AssetTypeSynapseSource
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

// Scheduled query data structures.
type ScheduledQuery struct {
	Name        string
	DisplayName string
	Query       string
	Schedule    string
	Dataset     string
	Config      *datatransferpb.TransferConfig
}

// scheduledQueryItem implements list.Item interface.
type scheduledQueryItem struct {
	query    ScheduledQuery
	selected bool
}

func (i scheduledQueryItem) Title() string {
	if i.query.DisplayName != "" {
		return i.query.DisplayName
	}
	if i.query.Name != "" {
		return i.query.Name
	}
	return "Unnamed Query"
}

func (i scheduledQueryItem) Description() string {
	var parts []string
	if i.query.Schedule != "" {
		parts = append(parts, "Schedule: "+i.query.Schedule)
	}
	if i.query.Dataset != "" {
		parts = append(parts, "Dataset: "+i.query.Dataset)
	}
	if len(parts) == 0 {
		return "No additional details"
	}
	return strings.Join(parts, " | ")
}

func (i scheduledQueryItem) FilterValue() string {
	return i.Title()
}

// customDelegate implements list.ItemDelegate with selection indicators.
type customDelegate struct {
	list.DefaultDelegate
	selectedItems map[int]bool
}

func (d customDelegate) Render(w io.Writer, m list.Model, index int, listItem list.Item) {
	// Try to render as scheduledQueryItem
	if sqItem, ok := listItem.(scheduledQueryItem); ok {
		d.renderScheduledQueryItem(w, m, index, sqItem)
		return
	}

	// Try to render as tableauDashboardItem
	if tdItem, ok := listItem.(tableauDashboardItem); ok {
		d.renderTableauDashboardItem(w, m, index, tdItem)
		return
	}

	// Fallback to default rendering
	d.DefaultDelegate.Render(w, m, index, listItem)
}

func (d customDelegate) renderScheduledQueryItem(w io.Writer, m list.Model, index int, item scheduledQueryItem) {
	// Check if this item is selected
	isSelected := d.selectedItems[index]
	isCurrent := index == m.Index()

	// Create selection indicator with better styling
	checkbox := "[ ]"
	if isSelected {
		checkbox = "[x]"
	}

	// Create clean title and description
	title := item.Title()
	desc := item.Description()

	// Apply consistent styling
	if isCurrent {
		// Current item - purple background
		styledLine := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(lipgloss.Color("#7C3AED")).
			Width(m.Width()-4).
			Padding(0, 1).
			MarginTop(1).
			Render(fmt.Sprintf("%s %s", checkbox, title))

		descLine := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#9CA3AF")).
			Padding(0, 5).
			Render(desc)

		fmt.Fprintf(w, "%s\n%s", styledLine, descLine)
	} else {
		// Non-current item
		titleColor := colorGray
		if isSelected {
			titleColor = colorSuccess
		}

		titleLine := lipgloss.NewStyle().
			Foreground(lipgloss.Color(titleColor)).
			Padding(0, 1).
			MarginTop(1).
			Render(fmt.Sprintf("%s %s", checkbox, title))

		descLine := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#9CA3AF")).
			Padding(0, 5).
			Render(desc)

		fmt.Fprintf(w, "%s\n%s", titleLine, descLine)
	}
}

func (d customDelegate) renderTableauDashboardItem(w io.Writer, m list.Model, index int, item tableauDashboardItem) {
	// Check if this item is selected
	isSelected := d.selectedItems[index]
	isCurrent := index == m.Index()

	// Create selection indicator with better styling
	checkbox := "[ ]"
	if isSelected {
		checkbox = "[x]"
	}

	// Create clean title and description
	title := item.Title()
	desc := item.Description()

	// Apply consistent styling
	if isCurrent {
		// Current item - purple background
		styledLine := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(lipgloss.Color("#7C3AED")).
			Width(m.Width()-4).
			Padding(0, 1).
			MarginTop(1).
			Render(fmt.Sprintf("%s %s", checkbox, title))

		descLine := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#D8B4FE")).
			Background(lipgloss.Color("#7C3AED")).
			Width(m.Width()-4).
			Padding(0, 5).
			Render(desc)

		fmt.Fprintf(w, "%s\n%s", styledLine, descLine)
	} else {
		// Non-current item
		titleColor := colorGray
		if isSelected {
			titleColor = colorSuccess
		}

		titleLine := lipgloss.NewStyle().
			Foreground(lipgloss.Color(titleColor)).
			Padding(0, 1).
			MarginTop(1).
			Render(fmt.Sprintf("%s %s", checkbox, title))

		descLine := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#9CA3AF")).
			Padding(0, 5).
			Render(desc)

		fmt.Fprintf(w, "%s\n%s", titleLine, descLine)
	}
}

// Bubbletea model for scheduled query selection.
type scheduledQueryModel struct {
	queries       []ScheduledQuery
	list          list.Model
	delegate      customDelegate
	rightViewport viewport.Model
	selected      map[int]bool
	windowWidth   int
	windowHeight  int
	quitting      bool
	confirmed     bool // true if user pressed Enter, false if they pressed q
	err           error
	focusedPane   int // 0 for left pane, 1 for right pane
}

// Color constants for UI styling.
const (
	colorGray      = "#374151"
	colorBlue      = "#4F46E5"
	colorOrange    = "#FF6B35"
	colorGreen     = "#10B981"
	colorPurple    = "#A78BFA"
	colorLightGray = "#9CA3AF"
	colorDarkGray  = "#6B7280"
	colorDarkBg    = "#1F2937"
	colorSuccess   = "#059669"
)

// Styles for the UI.
var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color(colorOrange)).
			MarginTop(1).
			MarginBottom(1)

	// Panel styles.
	leftPanelStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(colorBlue)).
			Padding(1)

	rightPanelStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(colorOrange)).
			Padding(0, 1)

	// Status bar style.
	statusBarStyle = lipgloss.NewStyle().
			Background(lipgloss.Color(colorDarkBg)).
			Foreground(lipgloss.Color(colorLightGray)).
			Padding(0, 1)
)

func (m *scheduledQueryModel) Init() tea.Cmd {
	return nil
}

func (m *scheduledQueryModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			m.quitting = true
			return m, tea.Quit
		case "tab":
			// Switch focus between left and right panes
			m.focusedPane = (m.focusedPane + 1) % 2
		case " ":
			// Only allow selection when focused on left pane
			if m.focusedPane == 0 {
				currentIndex := m.list.Index()
				if m.selected == nil {
					m.selected = make(map[int]bool)
				}
				m.selected[currentIndex] = !m.selected[currentIndex]

				// Update the delegate's selected items map for visual feedback
				m.delegate.selectedItems[currentIndex] = m.selected[currentIndex]
				m.list.SetDelegate(m.delegate)

				m.updateRightPanelContent()
			}
		case "enter":
			// Finish selection
			m.confirmed = true
			m.quitting = true
			return m, tea.Quit
		case "a":
			// Select all queries
			if m.selected == nil {
				m.selected = make(map[int]bool)
			}
			for i := range m.queries {
				m.selected[i] = true
				m.delegate.selectedItems[i] = true
			}
			m.list.SetDelegate(m.delegate)
			m.updateRightPanelContent()
		case "n":
			// Deselect all queries
			if m.selected == nil {
				m.selected = make(map[int]bool)
			}
			for i := range m.queries {
				m.selected[i] = false
				m.delegate.selectedItems[i] = false
			}
			m.list.SetDelegate(m.delegate)
			m.updateRightPanelContent()
		}

	case tea.WindowSizeMsg:
		m.windowWidth = msg.Width
		m.windowHeight = msg.Height

		// Calculate sizes for split-pane layout
		leftWidth := m.windowWidth/2 - 4
		rightWidth := m.windowWidth - leftWidth - 8
		viewportHeight := m.windowHeight - 14 // Account for all header content, extra spacing, and status bar

		if leftWidth < 30 {
			leftWidth = 30
		}
		if rightWidth < 40 {
			rightWidth = 40
		}
		if viewportHeight < 10 {
			viewportHeight = 10
		}

		// Update list size
		m.list.SetSize(leftWidth, viewportHeight)

		// Initialize or update right viewport
		if m.rightViewport.Width == 0 {
			m.rightViewport = viewport.New(rightWidth, viewportHeight)
			m.updateRightPanelContent()
		} else {
			m.rightViewport.Width = rightWidth
			m.rightViewport.Height = viewportHeight
			m.updateRightPanelContent()
		}
	}

	// Update list only when focused on left pane
	if m.focusedPane == 0 {
		m.list, cmd = m.list.Update(msg)
		cmds = append(cmds, cmd)
	}

	// Update right viewport only when focused on right pane
	if m.focusedPane == 1 {
		m.rightViewport, cmd = m.rightViewport.Update(msg)
		cmds = append(cmds, cmd)
	}

	// Update right panel when list selection changes
	if msg, ok := msg.(tea.KeyMsg); ok {
		if msg.String() == "up" || msg.String() == "down" || msg.String() == "k" || msg.String() == "j" {
			m.updateRightPanelContent()
		}
	}

	return m, tea.Batch(cmds...)
}

func (m *scheduledQueryModel) updateRightPanelContent() {
	if m.rightViewport.Width == 0 {
		return // Viewport not initialized yet
	}

	currentIndex := m.list.Index()
	if currentIndex >= len(m.queries) {
		m.rightViewport.SetContent(lipgloss.NewStyle().
			Foreground(lipgloss.Color(colorDarkGray)).
			Render("No query selected"))
		return
	}

	query := m.queries[currentIndex]
	var content strings.Builder

	sectionHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorPurple))

	regularTextStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(colorLightGray))

	if query.DisplayName != "" {
		content.WriteString(sectionHeaderStyle.Render("Name: "))
		content.WriteString(regularTextStyle.Render(query.DisplayName))
		content.WriteString("\n")
	}

	// Internal Name Section
	if query.Name != "" {
		content.WriteString(sectionHeaderStyle.Render("ID: "))
		content.WriteString(regularTextStyle.Render(query.Name))
		content.WriteString("\n")
	}

	// Dataset Section
	if query.Dataset != "" {
		content.WriteString(sectionHeaderStyle.Render("Target Dataset: "))
		content.WriteString(regularTextStyle.Render(query.Dataset))
		content.WriteString("\n")
	}

	// Schedule Section
	if query.Schedule != "" {
		content.WriteString(sectionHeaderStyle.Render("Schedule: "))
		content.WriteString(regularTextStyle.Render(query.Schedule))
		content.WriteString("\n")
	}

	if strings.TrimSpace(query.Query) == "" {
		noQueryStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color(colorDarkGray)).
			Italic(true).
			Padding(1, 2).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(colorGray))

		content.WriteString(noQueryStyle.Render("-- No SQL query available --"))
	} else {
		highlightedSQL := highlightCode(query.Query, "sql")

		content.WriteString("\n")
		content.WriteString(sectionHeaderStyle.Render("Query"))
		content.WriteString("\n\n")

		// Wrap highlighted SQL in a styled code block
		content.WriteString(regularTextStyle.Render(highlightedSQL))
	}

	m.rightViewport.SetContent(content.String())
}

func (m *scheduledQueryModel) View() string {
	if m.quitting {
		return ""
	}

	if m.err != nil {
		return fmt.Sprintf("Error: %v\n", m.err)
	}

	if len(m.queries) == 0 {
		return titleStyle.Render("No scheduled queries found.") + "\n"
	}

	// If viewports aren't initialized yet, show a loading message
	if m.rightViewport.Width == 0 {
		return titleStyle.Render("Loading...") + "\n"
	}

	// Selection summary
	selectedCount := 0
	for _, selected := range m.selected {
		if selected {
			selectedCount++
		}
	}

	summaryText := fmt.Sprintf("Selected: %d/%d queries", selectedCount, len(m.queries))
	if selectedCount > 0 {
		summaryText += " ✓"
	}

	summary := lipgloss.NewStyle().
		Foreground(lipgloss.Color(colorGreen)).
		Bold(true).
		Render(summaryText)

	// Left panel title with focus indicator
	leftPanelTitle := "📋 Scheduled Queries"
	if m.focusedPane == 0 {
		leftPanelTitle += " •"
	}

	leftTitle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorBlue)).
		Padding(0, 1).
		Render(leftPanelTitle)

	// Panel styles with dynamic border colors based on focus
	leftBorderColor := colorGray
	rightBorderColor := colorGray

	if m.focusedPane == 0 {
		leftBorderColor = colorBlue
	} else if m.focusedPane == 1 {
		rightBorderColor = colorOrange
	}

	// Use the same height for both panels to ensure alignment
	panelHeight := maxInt(m.list.Height()+4, m.rightViewport.Height+4) // +4 for borders and padding

	currentLeftPanelStyle := leftPanelStyle.
		BorderForeground(lipgloss.Color(leftBorderColor)).
		Width(m.list.Width() + 2).
		Height(panelHeight)

	currentRightPanelStyle := rightPanelStyle.
		BorderForeground(lipgloss.Color(rightBorderColor)).
		Width(m.rightViewport.Width + 2).
		Height(panelHeight)

	// Left panel content
	leftContent := lipgloss.JoinVertical(
		lipgloss.Left,
		leftTitle,
		"",
		m.list.View(),
	)

	// Right panel content
	rightContent := lipgloss.JoinVertical(
		lipgloss.Left,
		m.rightViewport.View(),
	)

	leftPane := currentLeftPanelStyle.Render(leftContent)
	rightPane := currentRightPanelStyle.Render(rightContent)

	content := lipgloss.JoinHorizontal(
		lipgloss.Top,
		leftPane,
		rightPane,
	)

	// Enhanced status bar with more helpful shortcuts
	var statusParts []string

	// Navigation
	statusParts = append(statusParts, "↑/↓/j/k: Navigate")
	statusParts = append(statusParts, "Tab: Switch Panes")

	// Selection
	statusParts = append(statusParts, "Space: Select")
	statusParts = append(statusParts, "a: Select All")
	statusParts = append(statusParts, "n: Select None")

	// Scrolling hint
	if m.focusedPane == 1 {
		statusParts = append(statusParts, "↑/↓: Scroll Details")
	}

	// Actions
	statusParts = append(statusParts, "Enter: Import")
	statusParts = append(statusParts, "q/Esc: Quit")

	statusText := strings.Join(statusParts, " • ")
	statusBar := statusBarStyle.
		Width(m.windowWidth).
		Render(statusText)

	// Calculate remaining space and add it before status bar to push it to bottom
	usedHeight := 2 + 1 + 2 + panelHeight + 1 // summary + spacing + content + spacing for status bar
	remainingHeight := m.windowHeight - usedHeight
	if remainingHeight < 0 {
		remainingHeight = 0
	}

	fillerLines := make([]string, remainingHeight)
	for i := range remainingHeight {
		fillerLines[i] = ""
	}

	elements := []string{summary, "", content}
	elements = append(elements, fillerLines...)
	elements = append(elements, statusBar)

	return lipgloss.JoinVertical(lipgloss.Left, elements...)
}

func runScheduledQueriesImport(ctx context.Context, pipelinePath, connectionName, environment, configFile, projectID, location string) error {
	fs := afero.NewOsFs()

	// Get BigQuery connection
	conn, err := getConnectionFromConfigWithContext(ctx, environment, connectionName, fs, configFile)
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
			return errors.New("could not determine project ID from connection, please specify --project-id")
		}
	}

	if location == "" {
		location = bqClient.Location()
	}

	// Create Data Transfer Service client using the same credentials as BigQuery
	dtClient, err := bqClient.NewDataTransferClient(ctx)
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
	var allQueries []ScheduledQuery

	// If location is specified, only search in that location
	if location != "" {
		fmt.Printf("Looking for scheduled queries in location: %s\n", location)
		queries, err := listScheduledQueriesInLocation(ctx, client, projectID, location, true)
		if err != nil {
			return nil, err
		}
		allQueries = append(allQueries, queries...)
	} else {
		// If no location specified, search in all common BigQuery locations
		fmt.Println("🔍 Searching for scheduled queries across all BigQuery regions...")

		locations := getCommonBigQueryLocations()
		allQueries = searchLocationsInParallel(ctx, client, projectID, locations)
	}

	fmt.Printf("Total scheduled queries found across all locations: %d\n", len(allQueries))
	return allQueries, nil
}

// searchLocationsInParallel searches all locations in parallel with a beautiful loader.
func searchLocationsInParallel(ctx context.Context, client *datatransfer.Client, projectID string, locations []string) []ScheduledQuery {
	var allQueries []ScheduledQuery
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Results channel to collect findings
	type locationResult struct {
		location string
		queries  []ScheduledQuery
		err      error
	}

	results := make(chan locationResult, len(locations))

	// Start the animated loader
	done := make(chan bool)
	go showAnimatedLoader(done, len(locations))

	// Launch goroutines for each location
	for _, loc := range locations {
		wg.Add(1)
		go func(location string) {
			defer wg.Done()
			queries, err := listScheduledQueriesInLocation(ctx, client, projectID, location, false)
			results <- locationResult{
				location: location,
				queries:  queries,
				err:      err,
			}
		}(loc)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(results)
		done <- true
	}()

	// Collect results
	locationsWithQueries := 0
	totalQueries := 0

	for result := range results {
		if result.err != nil {
			// Silently skip locations with errors (common for disabled regions)
			continue
		}
		if len(result.queries) > 0 {
			locationsWithQueries++
			totalQueries += len(result.queries)

			mu.Lock()
			allQueries = append(allQueries, result.queries...)
			mu.Unlock()

			// Show real-time findings with beautiful styling
			successStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#059669")).Bold(true)
			locationStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#7C3AED"))

			queryWord := "query"
			if len(result.queries) > 1 {
				queryWord = "queries"
			}

			message := fmt.Sprintf("✨ Found %d %s in %s",
				len(result.queries),
				queryWord,
				locationStyle.Render(result.location))

			fmt.Printf("\r%s%s\n",
				successStyle.Render(message),
				strings.Repeat(" ", 20)) // Clear any remaining chars
		}
	}

	// Final summary
	if totalQueries > 0 {
		summaryStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#059669")).
			Bold(true).
			MarginTop(1)

		regionWord := "region"
		if locationsWithQueries > 1 {
			regionWord = "regions"
		}

		queryWord := "query"
		if totalQueries > 1 {
			queryWord = "queries"
		}

		message := fmt.Sprintf("🎉 Search complete! Found %d %s across %d %s",
			totalQueries, queryWord, locationsWithQueries, regionWord)

		fmt.Printf("\n%s\n", summaryStyle.Render(message))
	} else {
		emptyStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color(colorDarkGray)).
			MarginTop(1)

		fmt.Printf("\n%s\n", emptyStyle.Render("📭 No scheduled queries found in any region"))
	}

	return allQueries
}

// showAnimatedLoader displays a beautiful animated loader.
func showAnimatedLoader(done chan bool, totalLocations int) {
	frames := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	frameIndex := 0

	// Color styles
	spinnerStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#7C3AED")).Bold(true)
	textStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#374151"))

	ticker := time.NewTicker(80 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			// Clear the line completely
			fmt.Printf("\r%s\r", strings.Repeat(" ", 60))
			return
		case <-ticker.C:
			spinner := spinnerStyle.Render(frames[frameIndex])
			text := textStyle.Render(fmt.Sprintf("Scanning %d regions in parallel...", totalLocations))
			fmt.Printf("\r%s %s", spinner, text)
			frameIndex = (frameIndex + 1) % len(frames)
		}
	}
}

// getCommonBigQueryLocations returns a list of common BigQuery locations to search.
func getCommonBigQueryLocations() []string {
	return []string{
		"us",                      // Multi-regional US
		"eu",                      // Multi-regional EU
		"asia",                    // Multi-regional Asia
		"us-central1",             // Iowa
		"us-east1",                // South Carolina
		"us-east4",                // Northern Virginia
		"us-west1",                // Oregon
		"us-west2",                // Los Angeles
		"us-west3",                // Salt Lake City
		"us-west4",                // Las Vegas
		"europe-north1",           // Finland
		"europe-west1",            // Belgium
		"europe-west2",            // London
		"europe-west3",            // Frankfurt
		"europe-west4",            // Netherlands
		"europe-west6",            // Zurich
		"asia-east1",              // Taiwan
		"asia-east2",              // Hong Kong
		"asia-northeast1",         // Tokyo
		"asia-northeast2",         // Osaka
		"asia-northeast3",         // Seoul
		"asia-south1",             // Mumbai
		"asia-southeast1",         // Singapore
		"asia-southeast2",         // Jakarta
		"australia-southeast1",    // Sydney
		"northamerica-northeast1", // Montreal
		"southamerica-east1",      // São Paulo
	}
}

// listScheduledQueriesInLocation searches for scheduled queries in a specific location.
func listScheduledQueriesInLocation(ctx context.Context, client *datatransfer.Client, projectID, location string, debug bool) ([]ScheduledQuery, error) {
	parent := fmt.Sprintf("projects/%s/locations/%s", projectID, location)

	req := &datatransferpb.ListTransferConfigsRequest{
		Parent:        parent,
		DataSourceIds: []string{"scheduled_query"}, // Filter for scheduled queries only
	}

	var queries []ScheduledQuery
	it := client.ListTransferConfigs(ctx, req)

	queryCount := 0
	for {
		config, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}

		// Only process scheduled queries (double-check in case the filter didn't work)
		if config.GetDataSourceId() != "scheduled_query" {
			continue
		}

		queryCount++
		if debug {
			fmt.Printf("  Found scheduled query #%d: %s (Display Name: %s)\n", queryCount, config.GetName(), config.GetDisplayName())
		}

		// Extract query parameters
		queryText := ""
		if config.GetParams() != nil && config.GetParams().GetFields() != nil {
			if queryField, ok := config.GetParams().GetFields()["query"]; ok {
				queryText = queryField.GetStringValue()
			}
		}

		query := ScheduledQuery{
			Name:        config.GetName(),
			DisplayName: config.GetDisplayName(),
			Query:       queryText,
			Schedule:    config.GetSchedule(),
			Dataset:     config.GetDestinationDatasetId(),
			Config:      config,
		}

		queries = append(queries, query)
	}

	if debug && len(queries) > 0 {
		fmt.Printf("Total scheduled queries found in %s: %d\n", location, len(queries))
	}

	return queries, nil
}

func showScheduledQuerySelector(queries []ScheduledQuery) ([]ScheduledQuery, error) {
	// Convert queries to list items
	items := make([]list.Item, len(queries))
	for i, query := range queries {
		items[i] = scheduledQueryItem{
			query:    query,
			selected: false,
		}
	}

	// Create list with custom delegate
	delegate := customDelegate{
		selectedItems: make(map[int]bool),
	}
	delegate.ShowDescription = true
	delegate.SetHeight(3)

	queryList := list.New(items, delegate, 0, 0)
	queryList.Title = ""
	queryList.SetShowStatusBar(false)
	queryList.SetFilteringEnabled(true)
	queryList.SetShowHelp(false)
	queryList.SetShowTitle(false)

	model := &scheduledQueryModel{
		queries:     queries,
		list:        queryList,
		delegate:    delegate,
		selected:    make(map[int]bool),
		focusedPane: 0,
	}

	p := tea.NewProgram(model, tea.WithAltScreen())
	finalModel, err := p.Run()
	if err != nil {
		return nil, err
	}

	final := finalModel.(*scheduledQueryModel)
	if final.confirmed {
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

func showTableauDashboardSelector(dashboards []TableauDashboard) ([]TableauDashboard, error) {
	// Convert dashboards to list items
	items := make([]list.Item, len(dashboards))
	for i, dashboard := range dashboards {
		items[i] = tableauDashboardItem{
			dashboard: dashboard,
			selected:  false,
		}
	}

	// Create list with custom delegate
	delegate := customDelegate{
		selectedItems: make(map[int]bool),
	}
	delegate.ShowDescription = true
	delegate.SetHeight(3)

	dashboardList := list.New(items, delegate, 0, 0)
	dashboardList.Title = ""
	dashboardList.SetShowStatusBar(false)
	dashboardList.SetFilteringEnabled(true)
	dashboardList.SetShowHelp(false)
	dashboardList.SetShowTitle(false)

	model := &tableauDashboardModel{
		dashboards:  dashboards,
		list:        dashboardList,
		delegate:    delegate,
		selected:    make(map[int]bool),
		focusedPane: 0,
	}

	p := tea.NewProgram(model, tea.WithAltScreen())
	finalModel, err := p.Run()
	if err != nil {
		return nil, err
	}

	final := finalModel.(*tableauDashboardModel)
	if final.confirmed {
		var selected []TableauDashboard
		for i, isSelected := range final.selected {
			if isSelected && i < len(dashboards) {
				selected = append(selected, dashboards[i])
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
		asset := createAssetFromScheduledQuery(query, assetsPath)

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

func createAssetFromScheduledQuery(query ScheduledQuery, assetsPath string) *pipeline.Asset {
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
		Name: assetName,
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Name:    fileName,
			Path:    filePath,
			Content: query.Query, // Add the SQL query content
		},
		Description: "Imported from scheduled query: " + query.DisplayName,
	}

	// Set the materialization if we have dataset info
	if query.Dataset != "" {
		asset.Materialization = pipeline.Materialization{
			Type: pipeline.MaterializationTypeTable,
		}
	}

	return asset
}

func ImportTableauDashboards() *cli.Command {
	return &cli.Command{
		Name:  "tableau",
		Usage: "Import Tableau dashboards and views as Bruin assets",
		Description: `Import Tableau dashboards and views from Tableau Cloud/Server as individual Bruin assets.

This command connects to Tableau using the REST API, lists all workbooks and their views/dashboards,
and presents them in an interactive terminal UI where you can:
- Navigate with arrow keys or j/k
- Select/deselect items with space bar
- Toggle preview mode with 'p' to see dashboard details in a dual-pane view
- Press Enter to import selected dashboards
- Press 'q' to quit without importing

You can also use the --all flag to import all dashboards without the interactive UI.

Selected dashboards will be imported as .yml files in the current pipeline's assets/tableau folder.
Each imported asset will contain the necessary metadata to reference and refresh the dashboard.

Example:
  bruin import tableau ./my-pipeline --connection my-tableau-conn --env prod
  bruin import tableau ./my-pipeline --connection my-tableau-conn --all`,
		ArgsUsage: "[pipeline path]",
		Before:    telemetry.BeforeCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"c"},
				Usage:    "the name of the Tableau connection to use",
				Required: true,
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
			&cli.StringFlag{
				Name:    "workbook",
				Aliases: []string{"w"},
				Usage:   "Filter by specific workbook name (optional)",
			},
			&cli.StringFlag{
				Name:    "project",
				Aliases: []string{"p"},
				Usage:   "Filter by Tableau project name (optional)",
			},
			&cli.BoolFlag{
				Name:    "all",
				Aliases: []string{"a"},
				Usage:   "Import all dashboards without interactive selection",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			pipelinePath := c.Args().Get(0)
			if pipelinePath == "" {
				return cli.Exit("pipeline path is required", 1)
			}

			connectionName := c.String("connection")
			environment := c.String("environment")
			configFile := c.String("config-file")
			workbookFilter := c.String("workbook")
			projectFilter := c.String("project")
			importAll := c.Bool("all")

			return runTableauImport(ctx, pipelinePath, connectionName, environment, configFile, workbookFilter, projectFilter, importAll)
		},
	}
}

// TableauDashboard represents a dashboard/view with its workbook information.
type TableauDashboard struct {
	ViewID       string
	ViewName     string
	WorkbookID   string
	WorkbookName string
	WorkbookURL  string // Added for workbook URL in meta
	ProjectID    string // Added for project hierarchy
	ProjectName  string
	ProjectPath  []string // Added for full project hierarchy path
	OwnerName    string
	ContentURL   string
	ViewURL      string
	UpdatedAt    string
	Tags         []string
	DataSources  []tableau.DataSourceInfo     // Added for data source dependencies
	Connections  []tableau.WorkbookConnection // Added for connection tracking
}

// tableauDashboardItem implements list.Item interface for the TUI.
type tableauDashboardItem struct {
	dashboard TableauDashboard
	selected  bool
}

func (i tableauDashboardItem) Title() string {
	if i.dashboard.ViewName != "" {
		return i.dashboard.ViewName
	}
	return "Unnamed Dashboard"
}

func (i tableauDashboardItem) Description() string {
	var parts []string
	if i.dashboard.WorkbookName != "" {
		parts = append(parts, "Workbook: "+i.dashboard.WorkbookName)
	}
	if i.dashboard.ProjectName != "" {
		parts = append(parts, "Project: "+i.dashboard.ProjectName)
	}
	if len(parts) == 0 {
		return "No additional details"
	}
	return strings.Join(parts, " | ")
}

func (i tableauDashboardItem) FilterValue() string {
	return i.Title() + " " + i.dashboard.WorkbookName + " " + i.dashboard.ProjectName
}

// tableauDashboardModel is the Bubbletea model for dashboard selection.
type tableauDashboardModel struct {
	dashboards    []TableauDashboard
	list          list.Model
	delegate      customDelegate
	rightViewport viewport.Model
	selected      map[int]bool
	windowWidth   int
	windowHeight  int
	quitting      bool
	confirmed     bool
	err           error
	focusedPane   int
}

func (m *tableauDashboardModel) Init() tea.Cmd {
	return nil
}

func (m *tableauDashboardModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			m.quitting = true
			return m, tea.Quit
		case "tab":
			m.focusedPane = (m.focusedPane + 1) % 2
		case " ":
			if m.focusedPane == 0 {
				currentIndex := m.list.Index()
				if m.selected == nil {
					m.selected = make(map[int]bool)
				}
				m.selected[currentIndex] = !m.selected[currentIndex]
				m.delegate.selectedItems[currentIndex] = m.selected[currentIndex]
				m.list.SetDelegate(m.delegate)
				m.updateRightPanelContent()
			}
		case "enter":
			m.confirmed = true
			m.quitting = true
			return m, tea.Quit
		case "a":
			if m.selected == nil {
				m.selected = make(map[int]bool)
			}
			for i := range m.dashboards {
				m.selected[i] = true
				m.delegate.selectedItems[i] = true
			}
			m.list.SetDelegate(m.delegate)
			m.updateRightPanelContent()
		case "n":
			if m.selected == nil {
				m.selected = make(map[int]bool)
			}
			for i := range m.dashboards {
				m.selected[i] = false
				m.delegate.selectedItems[i] = false
			}
			m.list.SetDelegate(m.delegate)
			m.updateRightPanelContent()
		}

	case tea.WindowSizeMsg:
		m.windowWidth = msg.Width
		m.windowHeight = msg.Height

		leftWidth := m.windowWidth/2 - 4
		rightWidth := m.windowWidth - leftWidth - 8
		viewportHeight := m.windowHeight - 14

		if leftWidth < 30 {
			leftWidth = 30
		}
		if rightWidth < 40 {
			rightWidth = 40
		}
		if viewportHeight < 10 {
			viewportHeight = 10
		}

		m.list.SetSize(leftWidth, viewportHeight)

		if m.rightViewport.Width == 0 {
			m.rightViewport = viewport.New(rightWidth, viewportHeight)
			m.updateRightPanelContent()
		} else {
			m.rightViewport.Width = rightWidth
			m.rightViewport.Height = viewportHeight
			m.updateRightPanelContent()
		}
	}

	if m.focusedPane == 0 {
		m.list, cmd = m.list.Update(msg)
		cmds = append(cmds, cmd)
	}

	if m.focusedPane == 1 {
		m.rightViewport, cmd = m.rightViewport.Update(msg)
		cmds = append(cmds, cmd)
	}

	if msg, ok := msg.(tea.KeyMsg); ok {
		if msg.String() == "up" || msg.String() == "down" || msg.String() == "k" || msg.String() == "j" {
			m.updateRightPanelContent()
		}
	}

	return m, tea.Batch(cmds...)
}

func (m *tableauDashboardModel) updateRightPanelContent() {
	if m.rightViewport.Width == 0 {
		return
	}

	currentIndex := m.list.Index()
	if currentIndex >= len(m.dashboards) {
		m.rightViewport.SetContent(lipgloss.NewStyle().
			Foreground(lipgloss.Color(colorDarkGray)).
			Render("No dashboard selected"))
		return
	}

	dashboard := m.dashboards[currentIndex]
	var content strings.Builder

	sectionHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorPurple))

	regularTextStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(colorLightGray))

	content.WriteString(sectionHeaderStyle.Render("Dashboard Details"))
	content.WriteString("\n\n")

	if dashboard.ViewName != "" {
		content.WriteString(sectionHeaderStyle.Render("Name: "))
		content.WriteString(regularTextStyle.Render(dashboard.ViewName))
		content.WriteString("\n")
	}

	if dashboard.ViewID != "" {
		content.WriteString(sectionHeaderStyle.Render("View ID: "))
		content.WriteString(regularTextStyle.Render(dashboard.ViewID))
		content.WriteString("\n")
	}

	if dashboard.WorkbookName != "" {
		content.WriteString(sectionHeaderStyle.Render("Workbook: "))
		content.WriteString(regularTextStyle.Render(dashboard.WorkbookName))
		content.WriteString("\n")
	}

	if dashboard.ProjectName != "" {
		content.WriteString(sectionHeaderStyle.Render("Project: "))
		content.WriteString(regularTextStyle.Render(dashboard.ProjectName))
		content.WriteString("\n")
	}

	if dashboard.OwnerName != "" {
		content.WriteString(sectionHeaderStyle.Render("Owner: "))
		content.WriteString(regularTextStyle.Render(dashboard.OwnerName))
		content.WriteString("\n")
	}

	if dashboard.UpdatedAt != "" {
		content.WriteString(sectionHeaderStyle.Render("Last Updated: "))
		content.WriteString(regularTextStyle.Render(dashboard.UpdatedAt))
		content.WriteString("\n")
	}

	if len(dashboard.Tags) > 0 {
		content.WriteString(sectionHeaderStyle.Render("Tags: "))
		content.WriteString(regularTextStyle.Render(strings.Join(dashboard.Tags, ", ")))
		content.WriteString("\n")
	}

	if dashboard.ContentURL != "" {
		content.WriteString("\n")
		content.WriteString(sectionHeaderStyle.Render("Content URL: "))
		content.WriteString(regularTextStyle.Render(dashboard.ContentURL))
		content.WriteString("\n")
	}

	m.rightViewport.SetContent(content.String())
}

func (m *tableauDashboardModel) View() string {
	if m.quitting {
		return ""
	}

	if m.err != nil {
		return fmt.Sprintf("Error: %v\n", m.err)
	}

	if len(m.dashboards) == 0 {
		return titleStyle.Render("No Tableau dashboards found.") + "\n"
	}

	if m.rightViewport.Width == 0 {
		return titleStyle.Render("Loading...") + "\n"
	}

	selectedCount := 0
	for _, selected := range m.selected {
		if selected {
			selectedCount++
		}
	}

	summaryText := fmt.Sprintf("Selected: %d/%d dashboards", selectedCount, len(m.dashboards))
	if selectedCount > 0 {
		summaryText += " ✓"
	}

	summary := lipgloss.NewStyle().
		Foreground(lipgloss.Color(colorGreen)).
		Bold(true).
		Render(summaryText)

	leftPanelTitle := "📊 Tableau Dashboards"
	if m.focusedPane == 0 {
		leftPanelTitle += " •"
	}

	leftTitle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorBlue)).
		Padding(0, 1).
		Render(leftPanelTitle)

	leftBorderColor := colorGray
	rightBorderColor := colorGray

	if m.focusedPane == 0 {
		leftBorderColor = colorBlue
	} else if m.focusedPane == 1 {
		rightBorderColor = colorOrange
	}

	panelHeight := maxInt(m.list.Height()+4, m.rightViewport.Height+4)

	currentLeftPanelStyle := leftPanelStyle.
		BorderForeground(lipgloss.Color(leftBorderColor)).
		Width(m.list.Width() + 2).
		Height(panelHeight)

	currentRightPanelStyle := rightPanelStyle.
		BorderForeground(lipgloss.Color(rightBorderColor)).
		Width(m.rightViewport.Width + 2).
		Height(panelHeight)

	leftContent := lipgloss.JoinVertical(
		lipgloss.Left,
		leftTitle,
		"",
		m.list.View(),
	)

	rightContent := lipgloss.JoinVertical(
		lipgloss.Left,
		m.rightViewport.View(),
	)

	leftPane := currentLeftPanelStyle.Render(leftContent)
	rightPane := currentRightPanelStyle.Render(rightContent)

	content := lipgloss.JoinHorizontal(
		lipgloss.Top,
		leftPane,
		rightPane,
	)

	var statusParts []string
	statusParts = append(statusParts, "↑/↓/j/k: Navigate")
	statusParts = append(statusParts, "Tab: Switch Panes")
	statusParts = append(statusParts, "Space: Select")
	statusParts = append(statusParts, "a: Select All")
	statusParts = append(statusParts, "n: Select None")
	if m.focusedPane == 1 {
		statusParts = append(statusParts, "↑/↓: Scroll Details")
	}
	statusParts = append(statusParts, "Enter: Import")
	statusParts = append(statusParts, "q/Esc: Quit")

	statusText := strings.Join(statusParts, " • ")
	statusBar := statusBarStyle.
		Width(m.windowWidth).
		Render(statusText)

	usedHeight := 2 + 1 + 2 + panelHeight + 1
	remainingHeight := m.windowHeight - usedHeight
	if remainingHeight < 0 {
		remainingHeight = 0
	}

	fillerLines := make([]string, remainingHeight)
	for i := range remainingHeight {
		fillerLines[i] = ""
	}

	elements := []string{summary, "", content}
	elements = append(elements, fillerLines...)
	elements = append(elements, statusBar)

	return lipgloss.JoinVertical(lipgloss.Left, elements...)
}
