package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/quicksight"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
	errors2 "github.com/pkg/errors"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

// QuickSightAssetItem represents a dataset or dashboard for selection.
type QuickSightAssetItem struct {
	ID         string
	Name       string
	Kind       string // "dataset" or "dashboard"
	ImportMode string // SPICE or DIRECT_QUERY for datasets
}

func (i QuickSightAssetItem) Title() string {
	return i.Name
}

func (i QuickSightAssetItem) Description() string {
	if i.Kind == "dataset" {
		return fmt.Sprintf("Dataset [%s]", i.ImportMode)
	}
	return "Dashboard"
}

func (i QuickSightAssetItem) FilterValue() string {
	return i.Name
}

func runQuickSightImport(ctx context.Context, pipelinePath, connectionName, environment, configFile string, importAll bool) error {
	fs := afero.NewOsFs()

	fmt.Printf("Getting QuickSight connection '%s'...\n", connectionName)

	conn, err := getConnectionFromConfigWithContext(ctx, environment, connectionName, fs, configFile)
	if err != nil {
		return errors2.Wrapf(err, "failed to get QuickSight connection '%s' from environment '%s'", connectionName, environment)
	}

	client, ok := conn.(*quicksight.Client)
	if !ok {
		return fmt.Errorf("connection '%s' is not a QuickSight connection. Please check your .bruin.yml configuration", connectionName)
	}

	fmt.Println("Fetching QuickSight datasets, dashboards, and data sources...")

	datasets, dashboards, err := fetchQuickSightAssets(ctx, client)
	if err != nil {
		return errors2.Wrap(err, "failed to fetch QuickSight assets")
	}

	if len(datasets) == 0 && len(dashboards) == 0 {
		fmt.Println("No QuickSight assets found.")
		return nil
	}

	fmt.Printf("Found %d datasets and %d dashboards.\n", len(datasets), len(dashboards))

	var items []QuickSightAssetItem
	for _, ds := range datasets {
		items = append(items, QuickSightAssetItem{
			ID:         ds.ID,
			Name:       ds.Name,
			Kind:       "dataset",
			ImportMode: ds.ImportMode,
		})
	}
	for _, d := range dashboards {
		items = append(items, QuickSightAssetItem{
			ID:   d.ID,
			Name: d.Name,
			Kind: "dashboard",
		})
	}

	var selected []QuickSightAssetItem

	if importAll {
		selected = items
		fmt.Printf("Importing all %d assets...\n", len(selected))
	} else {
		selected, err = showQuickSightSelector(items)
		if err != nil {
			return errors2.Wrap(err, "failed to show asset selector")
		}

		if len(selected) == 0 {
			fmt.Println("No assets selected.")
			return nil
		}
	}

	return importSelectedQuickSightAssets(ctx, pipelinePath, selected, fs, client, datasets)
}

func fetchQuickSightAssets(ctx context.Context, client *quicksight.Client) ([]quicksight.DataSetSummary, []quicksight.DashboardSummary, error) {
	var (
		datasets   []quicksight.DataSetSummary
		dashboards []quicksight.DashboardSummary
		mu         sync.Mutex
	)

	p := pool.New().WithMaxGoroutines(2).WithContext(ctx)

	p.Go(func(ctx context.Context) error {
		ds, err := client.ListDataSets(ctx)
		if err != nil {
			return err
		}
		mu.Lock()
		datasets = ds
		mu.Unlock()
		return nil
	})

	p.Go(func(ctx context.Context) error {
		d, err := client.ListDashboards(ctx)
		if err != nil {
			return err
		}
		mu.Lock()
		dashboards = d
		mu.Unlock()
		return nil
	})

	if err := p.Wait(); err != nil {
		return nil, nil, err
	}

	return datasets, dashboards, nil
}

func showQuickSightSelector(items []QuickSightAssetItem) ([]QuickSightAssetItem, error) {
	listItems := make([]list.Item, len(items))
	for i, item := range items {
		listItems[i] = quickSightListItem{item: item}
	}

	delegate := customDelegate{
		selectedItems: make(map[int]bool),
	}
	delegate.ShowDescription = true
	delegate.SetHeight(3)

	l := list.New(listItems, delegate, 0, 0)
	l.Title = ""
	l.SetShowStatusBar(false)
	l.SetFilteringEnabled(true)
	l.SetShowHelp(false)
	l.SetShowTitle(false)

	model := &quickSightSelectorModel{
		items:    items,
		list:     l,
		delegate: delegate,
		selected: make(map[int]bool),
	}

	prog := tea.NewProgram(model, tea.WithAltScreen())
	finalModel, err := prog.Run()
	if err != nil {
		return nil, err
	}

	final := finalModel.(*quickSightSelectorModel)
	if final.confirmed {
		var selected []QuickSightAssetItem
		for i, isSelected := range final.selected {
			if isSelected && i < len(items) {
				selected = append(selected, items[i])
			}
		}
		return selected, nil
	}

	return nil, nil
}

type quickSightListItem struct {
	item QuickSightAssetItem
}

func (i quickSightListItem) Title() string       { return i.item.Title() }
func (i quickSightListItem) Description() string { return i.item.Description() }
func (i quickSightListItem) FilterValue() string { return i.item.FilterValue() }

type quickSightSelectorModel struct {
	items     []QuickSightAssetItem
	list      list.Model
	delegate  customDelegate
	selected  map[int]bool
	quitting  bool
	confirmed bool
}

func (m *quickSightSelectorModel) Init() tea.Cmd {
	return nil
}

func (m *quickSightSelectorModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case keyCtrlC, "q", "esc":
			m.quitting = true
			return m, tea.Quit
		case " ":
			i := m.list.Index()
			m.selected[i] = !m.selected[i]
			m.delegate.selectedItems[i] = m.selected[i]
			m.list.SetDelegate(m.delegate)
			return m, nil
		case "a":
			for i := range m.items {
				m.selected[i] = true
				m.delegate.selectedItems[i] = true
			}
			m.list.SetDelegate(m.delegate)
			return m, nil
		case "n":
			for i := range m.items {
				m.selected[i] = false
				m.delegate.selectedItems[i] = false
			}
			m.list.SetDelegate(m.delegate)
			return m, nil
		case "enter":
			m.confirmed = true
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		m.list.SetSize(msg.Width, msg.Height-2)
	}

	var cmd tea.Cmd
	m.list, cmd = m.list.Update(msg)
	return m, cmd
}

func (m *quickSightSelectorModel) View() string {
	if m.quitting {
		return ""
	}

	selectedCount := 0
	for _, v := range m.selected {
		if v {
			selectedCount++
		}
	}

	header := fmt.Sprintf("QuickSight Assets (%d selected) - [space] toggle, [a]ll, [n]one, [enter] confirm, [q]uit\n\n", selectedCount)
	return header + m.list.View()
}

func importSelectedQuickSightAssets(
	ctx context.Context,
	pipelinePath string,
	selected []QuickSightAssetItem,
	fs afero.Fs,
	client *quicksight.Client,
	allDatasets []quicksight.DataSetSummary,
) error {
	pathParts := strings.Split(pipelinePath, "/")
	if pathParts[len(pathParts)-1] == "pipeline.yml" || pathParts[len(pathParts)-1] == "pipeline.yaml" {
		pipelinePath = strings.Join(pathParts[:len(pathParts)-1], "/")
	}

	pipelineFound, err := GetPipelinefromPath(ctx, pipelinePath)
	if err != nil {
		return errors2.Wrap(err, "failed to get pipeline from path")
	}

	existingAssets := make(map[string]*pipeline.Asset, len(pipelineFound.Assets))
	for _, asset := range pipelineFound.Assets {
		existingAssets[asset.Name] = asset
	}

	// Build dataset ARN to summary map
	datasetArnMap := make(map[string]quicksight.DataSetSummary)
	for _, ds := range allDatasets {
		datasetArnMap[ds.Arn] = ds
	}

	// Fetch details for selected datasets in parallel
	datasetDetails := make(map[string]*quicksight.DataSetDetail)
	dashboardDetails := make(map[string]*quicksight.DashboardDetail)
	var mu sync.Mutex

	p := pool.New().WithMaxGoroutines(10).WithContext(ctx)

	for _, item := range selected {
		switch item.Kind {
		case "dataset":
			dsID := item.ID
			p.Go(func(ctx context.Context) error {
				detail, err := client.DescribeDataSet(ctx, dsID)
				if err != nil {
					fmt.Printf("  Warning: Could not describe dataset '%s': %v\n", dsID, err)
					return nil
				}
				mu.Lock()
				datasetDetails[dsID] = detail
				mu.Unlock()
				return nil
			})
		case "dashboard":
			dID := item.ID
			p.Go(func(ctx context.Context) error {
				detail, err := client.DescribeDashboardDefinition(ctx, dID)
				if err != nil {
					fmt.Printf("  Warning: Could not describe dashboard definition '%s': %v\n", dID, err)
					// Fall back to DescribeDashboard
					basic, err2 := client.DescribeDashboard(ctx, dID)
					if err2 != nil {
						fmt.Printf("  Warning: Could not describe dashboard '%s': %v\n", dID, err2)
						return nil
					}
					mu.Lock()
					dashboardDetails[dID] = basic
					mu.Unlock()
					return nil
				}
				mu.Lock()
				dashboardDetails[dID] = detail
				mu.Unlock()
				return nil
			})
		}
	}

	if err := p.Wait(); err != nil {
		return errors2.Wrap(err, "failed to fetch asset details")
	}

	// Create dataset assets
	datasetsPath := filepath.Join(pipelinePath, "assets", "quicksight", "datasets")
	dashboardsPath := filepath.Join(pipelinePath, "assets", "quicksight", "dashboards")

	if err := fs.MkdirAll(datasetsPath, 0o755); err != nil {
		return errors2.Wrapf(err, "failed to create datasets directory %s", datasetsPath)
	}
	if err := fs.MkdirAll(dashboardsPath, 0o755); err != nil {
		return errors2.Wrapf(err, "failed to create dashboards directory %s", dashboardsPath)
	}

	importedCount := 0
	skippedCount := 0

	// Track dataset asset names for dashboard dependency resolution
	datasetAssetNames := make(map[string]string) // dataset ARN -> asset name

	for _, item := range selected {
		if item.Kind != "dataset" {
			continue
		}

		detail := datasetDetails[item.ID]
		if detail == nil {
			continue
		}

		assetName := sanitizeQuickSightName("dataset_" + detail.Name)
		fullAssetName := "quicksight.datasets." + assetName
		datasetAssetNames[detail.Arn] = fullAssetName

		if existingAssets[fullAssetName] != nil {
			fmt.Printf("  Dataset asset '%s' already exists, skipping...\n", fullAssetName)
			skippedCount++
			continue
		}

		asset := createQuickSightDatasetAsset(detail, datasetsPath)
		if err := asset.Persist(fs); err != nil {
			return errors2.Wrapf(err, "failed to save dataset asset '%s'", assetName)
		}

		importedCount++
		fmt.Printf("  Imported dataset '%s'\n", detail.Name)
	}

	// Create dashboard assets
	for _, item := range selected {
		if item.Kind != "dashboard" {
			continue
		}

		detail := dashboardDetails[item.ID]
		if detail == nil {
			continue
		}

		assetName := sanitizeQuickSightName("dashboard_" + detail.Name)
		fullAssetName := "quicksight.dashboards." + assetName

		if existingAssets[fullAssetName] != nil {
			fmt.Printf("  Dashboard asset '%s' already exists, skipping...\n", fullAssetName)
			skippedCount++
			continue
		}

		asset := createQuickSightDashboardAsset(detail, dashboardsPath, datasetArnMap, datasetAssetNames)
		if err := asset.Persist(fs); err != nil {
			return errors2.Wrapf(err, "failed to save dashboard asset '%s'", assetName)
		}

		importedCount++
		fmt.Printf("  Imported dashboard '%s'\n", detail.Name)
	}

	fmt.Printf("\nSuccessfully imported %d QuickSight assets into pipeline '%s'\n", importedCount, pipelinePath)
	if skippedCount > 0 {
		fmt.Printf("  Skipped %d existing assets\n", skippedCount)
	}

	return nil
}

func createQuickSightDatasetAsset(
	detail *quicksight.DataSetDetail,
	assetsPath string,
) *pipeline.Asset {
	assetName := sanitizeQuickSightName("dataset_" + detail.Name)
	fileName := assetName + ".asset.yml"
	filePath := filepath.Join(assetsPath, fileName)

	parameters := map[string]string{
		"dataset_id":   detail.ID,
		"dataset_name": detail.Name,
		"import_mode":  detail.ImportMode,
		"refresh":      "false",
	}

	// Build columns
	columns := make([]pipeline.Column, 0, len(detail.Columns))
	for _, col := range detail.Columns {
		columns = append(columns, pipeline.Column{
			Name: col.Name,
			Type: mapQuickSightColumnType(col.Type),
		})
	}

	// Build upstreams from physical table maps
	var upstreams []pipeline.Upstream
	upstreamTables := make(map[string]bool)

	for _, pt := range detail.PhysicalTableMaps {
		if pt.TableName != "" {
			tableName := buildTableReference(pt.SchemaName, pt.TableName)
			if !upstreamTables[tableName] {
				upstreamTables[tableName] = true
				upstreams = append(upstreams, pipeline.Upstream{
					Type:  "asset",
					Value: tableName,
				})
			}
		}
	}

	// Add column-level upstreams
	for i, col := range columns {
		for _, pt := range detail.PhysicalTableMaps {
			if pt.TableName == "" {
				continue
			}
			tableName := buildTableReference(pt.SchemaName, pt.TableName)
			for _, ptCol := range pt.Columns {
				if strings.EqualFold(ptCol.Name, col.Name) {
					columns[i].Upstreams = append(columns[i].Upstreams, &pipeline.UpstreamColumn{
						Column: ptCol.Name,
						Table:  tableName,
					})
				}
			}
		}
	}

	asset := &pipeline.Asset{
		Type: pipeline.AssetTypeQuicksightDataset,
		ExecutableFile: pipeline.ExecutableFile{
			Name: fileName,
			Path: filePath,
		},
		Description: "QuickSight dataset: " + detail.Name,
		Parameters:  parameters,
		Columns:     columns,
		Upstreams:   upstreams,
	}

	return asset
}

func createQuickSightDashboardAsset(
	detail *quicksight.DashboardDetail,
	assetsPath string,
	datasetArnMap map[string]quicksight.DataSetSummary,
	datasetAssetNames map[string]string,
) *pipeline.Asset {
	assetName := sanitizeQuickSightName("dashboard_" + detail.Name)
	fileName := assetName + ".asset.yml"
	filePath := filepath.Join(assetsPath, fileName)

	parameters := map[string]string{
		"dashboard_id":   detail.ID,
		"dashboard_name": detail.Name,
	}

	// Build charts from sheets/visuals
	chartIndex := 0
	var columns []pipeline.Column
	columnSeen := make(map[string]bool)

	for _, sheet := range detail.Sheets {
		for _, visual := range sheet.Visuals {
			chartPrefix := fmt.Sprintf("charts[%d].", chartIndex)
			chartName := visual.Name
			if chartName == "" {
				chartName = fmt.Sprintf("%s_%d", visual.Type, chartIndex)
			}
			parameters[chartPrefix+"name"] = chartName
			parameters[chartPrefix+"type"] = visual.Type

			if visual.DataSetID != "" {
				// Try to resolve dataset name
				if dsAssetName, exists := datasetAssetNames[visual.DataSetID]; exists {
					parameters[chartPrefix+"dataset"] = dsAssetName
				}
			}

			if len(visual.Dimensions) > 0 {
				parameters[chartPrefix+"dimensions"] = strings.Join(visual.Dimensions, ",")
			}
			if len(visual.Metrics) > 0 {
				parameters[chartPrefix+"metrics"] = strings.Join(visual.Metrics, ",")
			}

			// Track columns for lineage
			for _, dim := range visual.Dimensions {
				if dim != "" && !columnSeen[dim] {
					columnSeen[dim] = true
					columns = append(columns, pipeline.Column{
						Name: dim,
						Type: "STRING",
					})
				}
			}
			for _, metric := range visual.Metrics {
				if metric != "" && !columnSeen[metric] {
					columnSeen[metric] = true
					columns = append(columns, pipeline.Column{
						Name: metric,
						Type: "FLOAT",
					})
				}
			}

			chartIndex++
		}
	}

	if chartIndex > 0 {
		parameters["chart_count"] = strconv.Itoa(chartIndex)
	}

	// Build upstreams from dataset ARNs
	var upstreams []pipeline.Upstream
	upstreamSeen := make(map[string]bool)

	for _, dsArn := range detail.DataSetArns {
		if assetName, exists := datasetAssetNames[dsArn]; exists {
			if !upstreamSeen[assetName] {
				upstreamSeen[assetName] = true
				upstreams = append(upstreams, pipeline.Upstream{
					Type:  "asset",
					Value: assetName,
				})
			}
		} else if dsSummary, exists := datasetArnMap[dsArn]; exists {
			// Dataset wasn't selected for import — reference by name
			name := "quicksight.datasets.dataset_" + sanitizeQuickSightName(dsSummary.Name)
			if !upstreamSeen[name] {
				upstreamSeen[name] = true
				upstreams = append(upstreams, pipeline.Upstream{
					Type:  "asset",
					Value: name,
				})
			}
		}
	}

	// Add column-level upstreams linking to dataset columns
	for i, col := range columns {
		for _, dsArn := range detail.DataSetArns {
			dsAssetName := ""
			if name, exists := datasetAssetNames[dsArn]; exists {
				dsAssetName = name
			} else if dsSummary, exists := datasetArnMap[dsArn]; exists {
				dsAssetName = "quicksight.datasets.dataset_" + sanitizeQuickSightName(dsSummary.Name)
			}
			if dsAssetName != "" {
				columns[i].Upstreams = append(columns[i].Upstreams, &pipeline.UpstreamColumn{
					Column: col.Name,
					Table:  dsAssetName,
				})
			}
		}
	}

	asset := &pipeline.Asset{
		Type: pipeline.AssetTypeQuicksightDashboard,
		ExecutableFile: pipeline.ExecutableFile{
			Name: fileName,
			Path: filePath,
		},
		Description: "QuickSight dashboard: " + detail.Name,
		Parameters:  parameters,
		Columns:     columns,
		Upstreams:   upstreams,
	}

	return asset
}

func sanitizeQuickSightName(name string) string {
	sanitized := strings.ToLower(name)
	sanitized = strings.ReplaceAll(sanitized, " ", "_")
	sanitized = strings.ReplaceAll(sanitized, "-", "_")
	sanitized = strings.ReplaceAll(sanitized, ".", "_")
	sanitized = strings.ReplaceAll(sanitized, "/", "_")
	sanitized = strings.ReplaceAll(sanitized, "\\", "_")

	var b strings.Builder
	for _, r := range sanitized {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	result := b.String()

	for strings.Contains(result, "__") {
		result = strings.ReplaceAll(result, "__", "_")
	}
	result = strings.Trim(result, "_")

	if result == "" {
		return "unnamed"
	}
	return result
}

func buildTableReference(schema, table string) string {
	if schema != "" {
		return schema + "." + table
	}
	return table
}

func mapQuickSightColumnType(qsType string) string {
	switch strings.ToUpper(qsType) {
	case "STRING":
		return "STRING"
	case "INTEGER":
		return "INTEGER"
	case "DECIMAL":
		return "FLOAT"
	case "DATETIME":
		return "TIMESTAMP"
	default:
		return qsType
	}
}

func ImportQuickSightAssets() *cli.Command {
	return &cli.Command{
		Name:  "quicksight",
		Usage: "Import QuickSight datasets and dashboards as Bruin assets",
		Description: `Import AWS QuickSight datasets and dashboards as individual Bruin assets.

This command connects to QuickSight using the AWS API, lists all datasets and dashboards,
and presents them in an interactive terminal UI where you can:
- Navigate with arrow keys or j/k
- Select/deselect items with space bar
- Press Enter to import selected assets
- Press 'q' to quit without importing

You can also use the --all flag to import all assets without the interactive UI.

Selected assets will be imported as .yml files in the current pipeline's assets/quicksight folder.
Each imported asset will contain the necessary metadata including column definitions,
upstream warehouse table dependencies, and chart-level details for dashboards.

Example:
  bruin import quicksight ./my-pipeline --connection my-qs-conn --env prod
  bruin import quicksight ./my-pipeline --connection my-qs-conn --all`,
		ArgsUsage: "[pipeline path]",
		Before:    telemetry.BeforeCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"c"},
				Usage:    "the name of the QuickSight connection to use",
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
			&cli.BoolFlag{
				Name:    "all",
				Aliases: []string{"a"},
				Usage:   "Import all assets without interactive selection",
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
			importAll := c.Bool("all")

			return runQuickSightImport(ctx, pipelinePath, connectionName, environment, configFile, importAll)
		},
	}
}
