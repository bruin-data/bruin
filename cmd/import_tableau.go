package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/tableau"
	errors2 "github.com/pkg/errors"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/afero"
)

func runTableauImport(ctx context.Context, pipelinePath, connectionName, environment, configFile, workbookFilter, projectFilter string, importAll bool) error {
	fs := afero.NewOsFs()

	fmt.Printf("🔧 Getting Tableau connection '%s'...\n", connectionName)

	// Get Tableau connection
	_, conn, err := getConnectionFromConfigWithContext(ctx, environment, connectionName, fs, configFile)
	if err != nil {
		return errors2.Wrapf(err, "failed to get Tableau connection '%s' from environment '%s'", connectionName, environment)
	}

	// Ensure it's a Tableau connection
	tableauClient, ok := conn.(*tableau.Client)
	if !ok {
		return fmt.Errorf("connection '%s' is not a Tableau connection. Please check your .bruin.yml configuration", connectionName)
	}

	// Fetch all dashboards from Tableau with enhanced information
	fmt.Println("🔍 Fetching Tableau dashboards, projects, and data sources...")
	dashboards, err := fetchTableauDashboardsEnhanced(ctx, tableauClient, workbookFilter, projectFilter)
	if err != nil {
		return errors2.Wrap(err, "failed to fetch Tableau dashboards")
	}

	if len(dashboards) == 0 {
		fmt.Println("No Tableau dashboards found.")
		return nil
	}

	fmt.Printf("Found %d dashboards/views.\n", len(dashboards))

	var selected []TableauDashboard

	if importAll {
		// Import all dashboards without UI
		selected = dashboards
		fmt.Printf("Importing all %d dashboards...\n", len(selected))
	} else {
		// Show interactive selection UI
		selected, err = showTableauDashboardSelector(dashboards)
		if err != nil {
			return errors2.Wrap(err, "failed to show dashboard selector")
		}

		if len(selected) == 0 {
			fmt.Println("No dashboards selected.")
			return nil
		}
	}

	// Import selected dashboards with enhanced folder structure
	return importSelectedTableauDashboardsEnhanced(ctx, pipelinePath, selected, fs, tableauClient)
}

// Enhanced fetch function that gets project hierarchy and data sources.
func fetchTableauDashboardsEnhanced(ctx context.Context, client *tableau.Client, workbookFilter, projectFilter string) ([]TableauDashboard, error) {
	dashboards := make([]TableauDashboard, 0)

	// Fetch all projects first to build hierarchy
	fmt.Println("  Fetching projects hierarchy...")
	projects, err := client.ListProjects(ctx)
	if err != nil {
		fmt.Printf("  Warning: Could not fetch projects: %v\n", err)
		projects = []tableau.ProjectInfo{} // Continue without project info
	} else {
		fmt.Printf("  Found %d projects\n", len(projects))
	}

	// Build project hierarchy map
	projectMap := make(map[string]*tableau.ProjectInfo)
	for i := range projects {
		projectMap[projects[i].ID] = &projects[i]
	}

	// Function to build project path
	getProjectPath := func(projectID string) []string {
		var path []string
		currentID := projectID
		visited := make(map[string]bool) // Prevent infinite loops

		for currentID != "" && !visited[currentID] {
			visited[currentID] = true
			if proj, ok := projectMap[currentID]; ok {
				path = append([]string{proj.Name}, path...)
				currentID = proj.ParentProjectID
			} else {
				break
			}
		}
		return path
	}

	fmt.Println("  Fetching workbooks...")
	// Get all workbooks with details
	workbooks, err := client.ListWorkbooks(ctx)
	if err != nil {
		return nil, errors2.Wrap(err, "failed to list workbooks")
	}
	fmt.Printf("  Found %d workbooks\n", len(workbooks))

	// Filter workbooks if specified
	if workbookFilter != "" {
		var filtered []tableau.WorkbookInfo
		for _, wb := range workbooks {
			if strings.Contains(strings.ToLower(wb.Name), strings.ToLower(workbookFilter)) {
				filtered = append(filtered, wb)
			}
		}
		workbooks = filtered
		fmt.Printf("  Filtered to %d workbooks matching '%s'\n", len(workbooks), workbookFilter)
	}

	// Get detailed workbook information including connections
	workbookDetailsMap := make(map[string]*tableau.ExtendedWorkbookInfo)
	var detailsMutex sync.Mutex
	fmt.Println("  Fetching workbook details and connections...")

	// Use pool with max 10 workers for parallel fetching
	p := pool.New().WithMaxGoroutines(10).WithContext(ctx)

	for _, wb := range workbooks {
		// wb is already captured in the loop
		p.Go(func(ctx context.Context) error {
			extWb := &tableau.ExtendedWorkbookInfo{
				WorkbookInfo: wb,
			}

			// Get workbook details if we can (for more complete info)
			if details, err := client.GetWorkbookDetails(ctx, wb.ID); err == nil {
				extWb.WorkbookInfo = *details
			}

			// Get workbook connections
			if connections, err := client.GetWorkbookConnections(ctx, wb.ID); err == nil {
				extWb.Connections = connections
				fmt.Printf("    Workbook '%s' has %d connections\n", wb.Name, len(connections))
			} else {
				fmt.Printf("    Warning: Could not fetch connections for workbook '%s': %v\n", wb.Name, err)
			}

			detailsMutex.Lock()
			workbookDetailsMap[wb.ID] = extWb
			detailsMutex.Unlock()

			return nil
		})
	}

	// Wait for all workbook fetches to complete
	if err := p.Wait(); err != nil {
		// Log error but continue - some workbooks might have succeeded
		fmt.Printf("  Warning: Some workbook fetches failed: %v\n", err)
	}

	fmt.Println("  Fetching views/dashboards...")
	// Get all views
	allViews, err := client.ListAllViews(ctx)
	if err != nil {
		// Fallback to fetching per workbook
		fmt.Printf("  Warning: Could not fetch all views at once: %v\n", err)
		fmt.Println("  Attempting to fetch views from individual workbooks...")

		allViews = []tableau.ViewInfo{}
		var viewsMutex sync.Mutex

		// Use pool for parallel view fetching
		viewPool := pool.New().WithMaxGoroutines(10).WithContext(ctx)

		for _, wb := range workbooks {
			// wb is already captured in the loop
			viewPool.Go(func(ctx context.Context) error {
				wbViews, wbErr := client.GetWorkbookViews(ctx, wb.ID)
				if wbErr != nil {
					fmt.Printf("    Warning: Could not fetch views for workbook '%s': %v\n", wb.Name, wbErr)
					return nil // Don't fail the whole operation
				}
				// Set workbook ID for each view
				for i := range wbViews {
					wbViews[i].WorkbookID = wb.ID
				}

				viewsMutex.Lock()
				allViews = append(allViews, wbViews...)
				viewsMutex.Unlock()

				return nil
			})
		}

		// Wait for all view fetches to complete
		if err := viewPool.Wait(); err != nil {
			fmt.Printf("    Warning: Some view fetches failed: %v\n", err)
		}

		if len(allViews) == 0 && len(workbooks) > 0 {
			return nil, errors2.Wrap(err, "failed to list views")
		}
	}
	fmt.Printf("  Found %d views/dashboards\n", len(allViews))

	// Get data sources
	fmt.Println("  Fetching data sources...")
	dataSources, err := client.ListDatasources(ctx)
	if err != nil {
		fmt.Printf("  Warning: Could not fetch data sources: %v\n", err)
		dataSources = []tableau.DataSourceInfo{} // Continue without data source info
	} else {
		fmt.Printf("  Found %d data sources\n", len(dataSources))
	}

	// Create data source map for quick lookup
	dataSourceMap := make(map[string]*tableau.DataSourceInfo)
	for i := range dataSources {
		dataSourceMap[dataSources[i].ID] = &dataSources[i]
	}

	// Process views and create enhanced dashboard structs
	for _, view := range allViews {
		dashboard := TableauDashboard{
			ViewID:     view.ID,
			ViewName:   view.Name,
			ContentURL: view.ContentURL,
			ViewURL:    view.ViewURL,
			UpdatedAt:  view.UpdatedAt,
		}

		// Get workbook information
		var workbookInfo *tableau.ExtendedWorkbookInfo
		if view.WorkbookInfo != nil && view.WorkbookInfo.ID != "" {
			dashboard.WorkbookID = view.WorkbookInfo.ID
			dashboard.WorkbookName = view.WorkbookInfo.Name
			workbookInfo = workbookDetailsMap[view.WorkbookInfo.ID]
		} else if view.WorkbookID != "" {
			dashboard.WorkbookID = view.WorkbookID
			if wb, ok := workbookDetailsMap[view.WorkbookID]; ok {
				dashboard.WorkbookName = wb.Name
				workbookInfo = wb
			}
		}

		// Skip if workbook filter doesn't match
		if workbookFilter != "" && !strings.Contains(strings.ToLower(dashboard.WorkbookName), strings.ToLower(workbookFilter)) {
			continue
		}

		// Build workbook URL
		if dashboard.WorkbookID != "" {
			host := client.GetHost()
			siteID := client.GetSiteID()
			if workbookInfo != nil && workbookInfo.ContentURL != "" {
				dashboard.WorkbookURL = fmt.Sprintf("https://%s/#/site/%s/workbooks/%s",
					host, siteID, workbookInfo.ContentURL)
			} else {
				dashboard.WorkbookURL = fmt.Sprintf("https://%s/#/site/%s/workbooks/%s",
					host, siteID, dashboard.WorkbookID)
			}
		}

		// Get project information and hierarchy
		if view.Project.ID != "" {
			dashboard.ProjectID = view.Project.ID
			dashboard.ProjectName = view.Project.Name
			dashboard.ProjectPath = getProjectPath(view.Project.ID)
		} else if workbookInfo != nil && workbookInfo.Project.ID != "" {
			dashboard.ProjectID = workbookInfo.Project.ID
			dashboard.ProjectName = workbookInfo.Project.Name
			dashboard.ProjectPath = getProjectPath(workbookInfo.Project.ID)
		}

		// Apply project filter if specified
		if projectFilter != "" && !strings.Contains(strings.ToLower(dashboard.ProjectName), strings.ToLower(projectFilter)) {
			continue
		}

		// Get owner information
		if view.Owner.Name != "" {
			dashboard.OwnerName = view.Owner.Name
		} else if workbookInfo != nil && workbookInfo.Owner.Name != "" {
			dashboard.OwnerName = workbookInfo.Owner.Name
		}

		// Get tags
		if view.Tags != nil && view.Tags.Tag != nil {
			for _, tag := range view.Tags.Tag {
				dashboard.Tags = append(dashboard.Tags, tag.Label)
			}
		}

		// Get connections and data sources from workbook
		if workbookInfo != nil {
			dashboard.Connections = workbookInfo.Connections

			// Extract data sources from connections
			seenDataSources := make(map[string]bool)
			for _, conn := range workbookInfo.Connections {
				if conn.Datasource != nil && conn.Datasource.ID != "" {
					if !seenDataSources[conn.Datasource.ID] {
						dashboard.DataSources = append(dashboard.DataSources, *conn.Datasource)
						seenDataSources[conn.Datasource.ID] = true
					}
				}
			}
		}

		dashboards = append(dashboards, dashboard)
	}

	// Sort dashboards by project path and then by name for better organization
	sort.Slice(dashboards, func(i, j int) bool {
		// First sort by project path
		pathI := strings.Join(dashboards[i].ProjectPath, "/")
		pathJ := strings.Join(dashboards[j].ProjectPath, "/")
		if pathI != pathJ {
			return pathI < pathJ
		}
		// Then by workbook name
		if dashboards[i].WorkbookName != dashboards[j].WorkbookName {
			return dashboards[i].WorkbookName < dashboards[j].WorkbookName
		}
		// Finally by view name
		return dashboards[i].ViewName < dashboards[j].ViewName
	})

	return dashboards, nil
}

// Enhanced import function that creates folder structure and data source assets.
func importSelectedTableauDashboardsEnhanced(ctx context.Context, pipelinePath string, dashboards []TableauDashboard, fs afero.Fs, client *tableau.Client) error {
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

	// Track unique data sources and workbooks to create
	uniqueDataSources := make(map[string]*tableau.DataSourceInfo)
	dataSourceAssetNames := make(map[string]string) // Map data source ID to asset name

	uniqueWorkbooks := make(map[string]*TableauDashboard) // Map workbook ID to first dashboard (for workbook info)
	workbookAssetNames := make(map[string]string)         // Map workbook ID to asset name

	// Collect all unique data sources and workbooks from dashboards
	for _, dashboard := range dashboards {
		// Collect data sources
		for _, ds := range dashboard.DataSources {
			if _, exists := uniqueDataSources[ds.ID]; !exists {
				uniqueDataSources[ds.ID] = &ds
			}
		}

		// Collect workbooks - create a copy to preserve the data
		if dashboard.WorkbookID != "" && dashboard.WorkbookName != "" {
			if _, exists := uniqueWorkbooks[dashboard.WorkbookID]; !exists {
				// Create a copy of the dashboard data to preserve workbook information
				dashboardCopy := dashboard
				uniqueWorkbooks[dashboard.WorkbookID] = &dashboardCopy
			}
		}
	}

	// Create data source assets first
	if len(uniqueDataSources) > 0 {
		fmt.Printf("\nCreating %d data source assets...\n", len(uniqueDataSources))

		dataSourcesPath := filepath.Join(pipelinePath, "assets", "tableau", "data_sources")
		if err := fs.MkdirAll(dataSourcesPath, 0o755); err != nil {
			return errors2.Wrapf(err, "failed to create data sources directory %s", dataSourcesPath)
		}

		for dsID, ds := range uniqueDataSources {
			assetName := createDataSourceAssetName(ds.Name)
			dataSourceAssetNames[dsID] = assetName

			// Check if asset already exists
			if existingAssets[assetName] != nil {
				fmt.Printf("  Data source asset '%s' already exists, skipping...\n", assetName)
				continue
			}

			// Create data source asset
			asset := createDataSourceAsset(ds, dataSourcesPath, client)

			// Save the asset
			err = asset.Persist(fs)
			if err != nil {
				return errors2.Wrapf(err, "failed to save data source asset '%s'", assetName)
			}

			fmt.Printf("  Created data source asset '%s' for '%s'\n", assetName, ds.Name)
		}
	}

	// Create workbook assets
	if len(uniqueWorkbooks) > 0 {
		fmt.Printf("\nCreating %d workbook assets...\n", len(uniqueWorkbooks))

		workbooksPath := filepath.Join(pipelinePath, "assets", "tableau", "workbooks")
		if err := fs.MkdirAll(workbooksPath, 0o755); err != nil {
			return errors2.Wrapf(err, "failed to create workbooks directory %s", workbooksPath)
		}

		for wbID, dashboardInfo := range uniqueWorkbooks {
			assetName := createWorkbookAssetName(dashboardInfo.WorkbookName)
			workbookAssetNames[wbID] = assetName

			// Check if asset already exists
			if existingAssets[assetName] != nil {
				fmt.Printf("  Workbook asset '%s' already exists, skipping...\n", assetName)
				continue
			}

			// Create workbook asset
			asset := createWorkbookAsset(dashboardInfo, workbooksPath, client)

			// Save the asset
			err = asset.Persist(fs)
			if err != nil {
				return errors2.Wrapf(err, "failed to save workbook asset '%s'", assetName)
			}

			fmt.Printf("  Created workbook asset '%s' for '%s'\n", assetName, dashboardInfo.WorkbookName)
		}
	}

	importedCount := 0
	skippedCount := 0

	// Group dashboards by project path for folder creation
	dashboardsByPath := make(map[string][]TableauDashboard)
	for _, dashboard := range dashboards {
		// Build sanitized path from project hierarchy
		var sanitizedPath string
		if len(dashboard.ProjectPath) > 0 {
			sanitizedParts := make([]string, len(dashboard.ProjectPath))
			for i, part := range dashboard.ProjectPath {
				sanitizedParts[i] = sanitizeFolderName(part)
			}
			sanitizedPath = filepath.Join(sanitizedParts...)
		} else {
			sanitizedPath = "root" // Default folder for dashboards without project
		}
		dashboardsByPath[sanitizedPath] = append(dashboardsByPath[sanitizedPath], dashboard)
	}

	// Create dashboards organized by project folders
	fmt.Printf("\nImporting %d dashboards organized by project folders...\n", len(dashboards))

	for projectPath, projectDashboards := range dashboardsByPath {
		// Create folder structure based on project path
		folderPath := filepath.Join(pipelinePath, "assets", "tableau")
		if projectPath != "root" {
			folderPath = filepath.Join(folderPath, projectPath)
		}

		if err := fs.MkdirAll(folderPath, 0o755); err != nil {
			return errors2.Wrapf(err, "failed to create directory %s", folderPath)
		}

		fmt.Printf("  Creating assets in: %s\n", folderPath)

		for _, dashboard := range projectDashboards {
			// Create enhanced asset from dashboard with dependencies
			asset := createEnhancedAssetFromTableauDashboard(dashboard, folderPath, client, dataSourceAssetNames, workbookAssetNames)

			// Generate asset name for checking duplicates
			assetName := generateAssetNameFromDashboard(dashboard)

			// Check if asset already exists
			if existingAssets[assetName] != nil {
				fmt.Printf("    Asset '%s' already exists, skipping...\n", assetName)
				skippedCount++
				continue
			}

			// Save the asset
			err = asset.Persist(fs)
			if err != nil {
				return errors2.Wrapf(err, "failed to save asset '%s'", assetName)
			}

			importedCount++
			fmt.Printf("    Imported dashboard '%s' as asset '%s'\n", dashboard.ViewName, assetName)
		}
	}

	fmt.Printf("\n✅ Successfully imported %d Tableau dashboards into pipeline '%s'\n", importedCount, pipelinePath)
	if skippedCount > 0 {
		fmt.Printf("   Skipped %d existing dashboards\n", skippedCount)
	}

	return nil
}

// Helper functions for asset creation

// sanitizeFolderName converts a string to a valid folder name.
func sanitizeFolderName(name string) string {
	// First check if the input only contains special characters and no alphanumeric characters
	hasAlphanumeric := false
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			hasAlphanumeric = true
			break
		}
	}

	// If no alphanumeric characters, return "unnamed"
	if !hasAlphanumeric {
		return "unnamed"
	}

	// Convert to lowercase and replace spaces and special characters
	sanitized := strings.ToLower(name)
	sanitized = strings.ReplaceAll(sanitized, " ", "_")
	sanitized = strings.ReplaceAll(sanitized, "-", "_")
	sanitized = strings.ReplaceAll(sanitized, ".", "_")
	sanitized = strings.ReplaceAll(sanitized, ",", "_")
	sanitized = strings.ReplaceAll(sanitized, "(", "_")
	sanitized = strings.ReplaceAll(sanitized, ")", "_")
	sanitized = strings.ReplaceAll(sanitized, "[", "_")
	sanitized = strings.ReplaceAll(sanitized, "]", "_")
	sanitized = strings.ReplaceAll(sanitized, "{", "_")
	sanitized = strings.ReplaceAll(sanitized, "}", "_")
	sanitized = strings.ReplaceAll(sanitized, "&", "_and_")
	sanitized = strings.ReplaceAll(sanitized, "+", "_plus_")
	sanitized = strings.ReplaceAll(sanitized, "@", "_at_")
	sanitized = strings.ReplaceAll(sanitized, "#", "_")
	sanitized = strings.ReplaceAll(sanitized, "$", "_")
	sanitized = strings.ReplaceAll(sanitized, "%", "_")
	sanitized = strings.ReplaceAll(sanitized, "^", "_")
	sanitized = strings.ReplaceAll(sanitized, "*", "_")
	sanitized = strings.ReplaceAll(sanitized, "!", "_")
	sanitized = strings.ReplaceAll(sanitized, "~", "_")
	sanitized = strings.ReplaceAll(sanitized, "`", "_")
	sanitized = strings.ReplaceAll(sanitized, "'", "_")
	sanitized = strings.ReplaceAll(sanitized, "\"", "_")
	sanitized = strings.ReplaceAll(sanitized, ";", "_")
	sanitized = strings.ReplaceAll(sanitized, ":", "_")
	sanitized = strings.ReplaceAll(sanitized, "?", "_")
	sanitized = strings.ReplaceAll(sanitized, "<", "_")
	sanitized = strings.ReplaceAll(sanitized, ">", "_")
	sanitized = strings.ReplaceAll(sanitized, "|", "_")
	sanitized = strings.ReplaceAll(sanitized, "/", "_")
	sanitized = strings.ReplaceAll(sanitized, "\\", "_")

	// Remove consecutive underscores
	for strings.Contains(sanitized, "__") {
		sanitized = strings.ReplaceAll(sanitized, "__", "_")
	}

	// Trim underscores from start and end
	sanitized = strings.Trim(sanitized, "_")

	// If empty, use default
	if sanitized == "" {
		sanitized = "unnamed"
	}

	return sanitized
}

func generateAssetNameFromDashboard(dashboard TableauDashboard) string {
	// Generate the same name that would be used for the dashboard asset
	assetName := dashboard.ViewName
	if assetName == "" {
		assetName = "tableau_dashboard"
	}

	// Sanitize the asset name for filename use
	assetName = strings.ToLower(assetName)
	assetName = strings.ReplaceAll(assetName, " ", "_")
	assetName = strings.ReplaceAll(assetName, "-", "_")
	assetName = strings.ReplaceAll(assetName, "/", "_")
	assetName = strings.ReplaceAll(assetName, "\\", "_")

	// Remove any characters that aren't alphanumeric or underscore
	var sanitized strings.Builder
	for _, r := range assetName {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			sanitized.WriteRune(r)
		}
	}
	assetName = sanitized.String()

	if assetName == "" {
		assetName = "tableau_dashboard"
	}

	// Create unique name if needed (add workbook prefix if available)
	if dashboard.WorkbookName != "" {
		workbookPrefix := strings.ToLower(dashboard.WorkbookName)
		workbookPrefix = strings.ReplaceAll(workbookPrefix, " ", "_")
		workbookPrefix = strings.ReplaceAll(workbookPrefix, "-", "_")

		var sanitizedPrefix strings.Builder
		for _, r := range workbookPrefix {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
				sanitizedPrefix.WriteRune(r)
			}
		}
		if sanitizedPrefix.Len() > 0 {
			assetName = sanitizedPrefix.String() + "_" + assetName
		}
	}

	return assetName
}

func createWorkbookAssetName(workbookName string) string {
	// Generate a safe filename from the workbook name
	if workbookName == "" {
		return "tableau_workbook"
	}

	// Sanitize the asset name for filename use
	assetName := strings.ToLower(workbookName)
	assetName = strings.ReplaceAll(assetName, " ", "_")
	assetName = strings.ReplaceAll(assetName, "-", "_")
	assetName = strings.ReplaceAll(assetName, "/", "_")
	assetName = strings.ReplaceAll(assetName, "\\", "_")

	// Remove any characters that aren't alphanumeric or underscore
	var sanitized strings.Builder
	for _, r := range assetName {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			sanitized.WriteRune(r)
		}
	}

	result := sanitized.String()
	if result == "" {
		return "tableau_workbook"
	}

	// Add workbook prefix for clarity
	return "workbook_" + result
}

func createDataSourceAssetName(datasourceName string) string {
	// Generate a safe filename from the data source name
	if datasourceName == "" {
		return "tableau_datasource"
	}

	// Sanitize the asset name for filename use
	assetName := strings.ToLower(datasourceName)
	assetName = strings.ReplaceAll(assetName, " ", "_")
	assetName = strings.ReplaceAll(assetName, "-", "_")
	assetName = strings.ReplaceAll(assetName, "/", "_")
	assetName = strings.ReplaceAll(assetName, "\\", "_")

	// Remove any characters that aren't alphanumeric or underscore
	var sanitized strings.Builder
	for _, r := range assetName {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			sanitized.WriteRune(r)
		}
	}

	result := sanitized.String()
	if result == "" {
		return "tableau_datasource"
	}

	// Add datasource prefix for clarity
	return "datasource_" + result
}

func createDataSourceAsset(dataSource *tableau.DataSourceInfo, assetsPath string, client *tableau.Client) *pipeline.Asset {
	assetName := createDataSourceAssetName(dataSource.Name)
	fileName := assetName + ".asset.yml"
	filePath := filepath.Join(assetsPath, fileName)

	// Build parameters map
	parameters := map[string]string{
		"datasource_id":   dataSource.ID,
		"datasource_name": dataSource.Name,
		"refresh":         "false", // Default to not auto-refreshing
	}

	// Add URL if we can construct it
	if client != nil {
		host := client.GetHost()
		siteID := client.GetSiteID()
		// Construct the full Tableau data source URL
		fullURL := fmt.Sprintf("https://%s/#/site/%s/datasources/%s", host, siteID, dataSource.ID)
		parameters["url"] = fullURL
	}

	// Build description
	description := "Tableau data source: " + dataSource.Name

	// Create the asset (without name - Bruin will extract from file path)
	asset := &pipeline.Asset{
		Type: pipeline.AssetTypeTableauDatasource, // Using proper Tableau datasource type
		ExecutableFile: pipeline.ExecutableFile{
			Name: fileName,
			Path: filePath,
		},
		Description: description,
		Parameters:  parameters,
	}

	return asset
}

func createWorkbookAsset(dashboardInfo *TableauDashboard, assetsPath string, client *tableau.Client) *pipeline.Asset {
	assetName := createWorkbookAssetName(dashboardInfo.WorkbookName)
	fileName := assetName + ".asset.yml"
	filePath := filepath.Join(assetsPath, fileName)

	// Build parameters map
	parameters := map[string]string{
		"workbook_id":   dashboardInfo.WorkbookID,
		"workbook_name": dashboardInfo.WorkbookName,
		"refresh":       "false", // Default to not auto-refreshing
	}

	// Add URL if available
	if dashboardInfo.WorkbookURL != "" {
		parameters["url"] = dashboardInfo.WorkbookURL
	} else if client != nil && dashboardInfo.WorkbookID != "" {
		// Construct URL if not available
		host := client.GetHost()
		siteID := client.GetSiteID()
		fullURL := fmt.Sprintf("https://%s/#/site/%s/workbooks/%s", host, siteID, dashboardInfo.WorkbookID)
		parameters["url"] = fullURL
	}

	// Build description
	description := "Tableau workbook: " + dashboardInfo.WorkbookName
	if len(dashboardInfo.ProjectPath) > 0 {
		description += fmt.Sprintf(" [Project: %s]", strings.Join(dashboardInfo.ProjectPath, " > "))
	}

	// Create the asset (without name - Bruin will extract from file path)
	asset := &pipeline.Asset{
		Type: pipeline.AssetTypeTableauWorkbook, // Using proper Tableau workbook type
		ExecutableFile: pipeline.ExecutableFile{
			Name: fileName,
			Path: filePath,
		},
		Description: description,
		Parameters:  parameters,
	}

	// Add metadata
	if len(dashboardInfo.ProjectPath) > 0 {
		metadata := make(map[string]string)
		metadata["project_hierarchy"] = strings.Join(dashboardInfo.ProjectPath, ",")
		if dashboardInfo.ProjectID != "" {
			metadata["project_id"] = dashboardInfo.ProjectID
		}
		asset.Meta = metadata
	}

	// Add owner if available
	if dashboardInfo.OwnerName != "" {
		asset.Owner = dashboardInfo.OwnerName
	}

	// Add tags if available
	if len(dashboardInfo.Tags) > 0 {
		asset.Tags = dashboardInfo.Tags
	}

	return asset
}

func createEnhancedAssetFromTableauDashboard(dashboard TableauDashboard, assetsPath string, client *tableau.Client, dataSourceAssetNames map[string]string, workbookAssetNames map[string]string) *pipeline.Asset {
	// Generate a safe filename from the dashboard name
	assetName := dashboard.ViewName
	if assetName == "" {
		assetName = "tableau_dashboard"
	}

	// Sanitize the asset name for filename use
	assetName = strings.ToLower(assetName)
	assetName = strings.ReplaceAll(assetName, " ", "_")
	assetName = strings.ReplaceAll(assetName, "-", "_")
	assetName = strings.ReplaceAll(assetName, "/", "_")
	assetName = strings.ReplaceAll(assetName, "\\", "_")

	// Remove any characters that aren't alphanumeric or underscore
	var sanitized strings.Builder
	for _, r := range assetName {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			sanitized.WriteRune(r)
		}
	}
	assetName = sanitized.String()

	if assetName == "" {
		assetName = "tableau_dashboard"
	}

	// Create unique name if needed (add workbook prefix if available)
	if dashboard.WorkbookName != "" {
		workbookPrefix := strings.ToLower(dashboard.WorkbookName)
		workbookPrefix = strings.ReplaceAll(workbookPrefix, " ", "_")
		workbookPrefix = strings.ReplaceAll(workbookPrefix, "-", "_")

		var sanitizedPrefix strings.Builder
		for _, r := range workbookPrefix {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
				sanitizedPrefix.WriteRune(r)
			}
		}
		if sanitizedPrefix.Len() > 0 {
			assetName = sanitizedPrefix.String() + "_" + assetName
		}
	}

	fileName := assetName + ".asset.yml"
	filePath := filepath.Join(assetsPath, fileName)

	// Build parameters map
	parameters := map[string]string{
		"dashboard_id":   dashboard.ViewID,
		"dashboard_name": dashboard.ViewName,
		"refresh":        "false",
	}

	if dashboard.WorkbookID != "" {
		parameters["workbook_id"] = dashboard.WorkbookID
	}
	if dashboard.WorkbookName != "" {
		parameters["workbook_name"] = dashboard.WorkbookName
	}

	// Build full URL to the dashboard
	if dashboard.ContentURL != "" && client != nil {
		host := client.GetHost()
		siteID := client.GetSiteID()

		// Remove "/sheets/" if present
		contentPath := dashboard.ContentURL
		contentPath = strings.Replace(contentPath, "/sheets/", "/", 1)

		fullURL := fmt.Sprintf("https://%s/#/site/%s/views/%s", host, siteID, contentPath)
		parameters["url"] = fullURL
	}

	// Build description
	description := "Tableau dashboard: " + dashboard.ViewName
	if dashboard.WorkbookName != "" {
		description += fmt.Sprintf(" (Workbook: %s)", dashboard.WorkbookName)
	}
	if len(dashboard.ProjectPath) > 0 {
		description += fmt.Sprintf(" [Project: %s]", strings.Join(dashboard.ProjectPath, " > "))
	}

	// Create the asset (without name - Bruin will extract from file path)
	asset := &pipeline.Asset{
		Type: pipeline.AssetTypeTableauDashboard,
		ExecutableFile: pipeline.ExecutableFile{
			Name: fileName,
			Path: filePath,
		},
		Description: description,
		Parameters:  parameters,
	}

	// Add metadata section with workbook information
	metadata := make(map[string]interface{})
	if dashboard.WorkbookName != "" || dashboard.WorkbookURL != "" {
		workbookMeta := make(map[string]string)
		if dashboard.WorkbookName != "" {
			workbookMeta["name"] = dashboard.WorkbookName
		}
		if dashboard.WorkbookURL != "" {
			workbookMeta["url"] = dashboard.WorkbookURL
		}
		if dashboard.WorkbookID != "" {
			workbookMeta["id"] = dashboard.WorkbookID
		}
		metadata["workbook"] = workbookMeta
	}

	// Add project hierarchy to metadata
	if len(dashboard.ProjectPath) > 0 {
		metadata["project_hierarchy"] = dashboard.ProjectPath
	}

	if len(metadata) > 0 {
		// Convert metadata to string map for Meta field
		metaStrings := make(map[string]string)
		for key, value := range metadata {
			if str, ok := value.(string); ok {
				metaStrings[key] = str
			} else if m, ok := value.(map[string]string); ok {
				// For nested maps, serialize as JSON
				for k, v := range m {
					metaStrings[key+"_"+k] = v
				}
			} else if arr, ok := value.([]string); ok {
				// For arrays, join with comma
				metaStrings[key] = strings.Join(arr, ",")
			}
		}
		asset.Meta = metaStrings
	}

	// Add owner if available
	if dashboard.OwnerName != "" {
		asset.Owner = dashboard.OwnerName
	}

	// Add tags if available
	if len(dashboard.Tags) > 0 {
		asset.Tags = dashboard.Tags
	}

	// Add dependencies on workbook and data sources
	var upstreams []pipeline.Upstream

	// Add workbook as dependency if available
	if dashboard.WorkbookID != "" {
		if workbookAssetName, exists := workbookAssetNames[dashboard.WorkbookID]; exists {
			// Use full path for workbook dependencies
			fullPath := "tableau.workbooks." + workbookAssetName
			upstreams = append(upstreams, pipeline.Upstream{
				Type:  "asset",
				Value: fullPath,
			})
		}
	}

	// Add data sources as dependencies with full path
	for _, ds := range dashboard.DataSources {
		if assetName, exists := dataSourceAssetNames[ds.ID]; exists {
			// Use full path for data source dependencies
			fullPath := "tableau.data_sources." + assetName
			upstreams = append(upstreams, pipeline.Upstream{
				Type:  "asset",
				Value: fullPath,
			})
		}
	}

	if len(upstreams) > 0 {
		asset.Upstreams = upstreams
	}

	return asset
}
