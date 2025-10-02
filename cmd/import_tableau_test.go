package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestTableauDashboardItem(t *testing.T) {
	t.Parallel()
	dashboard := TableauDashboard{
		ViewID:       "test-view-id",
		ViewName:     "Test Dashboard",
		WorkbookName: "Test Workbook",
		ProjectName:  "Test Project",
	}
	item := tableauDashboardItem{dashboard: dashboard}

	assert.Equal(t, "Test Dashboard", item.Title())
	assert.Equal(t, "Workbook: Test Workbook | Project: Test Project", item.Description())
	assert.Contains(t, item.FilterValue(), "Test Dashboard Test Workbook Test Project")
}

func TestImportTableauDashboards(t *testing.T) {
	t.Parallel()
	cmd := ImportTableauDashboards()

	// Test command basic properties
	assert.Equal(t, "tableau", cmd.Name)
	assert.Contains(t, cmd.Usage, "Import Tableau dashboards")
	assert.Equal(t, "[pipeline path]", cmd.ArgsUsage)

	// Test flags
	flags := cmd.Flags
	require.NotNil(t, flags)

	// Find connection flag
	var hasConnectionFlag bool
	var hasEnvironmentFlag bool
	var hasAllFlag bool
	for _, flag := range flags {
		switch f := flag.(type) {
		case *cli.StringFlag:
			if f.Name == "connection" {
				hasConnectionFlag = true
				assert.Equal(t, "c", f.Aliases[0])
				assert.Contains(t, f.Usage, "connection")
			}
			if f.Name == "environment" {
				hasEnvironmentFlag = true
				assert.Equal(t, "env", f.Aliases[0])
			}
		case *cli.BoolFlag:
			if f.Name == "all" {
				hasAllFlag = true
				assert.Contains(t, f.Usage, "Import all dashboards")
			}
		}
	}

	assert.True(t, hasConnectionFlag, "Should have connection flag")
	assert.True(t, hasEnvironmentFlag, "Should have environment flag")
	assert.True(t, hasAllFlag, "Should have all flag")
}

func TestGenerateAssetNameFromDashboard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		dashboard TableauDashboard
		expected  string
	}{
		{
			name: "simple dashboard name",
			dashboard: TableauDashboard{
				ViewName: "Sales Dashboard",
			},
			expected: "sales_dashboard",
		},
		{
			name: "dashboard with workbook",
			dashboard: TableauDashboard{
				ViewName:     "Revenue",
				WorkbookName: "Financial Reports",
			},
			expected: "financial_reports_revenue",
		},
		{
			name: "dashboard with special characters",
			dashboard: TableauDashboard{
				ViewName:     "Sales & Marketing (2024)",
				WorkbookName: "Company-Reports",
			},
			expected: "company_reports_sales__marketing_2024",
		},
		{
			name: "empty dashboard name",
			dashboard: TableauDashboard{
				ViewName: "",
			},
			expected: "tableau_dashboard",
		},
		{
			name: "dashboard with only special characters",
			dashboard: TableauDashboard{
				ViewName: "@#$%",
			},
			expected: "tableau_dashboard",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := generateAssetNameFromDashboard(tt.dashboard)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeFolderName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple name",
			input:    "My Project",
			expected: "my_project",
		},
		{
			name:     "name with special characters",
			input:    "Sales & Marketing (2024)",
			expected: "sales_and_marketing_2024",
		},
		{
			name:     "name with multiple spaces",
			input:    "Project   Name",
			expected: "project_name",
		},
		{
			name:     "name with consecutive special chars",
			input:    "Project--Name",
			expected: "project_name",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "unnamed",
		},
		{
			name:     "only special characters",
			input:    "@#$%",
			expected: "unnamed",
		},
		{
			name:     "leading and trailing underscores",
			input:    "_project_",
			expected: "project",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := sanitizeFolderName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCreateDataSourceAssetName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple data source name",
			input:    "Sales Data",
			expected: "datasource_sales_data",
		},
		{
			name:     "data source with special characters",
			input:    "Sales & Marketing (2024)",
			expected: "datasource_sales__marketing_2024",
		},
		{
			name:     "empty data source name",
			input:    "",
			expected: "tableau_datasource",
		},
		{
			name:     "data source with only special characters",
			input:    "@#$%",
			expected: "tableau_datasource",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := createDataSourceAssetName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCreateWorkbookAssetName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple workbook name",
			input:    "Financial Reports",
			expected: "workbook_financial_reports",
		},
		{
			name:     "workbook with special characters",
			input:    "Q4-2024 Reports",
			expected: "workbook_q4_2024_reports",
		},
		{
			name:     "empty workbook name",
			input:    "",
			expected: "tableau_workbook",
		},
		{
			name:     "workbook with only special characters",
			input:    "@#$%",
			expected: "tableau_workbook",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := createWorkbookAssetName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
