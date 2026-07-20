package ingestr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdaptySourceTables(t *testing.T) {
	t.Parallel()

	source, err := GetSourceTables("adapty")
	require.NoError(t, err)
	require.Equal(t, "adapty", source.Name)
	require.NotEmpty(t, source.Tables)

	var hasAnalytics, hasPaywalls bool
	for _, table := range source.Tables {
		switch table.Name {
		case "analytics?chart_id=<chart_id>":
			hasAnalytics = true
			require.Equal(t, "date", table.IncKey)
			require.Equal(t, "delete+insert", table.IncStrategy)
		case "paywalls":
			hasPaywalls = true
			require.Equal(t, "paywall_id", table.PrimaryKey)
			require.Equal(t, "updated_at", table.IncKey)
			require.Equal(t, "merge", table.IncStrategy)
		}
	}

	require.True(t, hasAnalytics)
	require.True(t, hasPaywalls)
}

func TestSharePointSourceTables(t *testing.T) {
	t.Parallel()

	source, err := GetSourceTables("sharepoint")
	require.NoError(t, err)
	require.Equal(t, "sharepoint", source.Name)
	require.NotEmpty(t, source.Tables)

	var hasExcelSheetExample, hasCSVExample bool
	for _, table := range source.Tables {
		switch table.Name {
		case "<path/to/file.xlsx>#sheet=<sheet_name>":
			hasExcelSheetExample = true
			require.Equal(t, "replace", table.IncStrategy)
		case "<path/to/file.csv>#csv,encoding=utf-16le,sep=tab":
			hasCSVExample = true
			require.Equal(t, "replace", table.IncStrategy)
		}
	}

	require.True(t, hasExcelSheetExample)
	require.True(t, hasCSVExample)
}
