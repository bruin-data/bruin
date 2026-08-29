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

func TestSklikSourceTables(t *testing.T) {
	t.Parallel()

	source, err := GetSourceTables("sklik")
	require.NoError(t, err)
	require.Equal(t, "sklik", source.Name)
	require.NotEmpty(t, source.Tables)

	var hasCampaigns, hasCampaignStats, hasSearchQueries bool
	for _, table := range source.Tables {
		switch table.Name {
		case "campaigns":
			hasCampaigns = true
			require.Equal(t, "id", table.PrimaryKey)
			require.Equal(t, "merge", table.IncStrategy)
		case "campaign_stats_daily":
			hasCampaignStats = true
			require.Equal(t, "id, date", table.PrimaryKey)
			require.Equal(t, "date", table.IncKey)
			require.Equal(t, "merge", table.IncStrategy)
		case "search_queries":
			hasSearchQueries = true
			require.Equal(t, "query, keyword_id, date", table.PrimaryKey)
			require.Equal(t, "date", table.IncKey)
			require.Equal(t, "merge", table.IncStrategy)
		}
	}

	require.True(t, hasCampaigns)
	require.True(t, hasCampaignStats)
	require.True(t, hasSearchQueries)
}

func TestCleverTapSourceTables(t *testing.T) {
	t.Parallel()

	source, err := GetSourceTables("clevertap")
	require.NoError(t, err)
	require.Equal(t, "clevertap", source.Name)
	require.NotEmpty(t, source.Tables)

	var hasEvents, hasProfiles, hasContentBlocks bool
	for _, table := range source.Tables {
		switch table.Name {
		case "events":
			hasEvents = true
			require.Equal(t, "ts", table.IncKey)
			require.Equal(t, "delete+insert", table.IncStrategy)
		case "profiles":
			hasProfiles = true
			require.Equal(t, "object_id", table.PrimaryKey)
			require.Equal(t, "replace", table.IncStrategy)
		case "content_blocks":
			hasContentBlocks = true
			require.Equal(t, "id", table.PrimaryKey)
			require.Equal(t, "updatedAt", table.IncKey)
			require.Equal(t, "merge", table.IncStrategy)
		}
	}

	require.True(t, hasEvents)
	require.True(t, hasProfiles)
	require.True(t, hasContentBlocks)
}

func TestOktaSourceTables(t *testing.T) {
	t.Parallel()

	source, err := GetSourceTables("okta")
	require.NoError(t, err)
	require.Equal(t, "okta", source.Name)
	require.NotEmpty(t, source.Tables)

	var hasUsers, hasGroupMembers, hasApplicationUsers, hasSystemLogEvents, hasRoles bool
	for _, table := range source.Tables {
		switch table.Name {
		case "users":
			hasUsers = true
			require.Equal(t, "id", table.PrimaryKey)
			require.Equal(t, "lastUpdated", table.IncKey)
			require.Equal(t, "merge", table.IncStrategy)
		case "group_members":
			hasGroupMembers = true
			require.Equal(t, "group_id,id", table.PrimaryKey)
			require.Equal(t, "replace", table.IncStrategy)
		case "application_users":
			hasApplicationUsers = true
			require.Equal(t, "app_id,id", table.PrimaryKey)
			require.Equal(t, "replace", table.IncStrategy)
		case "system_log_events":
			hasSystemLogEvents = true
			require.Equal(t, "uuid", table.PrimaryKey)
			require.Equal(t, "published", table.IncKey)
			require.Equal(t, "merge", table.IncStrategy)
		case "roles":
			hasRoles = true
			require.Equal(t, "id", table.PrimaryKey)
			require.Equal(t, "replace", table.IncStrategy)
		}
	}

	require.True(t, hasUsers)
	require.True(t, hasGroupMembers)
	require.True(t, hasApplicationUsers)
	require.True(t, hasSystemLogEvents)
	require.True(t, hasRoles)
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

func TestRipestatSourceTables(t *testing.T) {
	t.Parallel()

	source, err := GetSourceTables("ripestat")
	require.NoError(t, err)
	require.Equal(t, "ripestat", source.Name)
	require.NotEmpty(t, source.Tables)

	var hasASOverview, hasExampleResources bool
	for _, table := range source.Tables {
		switch table.Name {
		case "as-overview?resource=AS3333":
			hasASOverview = true
			require.Equal(t, "replace", table.IncStrategy)
		case "example-resources":
			hasExampleResources = true
			require.Equal(t, "replace", table.IncStrategy)
		}
	}

	require.True(t, hasASOverview)
	require.True(t, hasExampleResources)
}
