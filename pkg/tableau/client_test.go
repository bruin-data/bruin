package tableau

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewClient_WithUsernamePassword(t *testing.T) {
	t.Parallel()
	config := Config{
		Name:       "test-tableau",
		Host:       "tableau.example.com",
		Username:   "user",
		Password:   "pass",
		SiteID:     "site123",
		APIVersion: "3.4",
	}

	client, err := NewClient(config)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.Equal(t, config, client.config)
}

func TestNewClient_WithPAT(t *testing.T) {
	t.Parallel()
	config := Config{
		Name:                      "test-tableau",
		Host:                      "tableau.example.com",
		PersonalAccessTokenName:   "my-token",
		PersonalAccessTokenSecret: "my-secret",
		SiteID:                    "site123",
		APIVersion:                "3.4",
	}

	client, err := NewClient(config)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.Equal(t, config, client.config)
}

func TestNewClient_MissingRequiredFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name: "missing host",
			config: Config{
				Username: "user",
				Password: "pass",
				SiteID:   "site123",
			},
			wantErr: "host is required for Tableau connection",
		},
		{
			name: "missing site_id",
			config: Config{
				Host:     "tableau.example.com",
				Username: "user",
				Password: "pass",
			},
			wantErr: "site_id is required for Tableau connection",
		},
		{
			name: "missing both auth methods",
			config: Config{
				Host:   "tableau.example.com",
				SiteID: "site123",
			},
			wantErr: "either personal access token (name and secret) or username and password are required for Tableau connection",
		},
		{
			name: "incomplete PAT (missing secret)",
			config: Config{
				Host:                    "tableau.example.com",
				SiteID:                  "site123",
				PersonalAccessTokenName: "my-token",
			},
			wantErr: "either personal access token (name and secret) or username and password are required for Tableau connection",
		},
		{
			name: "incomplete username/password (missing password)",
			config: Config{
				Host:     "tableau.example.com",
				SiteID:   "site123",
				Username: "user",
			},
			wantErr: "either personal access token (name and secret) or username and password are required for Tableau connection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client, err := NewClient(tt.config)
			require.Error(t, err)
			require.Nil(t, client)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestNewClient_DefaultAPIVersion(t *testing.T) {
	t.Parallel()
	config := Config{
		Host:     "tableau.example.com",
		Username: "user",
		Password: "pass",
		SiteID:   "site123",
		// APIVersion not set
	}

	client, err := NewClient(config)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.Equal(t, "3.21", client.config.APIVersion)
}

func TestFindDatasourceIDByName(t *testing.T) {
	t.Parallel()
	datasources := []DataSourceInfo{
		{ID: "id1", Name: "Alpha"},
		{ID: "id2", Name: "Beta"},
	}

	id, err := FindDatasourceIDByName(context.Background(), "alpha", datasources)
	require.NoError(t, err)
	require.Equal(t, "id1", id)

	id, err = FindDatasourceIDByName(context.Background(), "BETA", datasources)
	require.NoError(t, err)
	require.Equal(t, "id2", id)

	id, err = FindDatasourceIDByName(context.Background(), "gamma", datasources)
	require.NoError(t, err)
	require.Empty(t, id)

	// nil datasources
	_, err = FindDatasourceIDByName(context.Background(), "alpha", nil)
	require.Error(t, err)

	// empty slice
	id, err = FindDatasourceIDByName(context.Background(), "alpha", []DataSourceInfo{})
	require.NoError(t, err)
	require.Empty(t, id)

	// empty name
	id, err = FindDatasourceIDByName(context.Background(), "", datasources)
	require.NoError(t, err)
	require.Empty(t, id)

	// name with spaces
	datasourcesWithSpaces := []DataSourceInfo{
		{ID: "id3", Name: "  Alpha  "},
	}
	id, err = FindDatasourceIDByName(context.Background(), "  alpha  ", datasourcesWithSpaces)
	require.NoError(t, err)
	require.Equal(t, "id3", id)

	// duplicate names, should return first match
	datasourcesDup := []DataSourceInfo{
		{ID: "id4", Name: "Gamma"},
		{ID: "id5", Name: "Gamma"},
	}
	id, err = FindDatasourceIDByName(context.Background(), "gamma", datasourcesDup)
	require.NoError(t, err)
	require.Equal(t, "id4", id)
}

func TestFindWorkbookIDByName(t *testing.T) {
	t.Parallel()
	workbooks := []WorkbookInfo{
		{ID: "w1", Name: "Superstore"},
		{ID: "w2", Name: "World Indicators"},
	}

	id, err := FindWorkbookIDByName(context.Background(), "superstore", workbooks)
	require.NoError(t, err)
	require.Equal(t, "w1", id)

	id, err = FindWorkbookIDByName(context.Background(), "WORLD INDICATORS", workbooks)
	require.NoError(t, err)
	require.Equal(t, "w2", id)

	id, err = FindWorkbookIDByName(context.Background(), "notfound", workbooks)
	require.NoError(t, err)
	require.Empty(t, id)

	// nil workbooks
	_, err = FindWorkbookIDByName(context.Background(), "superstore", nil)
	require.Error(t, err)

	// empty slice
	id, err = FindWorkbookIDByName(context.Background(), "superstore", []WorkbookInfo{})
	require.NoError(t, err)
	require.Empty(t, id)

	// empty name
	id, err = FindWorkbookIDByName(context.Background(), "", workbooks)
	require.NoError(t, err)
	require.Empty(t, id)

	// name with spaces
	workbooksWithSpaces := []WorkbookInfo{
		{ID: "w3", Name: "  Superstore  "},
	}
	id, err = FindWorkbookIDByName(context.Background(), "  superstore  ", workbooksWithSpaces)
	require.NoError(t, err)
	require.Equal(t, "w3", id)

	// duplicate names, should return first match
	workbooksDup := []WorkbookInfo{
		{ID: "w4", Name: "Gamma"},
		{ID: "w5", Name: "Gamma"},
	}
	id, err = FindWorkbookIDByName(context.Background(), "gamma", workbooksDup)
	require.NoError(t, err)
	require.Equal(t, "w4", id)
}
