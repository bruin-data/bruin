package config

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSaveCloudConnectionPreservesConfig(t *testing.T) {
	t.Parallel()
	files := afero.NewMemMapFs()
	require.NoError(t, files.MkdirAll("/repo", 0o700))
	original := []byte("# retain this\ndefault_environment: prod\nenvironments:\n  prod:\n    connections:\n      generic:\n        - name: unrelated\n          value: ${SECRET}\n      bruin:\n        - name: existing\n          api_token: old-token\ncloud:\n  default_team: old-team\n")
	require.NoError(t, afero.WriteFile(files, "/repo/.bruin.yml", original, 0o644))
	ref := CloudConnectionRef{Environment: "prod", Connection: BruinCloudConnection{ConnectionMetadata: ConnectionMetadata{Name: "existing"}, APIToken: "new-token", APIURL: "https://cloud.getbruin.com/api/v1"}}
	require.NoError(t, SaveCloudConnection(files, "/repo/.bruin.yml", original, ref, true, "acme"))
	data, err := afero.ReadFile(files, "/repo/.bruin.yml")
	require.NoError(t, err)
	assert.Contains(t, string(data), "# retain this")
	assert.Contains(t, string(data), "${SECRET}")
	assert.NotContains(t, string(data), "old-token")
	loaded, err := LoadFromFileOrEnv(files, "/repo/.bruin.yml")
	require.NoError(t, err)
	selected, err := loaded.ResolveCloudConnection()
	require.NoError(t, err)
	assert.Equal(t, "new-token", selected.Connection.APIToken)
	assert.Equal(t, "acme", loaded.GetDefaultTeam())
	info, err := files.Stat("/repo/.bruin.yml")
	require.NoError(t, err)
	assert.EqualValues(t, 0o600, info.Mode().Perm())
	require.ErrorContains(t, SaveCloudConnection(files, "/repo/.bruin.yml", original, ref, true, "acme"), "changed")
}

func TestResolveCloudConnection(t *testing.T) {
	t.Parallel()
	cm := Config{Environments: map[string]Environment{
		"dev":  {Connections: &Connections{BruinCloud: []BruinCloudConnection{{ConnectionMetadata: ConnectionMetadata{Name: "one"}, APIToken: "read"}}}},
		"prod": {Connections: &Connections{BruinCloud: []BruinCloudConnection{{ConnectionMetadata: ConnectionMetadata{Name: "two"}, APIToken: "write"}}}},
	}}
	_, err := cm.ResolveCloudConnection()
	require.ErrorContains(t, err, "multiple")
	cm.Cloud = &CloudConfig{Connection: "one", Environment: "dev"}
	ref, err := cm.ResolveCloudConnection()
	require.NoError(t, err)
	assert.Equal(t, "read", ref.Connection.APIToken)
	cm.Cloud.Connection = "missing"
	_, err = cm.ResolveCloudConnection()
	require.ErrorContains(t, err, "not found")
}
