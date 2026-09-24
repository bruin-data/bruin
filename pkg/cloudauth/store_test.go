package cloudauth

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestGlobalStore(t *testing.T) {
	t.Parallel()
	store := Store{Path: filepath.Join(t.TempDir(), "bruin", "cloud.yml")}
	credential := Credential{Token: "private-token", APIURL: DefaultAPIURL, DefaultTeam: "acme"}
	require.NoError(t, store.Save(credential, nil))
	data, err := os.ReadFile(store.Path)
	require.NoError(t, err)
	var cm config.Config
	require.NoError(t, yaml.Unmarshal(data, &cm))
	assert.Equal(t, "default", cm.DefaultEnvironmentName)
	assert.Equal(t, "acme", cm.GetDefaultTeam())
	ref, err := cm.ResolveCloudConnection()
	require.NoError(t, err)
	require.NotNil(t, ref)
	assert.Equal(t, "cloud", ref.Connection.Name)
	assert.Equal(t, credential.Token, ref.Connection.APIToken)
	assert.Equal(t, DefaultAPIURL, ref.Connection.APIURL)
	if runtime.GOOS != "windows" {
		info, err := os.Stat(store.Path)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		info, err = os.Stat(filepath.Dir(store.Path))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	}
	loaded, err := store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, credential.Token, loaded.Token)
	assert.Equal(t, "acme", loaded.DefaultTeam)
	_, err = store.Read("https://attacker.example/api/v1")
	require.ErrorContains(t, err, "different Cloud API destination")
	credential.Token = "replacement"
	require.NoError(t, store.Save(credential, data))
	loaded, err = store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "replacement", loaded.Token)
	require.NoError(t, store.Delete())
	loaded, err = store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Nil(t, loaded)
	require.NoError(t, store.Delete())
}

func TestGlobalStorePreservesExistingConfig(t *testing.T) {
	t.Parallel()
	store := Store{Path: filepath.Join(t.TempDir(), "cloud.yml")}
	original := []byte("default_environment: production\nenvironments:\n  production:\n    connections:\n      bruin:\n        - name: existing\n          api_token: old\n      postgres:\n        - name: warehouse\n          password: '${WAREHOUSE_PASSWORD}'\n")
	require.NoError(t, os.WriteFile(store.Path, original, 0o600))
	credential := Credential{Token: "new", APIURL: DefaultAPIURL}
	require.ErrorContains(t, store.Save(credential, nil), "configuration changed")
	data, err := os.ReadFile(store.Path)
	require.NoError(t, err)
	assert.Equal(t, original, data)
	require.NoError(t, store.Save(credential, original))
	loaded, _, err := store.Load()
	require.NoError(t, err)
	assert.Equal(t, "new", loaded.Token)
	assert.Equal(t, "production", loaded.connection.Environment)
	assert.Equal(t, "existing", loaded.connection.Connection.Name)
	require.NoError(t, store.Delete())
	data, err = os.ReadFile(store.Path)
	require.NoError(t, err)
	assert.Contains(t, string(data), "name: warehouse")
	assert.Contains(t, string(data), "'${WAREHOUSE_PASSWORD}'")
	assert.NotContains(t, string(data), "api_token:")
}
