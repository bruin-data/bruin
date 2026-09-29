package cloudauth

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/99designs/keyring"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestGlobalStore(t *testing.T) {
	t.Parallel()
	secrets := keyring.NewArrayKeyring(nil)
	store := Store{Path: filepath.Join(t.TempDir(), "bruin", "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	require.ErrorContains(t, store.Delete(), "no global login found")
	credential := Credential{Token: "private-token", TokenID: "1", APIURL: DefaultAPIURL, DefaultTeam: "acme"}
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
	assert.Contains(t, ref.Connection.APIToken, "keyring://bruin-cloud-")
	assert.NotContains(t, string(data), credential.Token)
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
	copied := Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: store.Open}
	require.NoError(t, os.WriteFile(copied.Path, data, 0o600))
	_, err = copied.Read(DefaultAPIURL)
	require.ErrorContains(t, err, "reference does not belong")
	require.ErrorContains(t, copied.Delete(), "reference does not belong")
	credential.Token, credential.TokenID = "replacement", "2"
	require.ErrorContains(t, store.Save(credential, nil), "configuration changed")
	loaded, err = store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "private-token", loaded.Token)
	keys, err := secrets.Keys()
	require.NoError(t, err)
	assert.Len(t, keys, 1)
	require.NoError(t, store.Save(credential, data))
	require.ErrorContains(t, store.Save(credential, data), "existing token identity")
	keys, err = secrets.Keys()
	require.NoError(t, err)
	assert.Len(t, keys, 1)
	loaded, err = store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "replacement", loaded.Token)
	require.NoError(t, store.Delete())
	loaded, err = store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Nil(t, loaded)
	keys, err = secrets.Keys()
	require.NoError(t, err)
	assert.Empty(t, keys)
	require.ErrorContains(t, store.Delete(), "no global login found")
}

func TestGlobalStorePreservesExistingConfig(t *testing.T) {
	t.Parallel()
	secrets := keyring.NewArrayKeyring(nil)
	store := Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	original := []byte("default_environment: production\nenvironments:\n  production:\n    connections:\n      bruin:\n        - name: existing\n          api_token: old\n      postgres:\n        - name: warehouse\n          password: '${WAREHOUSE_PASSWORD}'\n")
	require.NoError(t, os.WriteFile(store.Path, original, 0o600))
	_, err := store.Read(DefaultAPIURL)
	require.ErrorContains(t, err, "credential-store reference")
	credential := Credential{Token: "new-token", TokenID: "1", APIURL: DefaultAPIURL}
	require.ErrorContains(t, store.Save(credential, nil), "configuration changed")
	data, err := os.ReadFile(store.Path)
	require.NoError(t, err)
	assert.Equal(t, original, data)
	require.NoError(t, store.Save(credential, original))
	loaded, _, err := store.Load()
	require.NoError(t, err)
	assert.Empty(t, loaded.Token)
	assert.Equal(t, "production", loaded.connection.Environment)
	assert.Equal(t, "existing", loaded.connection.Connection.Name)
	data, err = os.ReadFile(store.Path)
	require.NoError(t, err)
	assert.NotContains(t, string(data), credential.Token)
	store.Open = func() (SecretStore, error) { return nil, errors.New("locked") }
	_, err = store.Read(DefaultAPIURL)
	require.ErrorContains(t, err, "locked")
	credential.TokenID = "2"
	require.ErrorContains(t, store.Save(credential, data), "locked")
	current, err := os.ReadFile(store.Path)
	require.NoError(t, err)
	assert.Equal(t, data, current)
	store.Open = func() (SecretStore, error) { return secrets, nil }
	require.NoError(t, secrets.Remove(loaded.key))
	_, err = store.Read(DefaultAPIURL)
	require.ErrorContains(t, err, "cannot read global token")
	require.NoError(t, store.Delete())
	data, err = os.ReadFile(store.Path)
	require.NoError(t, err)
	assert.Contains(t, string(data), "name: warehouse")
	assert.Contains(t, string(data), "'${WAREHOUSE_PASSWORD}'")
	assert.NotContains(t, string(data), "api_token:")
}
