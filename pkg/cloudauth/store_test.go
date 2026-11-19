package cloudauth

import (
	"errors"
	"net/url"
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
	store := Store{Path: filepath.Join(t.TempDir(), "bruin", "cloud.yml"), Open: func() (SecretStore, error) {
		t.Fatal("a new login must not touch the OS credential store")
		return nil, nil
	}}
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
	credential.Token, credential.TokenID = "replacement", "2"
	require.ErrorContains(t, store.Save(credential, nil), "configuration changed")
	loaded, err = store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "private-token", loaded.Token)
	require.NoError(t, store.Save(credential, data))
	loaded, err = store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "replacement", loaded.Token)
	require.NoError(t, store.Delete())
	loaded, err = store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Nil(t, loaded)
	require.ErrorContains(t, store.Delete(), "no global login found")
}

func TestGlobalStorePreservesExistingConfig(t *testing.T) {
	t.Parallel()
	store := Store{Path: filepath.Join(t.TempDir(), "cloud.yml")}
	original := []byte("default_environment: production\nenvironments:\n  production:\n    connections:\n      bruin:\n        - name: existing\n          api_token: old\n      postgres:\n        - name: warehouse\n          password: '${WAREHOUSE_PASSWORD}'\n")
	require.NoError(t, os.WriteFile(store.Path, original, 0o600))
	loaded, err := store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "old", loaded.Token)
	credential := Credential{Token: "new-token", TokenID: "1", APIURL: DefaultAPIURL}
	require.ErrorContains(t, store.Save(credential, nil), "configuration changed")
	data, err := os.ReadFile(store.Path)
	require.NoError(t, err)
	assert.Equal(t, original, data)
	require.NoError(t, store.Save(credential, original))
	loaded, err = store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "new-token", loaded.Token)
	assert.Equal(t, "production", loaded.connection.Environment)
	assert.Equal(t, "existing", loaded.connection.Connection.Name)
	require.NoError(t, store.Delete())
	data, err = os.ReadFile(store.Path)
	require.NoError(t, err)
	assert.Contains(t, string(data), "name: warehouse")
	assert.Contains(t, string(data), "'${WAREHOUSE_PASSWORD}'")
	assert.NotContains(t, string(data), "api_token:")
}

// writeLegacyLogin writes a global login the way older versions did: the token in the credential store and a
// keyring:// pointer in cloud.yml.
func writeLegacyLogin(t *testing.T, store Store, secrets keyring.Keyring, token, tokenID string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(store.Path), 0o700))
	key, err := store.key(tokenID, DefaultAPIURL)
	require.NoError(t, err)
	require.NoError(t, secrets.Set(keyring.Item{Key: key, Data: []byte(token)}))
	reference := url.URL{Scheme: "keyring", Host: key, Path: "/" + tokenID}
	ref := config.CloudConnectionRef{Environment: "default", Connection: config.BruinCloudConnection{ConnectionMetadata: config.ConnectionMetadata{Name: "cloud"}, APIToken: reference.String(), APIURL: DefaultAPIURL}}
	data, err := config.MarshalCloudConnection(nil, ref, "acme")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(store.Path, data, 0o600))
	return key
}

func TestGlobalStoreMigratesLegacyPointer(t *testing.T) {
	t.Parallel()
	secrets := keyring.NewArrayKeyring(nil)
	opened := 0
	store := Store{Path: filepath.Join(t.TempDir(), "bruin", "cloud.yml"), Open: func() (SecretStore, error) {
		opened++
		return secrets, nil
	}}
	writeLegacyLogin(t, store, secrets, "legacy-token", "7")

	loaded, err := store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "legacy-token", loaded.Token)
	assert.Equal(t, "acme", loaded.DefaultTeam)
	assert.Equal(t, 1, opened)
	data, err := os.ReadFile(store.Path)
	require.NoError(t, err)
	assert.Contains(t, string(data), "api_token: legacy-token")
	assert.NotContains(t, string(data), "keyring://")
	keys, err := secrets.Keys()
	require.NoError(t, err)
	assert.Empty(t, keys)

	loaded, err = store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "legacy-token", loaded.Token)
	assert.Equal(t, 1, opened, "a migrated login must not open the credential store again")
}

func TestGlobalStoreLegacyPointerErrors(t *testing.T) {
	t.Parallel()
	secrets := keyring.NewArrayKeyring(nil)
	store := Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	key := writeLegacyLogin(t, store, secrets, "legacy-token", "7")
	data, err := os.ReadFile(store.Path)
	require.NoError(t, err)

	copied := Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: store.Open}
	require.NoError(t, os.WriteFile(copied.Path, data, 0o600))
	_, err = copied.Read(DefaultAPIURL)
	require.ErrorContains(t, err, "reference does not belong")
	require.NoError(t, copied.Delete())
	keys, err := secrets.Keys()
	require.NoError(t, err)
	assert.Equal(t, []string{key}, keys, "a copied pointer must not remove another file's token")

	store.Open = func() (SecretStore, error) { return nil, errors.New("locked") }
	_, err = store.Read(DefaultAPIURL)
	require.ErrorContains(t, err, "locked")
	current, err := os.ReadFile(store.Path)
	require.NoError(t, err)
	assert.Equal(t, data, current)

	store.Open = func() (SecretStore, error) { return secrets, nil }
	require.NoError(t, secrets.Remove(key))
	_, err = store.Read(DefaultAPIURL)
	require.ErrorContains(t, err, "cannot read global token")
}

func TestGlobalStoreReplacesLegacyPointer(t *testing.T) {
	t.Parallel()
	secrets := keyring.NewArrayKeyring(nil)
	store := Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	writeLegacyLogin(t, store, secrets, "legacy-token", "7")
	data, err := os.ReadFile(store.Path)
	require.NoError(t, err)
	require.NoError(t, store.Save(Credential{Token: "fresh-token", TokenID: "8", APIURL: DefaultAPIURL}, data))
	loaded, err := store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "fresh-token", loaded.Token)
	keys, err := secrets.Keys()
	require.NoError(t, err)
	assert.Empty(t, keys)
}

func TestGlobalStoreDeletesLegacyPointer(t *testing.T) {
	t.Parallel()
	secrets := keyring.NewArrayKeyring(nil)
	store := Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	writeLegacyLogin(t, store, secrets, "legacy-token", "7")
	require.NoError(t, store.Delete())
	keys, err := secrets.Keys()
	require.NoError(t, err)
	assert.Empty(t, keys)
	loaded, err := store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Nil(t, loaded)
}
