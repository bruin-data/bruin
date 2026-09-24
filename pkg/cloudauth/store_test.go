package cloudauth

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/99designs/keyring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobalStore(t *testing.T) {
	t.Parallel()
	secrets := keyring.NewArrayKeyring(nil)
	store := Store{Path: filepath.Join(t.TempDir(), "bruin", "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	credential := Credential{Token: "private-token", TokenID: "123", APIURL: DefaultAPIURL, ExpiresAt: time.Now().Add(time.Hour), DefaultTeam: "acme"}
	require.NoError(t, store.Save(credential, nil))
	data, err := os.ReadFile(store.Path)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "private-token")
	loaded, err := store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, credential.Token, loaded.Token)
	assert.Equal(t, "acme", loaded.DefaultTeam)
	_, err = store.Read("https://attacker.example/api/v1")
	require.Error(t, err)
	copied := Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: store.Open}
	require.NoError(t, os.WriteFile(copied.Path, data, 0o600))
	_, err = copied.Read(DefaultAPIURL)
	require.ErrorContains(t, err, "reference")
	credential.TokenID = "456"
	credential.Token = "replacement"
	require.NoError(t, store.Save(credential, data))
	keys, err := secrets.Keys()
	require.NoError(t, err)
	assert.Len(t, keys, 1)
	loaded, err = store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "replacement", loaded.Token)
}

func TestGlobalSaveFailureKeepsPreviousCredential(t *testing.T) {
	t.Parallel()
	secrets := keyring.NewArrayKeyring(nil)
	store := Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	require.NoError(t, store.Save(Credential{Token: "old", TokenID: "1", APIURL: DefaultAPIURL}, nil))
	err := store.Save(Credential{Token: "new", TokenID: "2", APIURL: DefaultAPIURL}, nil)
	require.Error(t, err)
	loaded, err := store.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "old", loaded.Token)
	keys, err := secrets.Keys()
	require.NoError(t, err)
	assert.Len(t, keys, 1)
	store.Open = func() (SecretStore, error) { return nil, errors.New("locked") }
	_, err = store.Read(DefaultAPIURL)
	require.ErrorContains(t, err, "locked")
}

func TestGlobalStoreUnavailableAndExpired(t *testing.T) {
	t.Parallel()
	secrets := keyring.NewArrayKeyring(nil)
	store := Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	require.NoError(t, store.Save(Credential{Token: "secret", TokenID: "1", APIURL: DefaultAPIURL, ExpiresAt: time.Now().Add(-time.Hour)}, nil))
	_, err := store.Read(DefaultAPIURL)
	require.ErrorContains(t, err, "expired")
	require.NoError(t, store.Delete())
	keys, err := secrets.Keys()
	require.NoError(t, err)
	assert.Empty(t, keys)
	_, err = os.Stat(store.Path)
	require.ErrorIs(t, err, os.ErrNotExist)
	store.Open = func() (SecretStore, error) { return nil, errors.New("unavailable") }
	require.ErrorContains(t, store.Save(Credential{Token: "secret", TokenID: "2", APIURL: DefaultAPIURL}, nil), "unavailable")
	_, err = os.Stat(store.Path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCopiedGlobalConfigurationCanBeReauthenticated(t *testing.T) {
	t.Parallel()
	secrets := keyring.NewArrayKeyring(nil)
	original := Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	require.NoError(t, original.Save(Credential{Token: "original", TokenID: "1", APIURL: DefaultAPIURL}, nil))
	data, err := os.ReadFile(original.Path)
	require.NoError(t, err)
	copied := Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: original.Open}
	require.NoError(t, os.WriteFile(copied.Path, data, 0o600))
	require.NoError(t, copied.Save(Credential{Token: "replacement", TokenID: "2", APIURL: DefaultAPIURL}, data))
	loaded, err := copied.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "replacement", loaded.Token)
	loaded, err = original.Read(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "original", loaded.Token)
}
