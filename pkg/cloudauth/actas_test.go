package cloudauth

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/99designs/keyring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActAsSessionStore(t *testing.T) {
	t.Parallel()
	secrets := keyring.NewArrayKeyring(nil)
	store := Store{Path: filepath.Join(t.TempDir(), "bruin", "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	assert.Equal(t, filepath.Join(filepath.Dir(store.Path), "cloud-act-as.yml"), store.ActAsPath())

	session, err := store.ReadActAs(DefaultAPIURL)
	require.NoError(t, err)
	assert.Nil(t, session)
	cleared, err := store.ClearActAs()
	require.NoError(t, err)
	assert.Nil(t, cleared)
	require.ErrorContains(t, store.SaveActAs(ActAsSession{AgentID: 12, APIURL: DefaultAPIURL}), "incomplete")

	expiresAt := time.Now().Add(time.Hour).Truncate(time.Second)
	require.NoError(t, store.SaveActAs(ActAsSession{AgentID: 12, AgentName: "Reporter", Team: "acme", ExpiresAt: expiresAt, APIURL: DefaultAPIURL, Permissions: []string{"agent:list"}, Token: "agent-token"}))
	data, err := os.ReadFile(store.ActAsPath())
	require.NoError(t, err)
	assert.NotContains(t, string(data), "agent-token")

	session, err = store.ReadActAs(DefaultAPIURL)
	require.NoError(t, err)
	require.NotNil(t, session)
	assert.Equal(t, "agent-token", session.Token)
	assert.Equal(t, "Reporter", session.AgentName)
	assert.Equal(t, []string{"agent:list"}, session.Permissions)
	assert.True(t, expiresAt.Equal(session.ExpiresAt))
	_, err = store.ReadActAs("https://attacker.example/api/v1")
	require.ErrorContains(t, err, "different Cloud API destination")

	require.NoError(t, store.SaveActAs(ActAsSession{AgentID: 12, AgentName: "Reporter", Team: "acme", ExpiresAt: expiresAt, APIURL: DefaultAPIURL, Token: "renewed-token"}))
	session, err = store.ReadActAs(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "renewed-token", session.Token)
	keys, err := secrets.Keys()
	require.NoError(t, err)
	assert.Len(t, keys, 1)

	cleared, err = store.ClearActAs()
	require.NoError(t, err)
	require.NotNil(t, cleared)
	assert.Equal(t, 12, cleared.AgentID)
	keys, err = secrets.Keys()
	require.NoError(t, err)
	assert.Empty(t, keys)
	_, err = os.Stat(store.ActAsPath())
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestActAsSessionInvalidFileCanBeCleared(t *testing.T) {
	t.Parallel()
	secrets := keyring.NewArrayKeyring(nil)
	store := Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	require.NoError(t, os.WriteFile(store.ActAsPath(), []byte("agent_id: [broken"), 0o600))
	_, err := store.LoadActAs()
	require.ErrorContains(t, err, "act-as --stop")
	cleared, err := store.ClearActAs()
	require.NoError(t, err)
	assert.NotNil(t, cleared)
	session, err := store.LoadActAs()
	require.NoError(t, err)
	assert.Nil(t, session)
}
