package cloudauth

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
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

func TestActAsFailedSaveKeepsPreviousSession(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs a directory the process cannot write to")
	}
	secrets := keyring.NewArrayKeyring(nil)
	dir := filepath.Join(t.TempDir(), "bruin")
	store := Store{Path: filepath.Join(dir, "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	session := ActAsSession{AgentID: 12, AgentName: "Reporter", Team: "acme", ExpiresAt: time.Now().Add(time.Hour), APIURL: DefaultAPIURL, Token: "working-token"}
	require.NoError(t, store.SaveActAs(session))

	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	session.Token = "replacement-token"
	require.Error(t, store.SaveActAs(session))
	require.NoError(t, os.Chmod(dir, 0o700))

	loaded, err := store.ReadActAs(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "working-token", loaded.Token)
	keys, err := secrets.Keys()
	require.NoError(t, err)
	assert.Len(t, keys, 1)
}

type swappingSecrets struct {
	keyring.Keyring
	beforeGet func()
}

func (s *swappingSecrets) Get(key string) (keyring.Item, error) {
	if hook := s.beforeGet; hook != nil {
		s.beforeGet = nil
		hook()
	}
	return s.Keyring.Get(key)
}

func TestReadActAsRetriesWhenKeySwappedMidRead(t *testing.T) {
	t.Parallel()
	secrets := &swappingSecrets{Keyring: keyring.NewArrayKeyring(nil)}
	store := Store{Path: filepath.Join(t.TempDir(), "bruin", "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	session := ActAsSession{AgentID: 12, Team: "acme", ExpiresAt: time.Now().Add(time.Hour), APIURL: DefaultAPIURL, Token: "old-token"}
	require.NoError(t, store.SaveActAs(session))
	secrets.beforeGet = func() {
		session.Token = "renewed-token"
		require.NoError(t, store.SaveActAs(session))
	}

	loaded, err := store.ReadActAs(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, "renewed-token", loaded.Token)
}

// syncedSecrets stands in for the OS keychain, which is safe to share between processes.
type syncedSecrets struct {
	mu      sync.Mutex
	secrets *keyring.ArrayKeyring
}

func (s *syncedSecrets) Get(key string) (keyring.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.secrets.Get(key)
}

func (s *syncedSecrets) Set(item keyring.Item) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.secrets.Set(item)
}

func (s *syncedSecrets) Remove(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.secrets.Remove(key)
}

func TestReadActAsWaitsForWriters(t *testing.T) {
	t.Parallel()
	secrets := &syncedSecrets{secrets: keyring.NewArrayKeyring(nil)}
	store := Store{Path: filepath.Join(t.TempDir(), "bruin", "cloud.yml"), Open: func() (SecretStore, error) { return secrets, nil }}
	session := ActAsSession{AgentID: 12, Team: "acme", ExpiresAt: time.Now().Add(time.Hour), APIURL: DefaultAPIURL, Token: "old-token"}
	require.NoError(t, store.SaveActAs(session))

	read := make(chan string, 1)
	require.NoError(t, store.WithActAsLock(t.Context(), func() error {
		go func() {
			loaded, err := store.ReadActAs(DefaultAPIURL)
			if err != nil {
				read <- err.Error()
				return
			}
			read <- loaded.Token
		}()
		time.Sleep(200 * time.Millisecond)
		session.Token = "renewed-token"
		return store.SaveActAs(session)
	}))
	assert.Equal(t, "renewed-token", <-read)
}
