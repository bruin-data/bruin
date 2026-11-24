package cloudauth

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testActAsSession() ActAsSession {
	return ActAsSession{AgentID: 12, AgentName: "Reporter", Team: "acme", APIURL: DefaultAPIURL, Token: "agent-token", ExpiresAt: time.Now().Add(time.Hour).Truncate(time.Second)}
}

func TestActAsSessionRoundTrip(t *testing.T) {
	t.Parallel()
	store := Store{Path: filepath.Join(t.TempDir(), "bruin", "cloud.yml")}
	session, raw, err := store.LoadActAs()
	require.NoError(t, err)
	assert.Nil(t, session)
	assert.Nil(t, raw)

	require.NoError(t, store.SaveActAs(testActAsSession(), nil))
	if runtime.GOOS != "windows" {
		info, err := os.Stat(store.ActAsPath())
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	session, raw, err = store.LoadActAs()
	require.NoError(t, err)
	want := testActAsSession()
	assert.Equal(t, want.Token, session.Token)
	assert.True(t, want.ExpiresAt.Equal(session.ExpiresAt))

	require.Error(t, store.SaveActAs(testActAsSession(), []byte("stale")))
	require.NoError(t, store.SaveActAs(testActAsSession(), raw))

	cleared, err := store.ClearActAs(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 12, cleared.AgentID)
	cleared, err = store.ClearActAs(t.Context())
	require.NoError(t, err)
	assert.Nil(t, cleared)
}

func TestLoadActAsRejectsIncompleteSessions(t *testing.T) {
	t.Parallel()
	for name, content := range map[string]string{
		"corrupt yaml": "agent_id: [broken",
		"no api url":   "agent_id: 12\ntoken: agent-token\n",
		"no token":     "agent_id: 12\napi_url: " + DefaultAPIURL + "\n",
		"no agent":     "token: agent-token\napi_url: " + DefaultAPIURL + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := Store{Path: filepath.Join(t.TempDir(), "cloud.yml")}
			require.NoError(t, os.WriteFile(store.ActAsPath(), []byte(content), 0o600))
			_, _, err := store.LoadActAs()
			require.ErrorContains(t, err, "invalid act-as session")
			cleared, err := store.ClearActAs(t.Context())
			require.NoError(t, err)
			assert.Equal(t, &ActAsSession{}, cleared)
			assert.NoFileExists(t, store.ActAsPath())
		})
	}
}

func TestSaveActAsInUnwritableDirectoryKeepsSession(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs a directory the process cannot write to")
	}
	dir := filepath.Join(t.TempDir(), "bruin")
	store := Store{Path: filepath.Join(dir, "cloud.yml")}
	require.NoError(t, store.SaveActAs(testActAsSession(), nil))
	_, raw, err := store.LoadActAs()
	require.NoError(t, err)
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	replacement := testActAsSession()
	replacement.Token = "other-token"
	require.Error(t, store.SaveActAs(replacement, raw))
	session, _, err := store.LoadActAs()
	require.NoError(t, err)
	assert.Equal(t, "agent-token", session.Token)
}
