package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/99designs/keyring"
	"github.com/bruin-data/bruin/pkg/cloudauth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

type actAsRequest struct {
	path, auth, team string
}

type actAsServer struct {
	mu       sync.Mutex
	requests []actAsRequest
	status   int
	body     string
}

func (s *actAsServer) respond(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status, s.body = status, body
}

func (s *actAsServer) calls() []actAsRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]actAsRequest(nil), s.requests...)
}

func actAsResponse(t *testing.T, token string, expiresAt time.Time, team string) string {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"token":       token,
		"expires_at":  expiresAt.UTC().Format(time.RFC3339),
		"agent":       map[string]any{"id": 12, "name": "Reporter"},
		"team":        team,
		"permissions": []string{"agent:list", "dashboard:list"},
	})
	require.NoError(t, err)
	return string(data)
}

func newActAsTestEnv(t *testing.T) (*actAsServer, cloudauth.Store) {
	t.Helper()
	loginTestRepo(t, "")
	t.Setenv("BRUIN_CLOUD_TEAM", "")
	stub := &actAsServer{status: http.StatusCreated}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stub.mu.Lock()
		stub.requests = append(stub.requests, actAsRequest{path: r.Method + " " + r.URL.Path, auth: r.Header.Get("Authorization"), team: r.Header.Get("X-Bruin-Team")})
		status, body := stub.status, stub.body
		stub.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
	secrets := keyring.NewArrayKeyring(nil)
	store := cloudauth.Store{Path: filepath.Join(t.TempDir(), "bruin", "cloud.yml"), Open: func() (cloudauth.SecretStore, error) { return secrets, nil }}
	require.NoError(t, store.Save(cloudauth.Credential{Token: "person-token", TokenID: "1", APIURL: server.URL, DefaultTeam: "acme"}, nil))
	return stub, store
}

func runActAsCommand(t *testing.T, store cloudauth.Store, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	cloud := &cli.Command{Name: "cloud", Commands: []*cli.Command{actAsCommand(actAsDependencies{globalStore: func() (cloudauth.Store, error) { return store, nil }})}}
	addTeamFlag(cloud)
	root := &cli.Command{Name: "bruin", Writer: &output, Commands: []*cli.Command{cloud}, ExitErrHandler: func(context.Context, *cli.Command, error) {}}
	err := root.Run(t.Context(), append([]string{"bruin", "cloud", "act-as"}, args...))
	return output.String(), err
}

func resolveWithActAs(t *testing.T, store cloudauth.Store, args ...string) (resolvedCloudAuth, error) {
	t.Helper()
	var auth resolvedCloudAuth
	command := &cli.Command{Name: "test", Flags: []cli.Flag{apiKeyFlag(), teamFlag()}, Action: func(_ context.Context, c *cli.Command) error {
		var err error
		auth, err = resolveCloudAuthWithActAs(c, func() (cloudauth.Store, error) { return store, nil })
		return err
	}}
	err := command.Run(t.Context(), append([]string{"test"}, args...))
	return auth, err
}

func saveActAsSession(t *testing.T, store cloudauth.Store, token, team string, expiresAt time.Time) {
	t.Helper()
	require.NoError(t, store.SaveActAs(cloudauth.ActAsSession{AgentID: 12, AgentName: "Reporter", Team: team, ExpiresAt: expiresAt, APIURL: cloudauth.APIURL(), Token: token}))
}

func TestActAsStartStatusStop(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	stub, store := newActAsTestEnv(t)
	expiresAt := time.Now().Add(time.Hour).Truncate(time.Second)
	stub.respond(http.StatusCreated, actAsResponse(t, "agent-token", expiresAt, "acme"))

	output, err := runActAsCommand(t, store, "--agent", "12")
	require.NoError(t, err)
	assert.Equal(t, "Acting as agent \"Reporter\" (12) on team acme until "+actAsClock(expiresAt)+". Cloud commands now use the agent's permissions: agent:list, dashboard:list. Run 'bruin cloud act-as --stop' to go back.\n", output)
	assert.Equal(t, []actAsRequest{{path: "POST /agents/12/act-as", auth: "Bearer person-token", team: "acme"}}, stub.calls())

	session, err := store.ReadActAs(cloudauth.APIURL())
	require.NoError(t, err)
	require.NotNil(t, session)
	assert.Equal(t, "agent-token", session.Token)
	assert.Equal(t, "acme", session.Team)
	assert.True(t, expiresAt.Equal(session.ExpiresAt))
	data, err := os.ReadFile(store.ActAsPath())
	require.NoError(t, err)
	assert.NotContains(t, string(data), "agent-token")
	if runtime.GOOS != osWindows {
		info, err := os.Stat(store.ActAsPath())
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}

	output, err = runActAsCommand(t, store)
	require.NoError(t, err)
	assert.Equal(t, "Acting as agent \"Reporter\" (12) on team acme until "+actAsClock(expiresAt)+".\nPermissions: agent:list, dashboard:list\n", output)

	output, err = runActAsCommand(t, store, "--output", "json")
	require.NoError(t, err)
	var status actAsStatus
	require.NoError(t, json.Unmarshal([]byte(output), &status))
	assert.True(t, status.Active)
	assert.Equal(t, &actAsAgent{ID: 12, Name: "Reporter"}, status.Agent)
	assert.Equal(t, "acme", status.Team)

	output, err = runActAsCommand(t, store, "--stop")
	require.NoError(t, err)
	assert.Equal(t, "Stopped acting as agent \"Reporter\" (12). Cloud commands use your own login again.\n", output)
	session, err = store.LoadActAs()
	require.NoError(t, err)
	assert.Nil(t, session)

	output, err = runActAsCommand(t, store)
	require.NoError(t, err)
	assert.Equal(t, "Not acting as an agent.\n", output)
	output, err = runActAsCommand(t, store, "--stop")
	require.NoError(t, err)
	assert.Equal(t, "Not acting as an agent.\n", output)
}

func TestActAsStartUsesTeamFlagAndPersonalLoginOnly(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	stub, store := newActAsTestEnv(t)
	saveActAsSession(t, store, "previous-agent-token", "acme", time.Now().Add(time.Hour))
	stub.respond(http.StatusCreated, actAsResponse(t, "agent-token", time.Now().Add(time.Hour), "beta"))

	_, err := runActAsCommand(t, store, "--agent", "12", "--team", "beta")
	require.NoError(t, err)
	assert.Equal(t, []actAsRequest{{path: "POST /agents/12/act-as", auth: "Bearer person-token", team: "beta"}}, stub.calls())
	session, err := store.ReadActAs(cloudauth.APIURL())
	require.NoError(t, err)
	assert.Equal(t, "beta", session.Team)
	assert.Equal(t, "agent-token", session.Token)
}

func TestActAsStartSurfacesServerMessage(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	stub, store := newActAsTestEnv(t)
	stub.respond(http.StatusForbidden, `{"message":"You cannot act as this agent.","error":"forbidden"}`)

	_, err := runActAsCommand(t, store, "--agent", "12")
	require.EqualError(t, err, "could not act as agent 12: You cannot act as this agent.")
	session, err := store.LoadActAs()
	require.NoError(t, err)
	assert.Nil(t, session)

	output, err := runActAsCommand(t, store, "--agent", "12", "--output", "json")
	require.Error(t, err)
	assert.JSONEq(t, `{"error":"could not act as agent 12: You cannot act as this agent."}`, output)
}

func TestActAsFlagValidation(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	_, store := newActAsTestEnv(t)
	_, err := runActAsCommand(t, store, "--agent", "12", "--stop")
	require.EqualError(t, err, "--agent and --stop cannot be used together")
	_, err = runActAsCommand(t, store, "--agent", "0")
	require.ErrorContains(t, err, "positive agent ID")
}

func TestActAsNeedsGlobalLogin(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	stub, store := newActAsTestEnv(t)
	require.NoError(t, store.Delete())
	_, err := runActAsCommand(t, store, "--agent", "12")
	require.ErrorIs(t, err, errActAsNeedsGlobalLogin)
	assert.Empty(t, stub.calls())
}

func TestActAsResolutionPrecedence(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	stub, store := newActAsTestEnv(t)
	dir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".bruin.yml"), []byte(loginExistingConfig), 0o600))

	auth, err := resolveWithActAs(t, store)
	require.NoError(t, err)
	assert.Equal(t, "original-token", auth.token)
	assert.Equal(t, "repo", auth.source)

	saveActAsSession(t, store, "agent-token", "acme", time.Now().Add(time.Hour))
	auth, err = resolveWithActAs(t, store)
	require.NoError(t, err)
	assert.Equal(t, "agent-token", auth.token)
	assert.Equal(t, cloudAuthActAs, auth.source)
	assert.Equal(t, "acme", auth.team)

	auth, err = resolveWithActAs(t, store, "--api-key", "flag-token")
	require.NoError(t, err)
	assert.Equal(t, "flag-token", auth.token)
	t.Setenv("BRUIN_CLOUD_API_KEY", "env-token")
	auth, err = resolveWithActAs(t, store)
	require.NoError(t, err)
	assert.Equal(t, "env-token", auth.token)
	assert.Empty(t, stub.calls())
}

func TestActAsSessionBeatsGlobalLogin(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	_, store := newActAsTestEnv(t)
	auth, err := resolveWithActAs(t, store)
	require.NoError(t, err)
	assert.Equal(t, "person-token", auth.token)
	assert.Equal(t, cloudAuthGlobal, auth.source)

	saveActAsSession(t, store, "agent-token", "acme", time.Now().Add(time.Hour))
	auth, err = resolveWithActAs(t, store)
	require.NoError(t, err)
	assert.Equal(t, "agent-token", auth.token)
}

func TestActAsTeamIsPinned(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	_, store := newActAsTestEnv(t)
	saveActAsSession(t, store, "agent-token", "acme", time.Now().Add(time.Hour))

	_, err := resolveWithActAs(t, store, "--team", "beta")
	require.EqualError(t, err, "acting as agent \"Reporter\" (12) is pinned to team \"acme\"; run 'bruin cloud act-as --stop' before using --team \"beta\"")
	auth, err := resolveWithActAs(t, store, "--team", "acme")
	require.NoError(t, err)
	assert.Equal(t, "agent-token", auth.token)

	require.NoError(t, os.WriteFile(".bruin.yml", []byte(configWithDefaultTeamYML), 0o600))
	command := &cli.Command{Name: "test", Flags: []cli.Flag{teamFlag()}, Action: func(_ context.Context, c *cli.Command) error {
		assert.Equal(t, "acme", auth.teamFor(c))
		return nil
	}}
	require.NoError(t, command.Run(t.Context(), []string{"test"}))
}

func TestActAsRefreshesExpiringToken(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	stub, store := newActAsTestEnv(t)
	saveActAsSession(t, store, "old-agent-token", "beta", time.Now().Add(time.Minute))
	expiresAt := time.Now().Add(time.Hour).Truncate(time.Second)
	stub.respond(http.StatusCreated, actAsResponse(t, "new-agent-token", expiresAt, "beta"))

	auth, err := resolveWithActAs(t, store)
	require.NoError(t, err)
	assert.Equal(t, "new-agent-token", auth.token)
	assert.Equal(t, "beta", auth.team)
	assert.Equal(t, []actAsRequest{{path: "POST /agents/12/act-as", auth: "Bearer person-token", team: "beta"}}, stub.calls())

	session, err := store.ReadActAs(cloudauth.APIURL())
	require.NoError(t, err)
	assert.Equal(t, "new-agent-token", session.Token)
	assert.True(t, expiresAt.Equal(session.ExpiresAt))

	auth, err = resolveWithActAs(t, store)
	require.NoError(t, err)
	assert.Equal(t, "new-agent-token", auth.token)
	assert.Len(t, stub.calls(), 1)
}

func TestActAsRefreshFailureEndsSession(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	stub, store := newActAsTestEnv(t)
	saveActAsSession(t, store, "old-agent-token", "acme", time.Now().Add(-time.Minute))
	stub.respond(http.StatusForbidden, `{"message":"You no longer have access to this agent.","error":"forbidden"}`)

	_, err := resolveWithActAs(t, store)
	require.EqualError(t, err, "acting as agent \"Reporter\" (12) ended: You no longer have access to this agent. Run 'bruin cloud act-as --stop' to use your own login again")

	output, err := runActAsCommand(t, store)
	require.NoError(t, err)
	assert.Contains(t, output, "The agent token expired at")
}

func TestLogoutClearsActAsSession(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	_, store := newActAsTestEnv(t)
	saveActAsSession(t, store, "agent-token", "acme", time.Now().Add(time.Hour))
	var output bytes.Buffer
	command := logoutCommand(logoutDependencies{globalStore: func() (cloudauth.Store, error) { return store, nil }, interactive: func() bool { return false }})
	command.Writer = &output
	require.NoError(t, command.Run(t.Context(), []string{"logout", "--global"}))
	assert.Contains(t, output.String(), "Stopped acting as agent \"Reporter\" (12).")
	session, err := store.LoadActAs()
	require.NoError(t, err)
	assert.Nil(t, session)
	credential, err := store.Read(cloudauth.APIURL())
	require.NoError(t, err)
	assert.Nil(t, credential)
}
