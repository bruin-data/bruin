package cmd

import (
	"bytes"
	"context"
	"fmt"
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

type actAsStub struct {
	mu      sync.Mutex
	status  int
	body    string
	calls   []string
	hold    chan struct{}
	arrived chan struct{}
}

func (s *actAsStub) respond(status int, token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
	s.body = fmt.Sprintf(`{"token":%q,"expires_at":%q,"agent":{"id":12,"name":"Reporter"},"team":"acme","permissions":["agent:list","dashboard:list"]}`, token, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	if status != http.StatusCreated {
		s.body = `{"message":"` + token + `","error":"forbidden"}`
	}
}

func (s *actAsStub) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func newActAsEnv(t *testing.T) (*actAsStub, cloudauth.Store) {
	t.Helper()
	loginTestRepo(t, "")
	t.Setenv("BRUIN_CLOUD_TEAM", "")
	stub := &actAsStub{}
	stub.respond(http.StatusCreated, "agent-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stub.mu.Lock()
		stub.calls = append(stub.calls, r.Method+" "+r.URL.Path+" "+r.Header.Get("Authorization")+" team="+r.Header.Get("X-Bruin-Team"))
		status, body, hold, arrived := stub.status, stub.body, stub.hold, stub.arrived
		stub.hold = nil
		stub.mu.Unlock()
		if hold != nil {
			close(arrived)
			<-hold
		}
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

func runActAs(t *testing.T, store cloudauth.Store, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	cloud := &cli.Command{Name: "cloud", Commands: []*cli.Command{actAsCommand(func() (cloudauth.Store, error) { return store, nil })}}
	addTeamFlag(cloud)
	err := (&cli.Command{Name: "bruin", Writer: &output, Commands: []*cli.Command{cloud}}).Run(t.Context(), append([]string{"bruin", "cloud", "act-as"}, args...))
	return output.String(), err
}

func resolveIn(t *testing.T, store cloudauth.Store, args ...string) (resolvedCloudAuth, error) {
	t.Helper()
	var auth resolvedCloudAuth
	command := &cli.Command{Name: "test", Flags: []cli.Flag{apiKeyFlag(), teamFlag()}, Action: func(_ context.Context, c *cli.Command) error {
		var err error
		auth, err = resolveCloudAuthIn(c, func() (cloudauth.Store, error) { return store, nil })
		return err
	}}
	err := command.Run(t.Context(), append([]string{"test"}, args...))
	return auth, err
}

func saveSession(t *testing.T, store cloudauth.Store, token string, expiresIn time.Duration) {
	t.Helper()
	require.NoError(t, store.SaveActAs(cloudauth.ActAsSession{AgentID: 12, AgentName: "Reporter", Team: "acme", APIURL: cloudauth.APIURL(), Token: token, ExpiresAt: time.Now().Add(expiresIn)}))
}

func TestActAsStartStatusStop(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	stub, store := newActAsEnv(t)
	output, err := runActAs(t, store, "--agent", "12")
	require.NoError(t, err)
	session, err := store.LoadActAs()
	require.NoError(t, err)
	until := actAsClock(session.ExpiresAt)
	assert.Equal(t, `Acting as agent "Reporter" (12) on team acme until `+until+". Cloud commands now use the agent's permissions: agent:list, dashboard:list. Run 'bruin cloud act-as --stop' to go back.\n", output)
	assert.Equal(t, []string{"POST /agents/12/act-as Bearer person-token team=acme"}, stub.recorded())
	assert.Equal(t, "agent-token", session.Token)
	if runtime.GOOS != osWindows {
		info, err := os.Stat(store.ActAsPath())
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}

	for _, step := range []struct{ args, want string }{
		{"", `Acting as agent "Reporter" (12) on team acme until ` + until + ".\n"},
		{"--stop", `Stopped acting as agent "Reporter" (12). Cloud commands use your own login again.` + "\n"},
		{"", "Not acting as an agent.\n"},
		{"--stop", "Not acting as an agent.\n"},
	} {
		args := []string{}
		if step.args != "" {
			args = append(args, step.args)
		}
		output, err := runActAs(t, store, args...)
		require.NoError(t, err)
		assert.Equal(t, step.want, output)
	}
}

func TestActAsStartRefused(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	stub, store := newActAsEnv(t)
	stub.respond(http.StatusForbidden, "You cannot act as this agent.")
	_, err := runActAs(t, store, "--agent", "12")
	require.EqualError(t, err, "could not act as agent 12: You cannot act as this agent.")
	session, err := store.LoadActAs()
	require.NoError(t, err)
	assert.Nil(t, session)
}

//nolint:paralleltest // uses t.Chdir/t.Setenv
func TestActAsResolution(t *testing.T) {
	for _, tc := range []struct {
		name, env, wantToken, wantErr string
		session                       bool
		args                          []string
	}{
		{name: "global login without session", wantToken: "person-token"},
		{name: "session beats global login", session: true, wantToken: "agent-token"},
		{name: "flag beats session", session: true, args: []string{"--api-key", "flag-token"}, wantToken: "flag-token"},
		{name: "env beats session", session: true, env: "env-token", wantToken: "env-token"},
		{name: "same team is fine", session: true, args: []string{"--team", "acme"}, wantToken: "agent-token"},
		{name: "session is pinned to its team", session: true, args: []string{"--team", "beta"}, wantErr: `acting as agent "Reporter" (12) is pinned to team "acme"; run 'bruin cloud act-as --stop' before using --team "beta"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub, store := newActAsEnv(t)
			t.Setenv("BRUIN_CLOUD_API_KEY", tc.env)
			if tc.session {
				saveSession(t, store, "agent-token", time.Hour)
			}
			auth, err := resolveIn(t, store, tc.args...)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantToken, auth.token)
			assert.Empty(t, stub.recorded())
		})
	}
}

func TestActAsSessionBeatsRepoLogin(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	_, store := newActAsEnv(t)
	require.NoError(t, os.WriteFile(".bruin.yml", []byte(loginExistingConfig), 0o600))
	saveSession(t, store, "agent-token", time.Hour)
	auth, err := resolveIn(t, store)
	require.NoError(t, err)
	assert.Equal(t, "agent-token", auth.token)
	assert.Equal(t, "acme", auth.team)
}

//nolint:paralleltest // uses t.Chdir/t.Setenv
func TestActAsRenewal(t *testing.T) {
	for _, tc := range []struct {
		name, reply, wantToken, wantErr string
		status                          int
	}{
		{name: "renews with the person's login", status: http.StatusCreated, reply: "renewed-token", wantToken: "renewed-token"},
		{name: "refusal ends the session", status: http.StatusForbidden, reply: "You no longer have access to this agent.", wantErr: `acting as agent "Reporter" (12) ended: You no longer have access to this agent. Run 'bruin cloud act-as --stop' to use your own login again`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub, store := newActAsEnv(t)
			saveSession(t, store, "old-token", time.Minute)
			stub.respond(tc.status, tc.reply)
			auth, err := resolveIn(t, store)
			assert.Equal(t, []string{"POST /agents/12/act-as Bearer person-token team=acme"}, stub.recorded())
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantToken, auth.token)
			session, err := store.LoadActAs()
			require.NoError(t, err)
			assert.Equal(t, tc.wantToken, session.Token)
		})
	}
}

func TestActAsStopWinsOverInFlightRenewal(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	stub, store := newActAsEnv(t)
	saveSession(t, store, "old-token", time.Minute)
	stub.respond(http.StatusCreated, "renewed-token")
	hold, arrived := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })
	t.Cleanup(release)
	stub.mu.Lock()
	stub.hold, stub.arrived = hold, arrived
	stub.mu.Unlock()

	type outcome struct {
		auth resolvedCloudAuth
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		auth, err := resolveIn(t, store)
		done <- outcome{auth, err}
	}()
	<-arrived
	_, err := runActAs(t, store, "--stop")
	require.NoError(t, err)
	release()

	got := <-done
	require.NoError(t, got.err)
	assert.Equal(t, "person-token", got.auth.token)
	session, err := store.LoadActAs()
	require.NoError(t, err)
	assert.Nil(t, session)
}

func TestActAsFailedWriteKeepsSession(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	if runtime.GOOS == osWindows || os.Geteuid() == 0 {
		t.Skip("needs a directory the process cannot write to")
	}
	_, store := newActAsEnv(t)
	saveSession(t, store, "working-token", time.Hour)
	dir := filepath.Dir(store.Path)
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	require.Error(t, store.SaveActAs(cloudauth.ActAsSession{AgentID: 34, Token: "other-token"}))
	auth, err := resolveIn(t, store)
	require.NoError(t, err)
	assert.Equal(t, "working-token", auth.token)
}

func TestLogoutGlobalEndsActAs(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	_, store := newActAsEnv(t)
	saveSession(t, store, "agent-token", time.Hour)
	var output bytes.Buffer
	command := logoutCommand(logoutDependencies{globalStore: func() (cloudauth.Store, error) { return store, nil }, interactive: func() bool { return false }})
	command.Writer = &output
	require.NoError(t, command.Run(t.Context(), []string{"logout", "--global"}))
	assert.Contains(t, output.String(), `Stopped acting as agent "Reporter" (12).`)
	session, err := store.LoadActAs()
	require.NoError(t, err)
	assert.Nil(t, session)
}
