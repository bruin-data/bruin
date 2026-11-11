package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/fatih/color"
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

func (s *actAsStub) respond(status int, token, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
	s.body = fmt.Sprintf(`{"token":%q,"expires_at":%q,"agent":{"id":12,"name":"Reporter"},"team":"acme","permissions":["agent:list","dashboard:list"]}`, token, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	if message != "" {
		s.body = fmt.Sprintf(`{"message":%q,"error":"forbidden"}`, message)
	}
}

func (s *actAsStub) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

// holdNext parks the next request until the returned release runs; arrived closes once it is parked.
func (s *actAsStub) holdNext(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	hold, arrived := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })
	t.Cleanup(release)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hold, s.arrived = hold, arrived
	return arrived, release
}

func newActAsEnv(t *testing.T) (*actAsStub, cloudauth.Store) {
	t.Helper()
	loginTestRepo(t, "")
	t.Setenv("BRUIN_CLOUD_TEAM", "")
	stub := &actAsStub{}
	stub.respond(http.StatusCreated, "agent-token", "")
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
	// Same path as DefaultStore, so commands that open the default store see the session.
	store := cloudauth.Store{Path: filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "bruin", "cloud.yml"), Open: func() (cloudauth.SecretStore, error) { return secrets, nil }}
	require.NoError(t, store.Save(cloudauth.Credential{Token: "person-token", TokenID: "1", APIURL: server.URL, DefaultTeam: "acme"}, nil))
	return stub, store
}

func captureStdout(t *testing.T, run func() error) (string, error) {
	t.Helper()
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	originalStdout, originalColor := os.Stdout, color.Output
	os.Stdout, color.Output = writer, writer
	runErr := run()
	os.Stdout, color.Output = originalStdout, originalColor
	require.NoError(t, writer.Close())
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(data), runErr
}

func runActAsCLI(t *testing.T, store cloudauth.Store, args ...string) (string, error) {
	t.Helper()
	cloud := &cli.Command{Name: "cloud", Commands: []*cli.Command{actAsCommand(func() (cloudauth.Store, error) { return store, nil })}}
	addTeamFlag(cloud)
	root := &cli.Command{Name: "bruin", Commands: []*cli.Command{cloud}, ExitErrHandler: func(context.Context, *cli.Command, error) {}}
	return captureStdout(t, func() error {
		return root.Run(t.Context(), append([]string{"bruin", "cloud", "act-as"}, args...))
	})
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

func saveSession(t *testing.T, store cloudauth.Store, session cloudauth.ActAsSession) {
	t.Helper()
	if session.AgentID == 0 {
		session.AgentID, session.AgentName, session.Team = 12, "Reporter", "acme"
	}
	if session.APIURL == "" {
		session.APIURL = cloudauth.APIURL()
	}
	_, current, _ := store.LoadActAs()
	require.NoError(t, store.SaveActAs(session, current))
}

func TestActAsStartStatusStop(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	stub, store := newActAsEnv(t)
	output, err := runActAsCLI(t, store, "--agent-id", "12")
	require.NoError(t, err)
	session, _, err := store.LoadActAs()
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

	output, err = runActAsCLI(t, store, "--output", "json")
	require.NoError(t, err)
	var status actAsStatus
	require.NoError(t, json.Unmarshal([]byte(output), &status))
	assert.Equal(t, actAsStatus{Active: true, AgentID: 12, AgentName: "Reporter", Team: "acme", ExpiresAt: status.ExpiresAt}, status)

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
		output, err := runActAsCLI(t, store, args...)
		require.NoError(t, err)
		assert.Equal(t, step.want, output)
	}

	saveSession(t, store, cloudauth.ActAsSession{Token: "old-token", ExpiresAt: time.Now().Add(-time.Minute)})
	output, err = runActAsCLI(t, store)
	require.NoError(t, err)
	assert.Contains(t, output, "the agent token expired at")
}

//nolint:paralleltest // uses t.Chdir/t.Setenv
func TestActAsStartErrors(t *testing.T) {
	for _, tc := range []struct {
		name, refusal, want string
		args                []string
	}{
		{name: "server refusal", refusal: "You cannot act as this agent.", args: []string{"--agent-id", "12"}, want: "could not act as agent 12: You cannot act as this agent."},
		{name: "invalid id", args: []string{"--agent-id", "0"}, want: "--agent-id must be a positive integer, got 0"},
		{name: "start and stop", args: []string{"--agent-id", "12", "--stop"}, want: "--agent-id and --stop cannot be used together"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub, store := newActAsEnv(t)
			if tc.refusal != "" {
				stub.respond(http.StatusForbidden, "", tc.refusal)
			}
			output, err := runActAsCLI(t, store, tc.args...)
			require.Error(t, err)
			assert.Contains(t, output, tc.want)
			session, _, err := store.LoadActAs()
			require.NoError(t, err)
			assert.Nil(t, session)
		})
	}
}

func TestActAsStartWarnsWhenAPIKeyIsSet(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	_, store := newActAsEnv(t)
	t.Setenv("BRUIN_CLOUD_API_KEY", "env-token")
	output, err := runActAsCLI(t, store, "--agent-id", "12")
	require.NoError(t, err)
	assert.Contains(t, output, "BRUIN_CLOUD_API_KEY is set and takes precedence over acting as the agent.")
}

//nolint:paralleltest // uses t.Chdir/t.Setenv
func TestActAsResolution(t *testing.T) {
	for _, tc := range []struct {
		name, env, apiURL, wantToken, wantErr string
		session                               bool
		args                                  []string
	}{
		{name: "global login without session", wantToken: "person-token"},
		{name: "session beats global login", session: true, wantToken: "agent-token"},
		{name: "flag beats session", session: true, args: []string{"--api-key", "flag-token"}, wantToken: "flag-token"},
		{name: "env beats session", session: true, env: "env-token", wantToken: "env-token"},
		{name: "same team is fine", session: true, args: []string{"--team", "acme"}, wantToken: "agent-token"},
		{name: "session is pinned to its team", session: true, args: []string{"--team", "beta"}, wantErr: `acting as agent "Reporter" (12) is pinned to team "acme"; run 'bruin cloud act-as --stop' before using --team "beta"`},
		{name: "other API destination", session: true, apiURL: "https://other.example/api/v1", wantErr: "act-as session belongs to a different Cloud API destination; run 'bruin cloud act-as --stop'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub, store := newActAsEnv(t)
			t.Setenv("BRUIN_CLOUD_API_KEY", tc.env)
			if tc.session {
				saveSession(t, store, cloudauth.ActAsSession{Token: "agent-token", APIURL: tc.apiURL, ExpiresAt: time.Now().Add(time.Hour)})
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

func TestActAsSessionBeatsRepoLoginAndPinsTeam(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	_, store := newActAsEnv(t)
	require.NoError(t, os.WriteFile(".bruin.yml", []byte("cloud:\n  default_team: acme-corp\n"+loginExistingConfig), 0o600))
	saveSession(t, store, cloudauth.ActAsSession{Token: "agent-token", ExpiresAt: time.Now().Add(time.Hour)})
	auth, err := resolveIn(t, store)
	require.NoError(t, err)
	assert.Equal(t, "agent-token", auth.token)

	var output bytes.Buffer
	isDebug := false
	command := &cli.Command{Name: "bruin", Writer: &output, Commands: []*cli.Command{Cloud(&isDebug)}}
	require.NoError(t, command.Run(t.Context(), []string{"bruin", "cloud", "auth", "status"}))
	assert.Contains(t, output.String(), "Credential source: act-as")
	assert.Contains(t, output.String(), "Default team: acme\n")
}

//nolint:paralleltest // uses t.Chdir/t.Setenv
func TestActAsRenewal(t *testing.T) {
	const ended = `acting as agent "Reporter" (12) ended: %s. Run 'bruin cloud act-as --stop' to use your own login again`
	for _, tc := range []struct {
		name, refusal, wantToken, wantErr string
		status                            int
		expiresIn                         time.Duration
	}{
		{name: "renews with the person's login", status: http.StatusCreated, expiresIn: time.Minute, wantToken: "renewed-token"},
		{name: "refusal ends the session", status: http.StatusForbidden, refusal: "You no longer have access to this agent.", expiresIn: time.Minute, wantErr: fmt.Sprintf(ended, "You no longer have access to this agent")},
		{name: "outage keeps a still-valid token", status: http.StatusBadGateway, refusal: "Bad gateway", expiresIn: time.Minute, wantToken: "old-token"},
		{name: "outage after expiry ends the session", status: http.StatusBadGateway, refusal: "Bad gateway", expiresIn: -time.Minute, wantErr: fmt.Sprintf(ended, "Bad gateway")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub, store := newActAsEnv(t)
			saveSession(t, store, cloudauth.ActAsSession{Token: "old-token", ExpiresAt: time.Now().Add(tc.expiresIn)})
			stub.respond(tc.status, "renewed-token", tc.refusal)
			auth, err := resolveIn(t, store)
			assert.Equal(t, []string{"POST /agents/12/act-as Bearer person-token team=acme"}, stub.recorded())
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantToken, auth.token)
		})
	}
}

func resolveDuringRenewal(t *testing.T, stub *actAsStub, store cloudauth.Store, change func(), args ...string) (resolvedCloudAuth, error) {
	t.Helper()
	saveSession(t, store, cloudauth.ActAsSession{Token: "old-token", ExpiresAt: time.Now().Add(time.Minute)})
	stub.respond(http.StatusCreated, "renewed-token", "")
	arrived, release := stub.holdNext(t)
	type outcome struct {
		auth resolvedCloudAuth
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		auth, err := resolveIn(t, store, args...)
		done <- outcome{auth, err}
	}()
	<-arrived
	change()
	release()
	got := <-done
	return got.auth, got.err
}

func TestActAsStopWinsOverInFlightRenewal(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	stub, store := newActAsEnv(t)
	auth, err := resolveDuringRenewal(t, stub, store, func() {
		_, err := store.ClearActAs(t.Context())
		assert.NoError(t, err)
	})
	require.NoError(t, err)
	assert.Equal(t, "person-token", auth.token)
	session, _, err := store.LoadActAs()
	require.NoError(t, err)
	assert.Nil(t, session)
}

func TestActAsRenewalRechecksReplacedSession(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	stub, store := newActAsEnv(t)
	_, err := resolveDuringRenewal(t, stub, store, func() {
		saveSession(t, store, cloudauth.ActAsSession{AgentID: 34, AgentName: "Other", Team: "beta", Token: "beta-token", ExpiresAt: time.Now().Add(time.Hour)})
	}, "--team", "acme")
	require.ErrorContains(t, err, `pinned to team "beta"`)
	session, _, err := store.LoadActAs()
	require.NoError(t, err)
	assert.Equal(t, "beta-token", session.Token)
}

func TestActAsStopRecoversCorruptSession(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	_, store := newActAsEnv(t)
	require.NoError(t, os.WriteFile(store.ActAsPath(), []byte("agent_id: [broken"), 0o600))
	_, err := resolveIn(t, store)
	require.ErrorContains(t, err, "invalid act-as session")
	output, err := runActAsCLI(t, store, "--stop")
	require.NoError(t, err)
	assert.Equal(t, "Stopped acting as an agent. Cloud commands use your own login again.\n", output)
	auth, err := resolveIn(t, store)
	require.NoError(t, err)
	assert.Equal(t, "person-token", auth.token)
}

func TestLogoutGlobalEndsActAs(t *testing.T) { //nolint:paralleltest // uses t.Chdir/t.Setenv
	_, store := newActAsEnv(t)
	saveSession(t, store, cloudauth.ActAsSession{Token: "agent-token", ExpiresAt: time.Now().Add(time.Hour)})
	var output bytes.Buffer
	command := logoutCommand(logoutDependencies{globalStore: func() (cloudauth.Store, error) { return store, nil }, interactive: func() bool { return false }})
	command.Writer = &output
	require.NoError(t, command.Run(t.Context(), []string{"logout", "--global"}))
	assert.Contains(t, output.String(), `Stopped acting as agent "Reporter" (12).`)
	session, _, err := store.LoadActAs()
	require.NoError(t, err)
	assert.Nil(t, session)
}
