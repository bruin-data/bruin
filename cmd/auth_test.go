package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/99designs/keyring"
	"github.com/bruin-data/bruin/pkg/cloudauth"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

//nolint:paralleltest
func TestLogoutDestination(t *testing.T) {
	for _, tc := range []struct {
		name         string
		args         []string
		input        string
		interactive  bool
		noGit        bool
		target       string
		revokeStatus int
		wantError    string
	}{
		{name: "choose repository", input: "\r", interactive: true, target: "repo"},
		{name: "choose global", input: "\x1b[B\r", interactive: true, target: "global"},
		{name: "revoke repository", args: []string{"--revoke"}, input: "\r", interactive: true, target: "repo"},
		{name: "revoke global", args: []string{"--revoke"}, input: "\x1b[B\r", interactive: true, target: "global"},
		{name: "repository revocation fails", args: []string{"--revoke"}, input: "\r", interactive: true, target: "repo", revokeStatus: http.StatusInternalServerError, wantError: "repository login was not removed: token revocation failed (HTTP 500)"},
		{name: "global revocation fails", args: []string{"--revoke"}, input: "\x1b[B\r", interactive: true, target: "global", revokeStatus: http.StatusInternalServerError, wantError: "global login was not removed: token revocation failed (HTTP 500)"},
		{name: "cancel ctrl c", input: "\x03", interactive: true, wantError: "logout cancelled"},
		{name: "cancel escape", input: "\x1b", interactive: true, wantError: "logout cancelled"},
		{name: "cancel revocation", args: []string{"--revoke"}, input: "\x03", interactive: true, wantError: "logout cancelled"},
		{name: "outside repository", input: "\r", interactive: true, noGit: true, target: "global"},
		{name: "cancel outside repository", input: "\x03", interactive: true, noGit: true, wantError: "logout cancelled"},
		{name: "explicit repository", args: []string{"--repo"}, target: "repo"},
		{name: "explicit global", args: []string{"--global"}, target: "global"},
		{name: "explicit repository revocation", args: []string{"--repo", "--revoke"}, target: "repo"},
		{name: "explicit global revocation", args: []string{"--global", "--revoke"}, target: "global"},
		{name: "non-interactive", wantError: "specify --repo or --global in a non-interactive terminal"},
		{name: "conflicting scope", args: []string{"--repo", "--global"}, wantError: "--repo and --global cannot be used together"},
		{name: "unexpected argument", args: []string{"extra"}, wantError: "unexpected argument"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := loginTestRepo(t, loginExistingConfig)
			revocations := make(chan string, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/cli/oauth/revoke", r.URL.Path)
				revocations <- r.Header.Get("Authorization")
				status := tc.revokeStatus
				if status == 0 {
					status = http.StatusNoContent
				}
				w.WriteHeader(status)
			}))
			defer server.Close()
			t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
			if tc.noGit {
				require.NoError(t, os.RemoveAll(filepath.Join(dir, ".git")))
			}
			secrets := keyring.NewArrayKeyring(nil)
			store := cloudauth.Store{Path: filepath.Join(t.TempDir(), "cloud.yml"), Open: func() (cloudauth.SecretStore, error) { return secrets, nil }}
			require.NoError(t, store.Save(cloudauth.Credential{Token: "global-token", TokenID: "1", APIURL: server.URL}, nil))
			globalBefore, err := os.ReadFile(store.Path)
			require.NoError(t, err)
			deps := logoutDependencies{
				interactive: func() bool { return tc.interactive },
				globalStore: func() (cloudauth.Store, error) {
					require.Equal(t, "global", tc.target)
					return store, nil
				},
			}
			var output bytes.Buffer
			command := logoutCommand(deps)
			command.Reader = strings.NewReader(tc.input)
			command.Writer = &output
			err = command.Run(t.Context(), append([]string{"logout"}, tc.args...))
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				assert.NotContains(t, output.String(), "Token revoked")
				assert.NotContains(t, output.String(), "login removed")
			} else {
				require.NoError(t, err)
				if command.Bool("revoke") {
					assert.Contains(t, output.String(), "Token revoked in Bruin Cloud. Other copies of this token are now invalid.")
				} else {
					assert.NotContains(t, output.String(), "Token revoked")
				}
			}
			path := filepath.Join(dir, ".bruin.yml")
			if tc.target == "repo" && tc.wantError == "" {
				cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), path)
				require.NoError(t, err)
				assert.Empty(t, cm.CloudConnections())
				assert.Contains(t, output.String(), "Repository login removed")
			} else {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				assert.Equal(t, loginExistingConfig, string(data))
			}
			credential, err := store.Read(server.URL)
			require.NoError(t, err)
			keys, err := secrets.Keys()
			require.NoError(t, err)
			if tc.target == "global" && tc.wantError == "" {
				assert.Nil(t, credential)
				assert.Empty(t, keys)
				assert.Contains(t, output.String(), "Global login removed")
			} else {
				require.NotNil(t, credential)
				assert.Equal(t, "global-token", credential.Token)
				assert.Len(t, keys, 1)
				data, err := os.ReadFile(store.Path)
				require.NoError(t, err)
				assert.Equal(t, globalBefore, data)
			}
			assert.NotContains(t, output.String(), "original-token")
			assert.NotContains(t, output.String(), "global-token")
			if command.Bool("revoke") && tc.target != "" {
				require.Len(t, revocations, 1)
				token := "original-token"
				if tc.target == "global" {
					token = "global-token"
				}
				assert.Equal(t, "Bearer "+token, <-revocations)
			} else {
				assert.Empty(t, revocations)
			}
		})
	}
}

func TestLogoutRemovesRepositoryConnection(t *testing.T) { //nolint:paralleltest
	dir := loginTestRepo(t, loginExistingConfig)
	revocations := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/cli/oauth/revoke", r.URL.Path)
		revocations <- r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
	var output bytes.Buffer
	isDebug := false
	command := &cli.Command{Name: "bruin", Writer: &output, Commands: []*cli.Command{Cloud(&isDebug)}}
	require.NoError(t, command.Run(t.Context(), []string{"bruin", "cloud", "logout", "--repo", "--revoke"}))
	require.Len(t, revocations, 1)
	assert.Equal(t, "Bearer original-token", <-revocations)
	cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), filepath.Join(dir, ".bruin.yml"))
	require.NoError(t, err)
	assert.Empty(t, cm.CloudConnections())
	assert.Contains(t, output.String(), "Token revoked in Bruin Cloud")
	assert.Contains(t, output.String(), "Repository login removed")
	assert.NotContains(t, output.String(), "original-token")
}
