package cmd

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dataAppGuideResponse = `{"name":"data-apps","description":"Build data apps.","version":"abc123def456","content":"# Data Apps\n\nPull it first."}`

const dataAppResponse = `{"id":42,"title":"Sales","agent_id":7,"visibility":"team","updated_at":"2026-11-13T10:00:00Z","version":"v1",` +
	`"code":"<html>app</html>","queries":[{"name":"revenue","connection":"pg","sql":"select 1"}],"http_requests":[],` +
	`"guide":{"url":"http://cloud/api/v1/data-apps/guide","version":"g1"}}`

// dataAppRequest is what the fake API saw.
type dataAppRequest struct {
	Method string
	Path   string
	Body   map[string]any
}

// fakeDataAppAPI answers each "METHOD /path" from responses (status 200 unless given as
// "status|body") and records every request.
func fakeDataAppAPI(t *testing.T, responses map[string]string) *[]dataAppRequest {
	t.Helper()
	var seen []dataAppRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer user-key", r.Header.Get("Authorization"))
		req := dataAppRequest{Method: r.Method, Path: r.URL.Path}
		if data, _ := io.ReadAll(r.Body); len(data) > 0 {
			assert.NoError(t, json.Unmarshal(data, &req.Body))
		}
		seen = append(seen, req)

		resp, ok := responses[r.Method+" "+r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"message":"not found"}`)
			return
		}
		status := http.StatusOK
		if len(resp) > 4 && resp[3] == '|' {
			switch resp[:3] {
			case "201":
				status = http.StatusCreated
			case "409":
				status = http.StatusConflict
			case "422":
				status = http.StatusUnprocessableEntity
			}
			resp = resp[4:]
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, resp)
	}))
	t.Cleanup(server.Close)
	t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
	return &seen
}

func runDataApps(t *testing.T, args ...string) (string, error) {
	t.Helper()
	return runCloudCLI(t, append(append([]string{"data-apps"}, args...), "--api-key", "user-key")...)
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsGuidePrintsTheMarkdown(t *testing.T) {
	fakeDataAppAPI(t, map[string]string{"GET /data-apps/guide": dataAppGuideResponse})

	out, err := runDataApps(t, "guide", "--output", "plain")
	require.NoError(t, err)
	assert.Equal(t, "# Data Apps\n\nPull it first.\n", out)
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsGuideJSON(t *testing.T) {
	fakeDataAppAPI(t, map[string]string{"GET /data-apps/guide": dataAppGuideResponse})

	out, err := runDataApps(t, "guide", "--output", "json")
	require.NoError(t, err)

	var got map[string]string
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	assert.Equal(t, "abc123def456", got["version"])
	assert.Equal(t, "# Data Apps\n\nPull it first.", got["content"])
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsList(t *testing.T) {
	fakeDataAppAPI(t, map[string]string{
		"GET /data-apps": `{"data_apps":[{"id":42,"title":"Sales","agent_id":7,"visibility":"team","updated_at":"2026-11-13"},{"id":43,"title":"Draft","agent_id":null,"visibility":"private","updated_at":"2026-11-12"}]}`,
	})

	out, err := runDataApps(t, "list", "--output", "plain")
	require.NoError(t, err)
	assert.Contains(t, out, "Sales")
	assert.Contains(t, out, "Draft")
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsSettings(t *testing.T) {
	fakeDataAppAPI(t, map[string]string{
		"GET /data-apps/settings": `{"read_only":true,"restrict_credential_hosts":true,"allowed_hosts":["api.stripe.com"]}`,
	})

	out, err := runDataApps(t, "settings", "--output", "plain")
	require.NoError(t, err)
	assert.Contains(t, out, "Read-only warehouse queries: true")
	assert.Contains(t, out, "api.stripe.com")
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsCreateWithoutAnAgentWarns(t *testing.T) {
	seen := fakeDataAppAPI(t, map[string]string{
		"POST /data-apps": `201|{"id":44,"title":"Sales","agent_id":null,"visibility":"private","version":"v0","guide":{"url":"u","version":"g1"}}`,
	})

	out, err := runDataApps(t, "create", "--title", "Sales", "--output", "plain")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"title": "Sales"}, (*seen)[0].Body)
	assert.Contains(t, out, "Created data app 44")
	assert.Contains(t, out, "no agent yet")
	assert.Contains(t, out, "bruin cloud data-apps guide")
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsCreateSendsTheAgent(t *testing.T) {
	seen := fakeDataAppAPI(t, map[string]string{
		"POST /data-apps": `201|{"id":44,"title":"Sales","agent_id":7,"visibility":"team","version":"v0"}`,
	})

	_, err := runDataApps(t, "create", "--title", "Sales", "--agent-id", "7", "--visibility", "team", "--output", "plain")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"title": "Sales", "agent_id": float64(7), "visibility": "team"}, (*seen)[0].Body)
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsGetListsDeclaredNames(t *testing.T) {
	fakeDataAppAPI(t, map[string]string{"GET /data-apps/42": dataAppResponse})

	out, err := runDataApps(t, "get", "--data-app-id", "42", "--output", "plain")
	require.NoError(t, err)
	assert.Contains(t, out, "Version:    v1")
	assert.Contains(t, out, "Queries:    revenue")
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsPullWritesTheFilesAndVersion(t *testing.T) {
	fakeDataAppAPI(t, map[string]string{"GET /data-apps/42": dataAppResponse})
	dir := t.TempDir()

	_, err := runDataApps(t, "pull", "--data-app-id", "42", "--dir", dir, "--output", "plain")
	require.NoError(t, err)

	html, _ := os.ReadFile(filepath.Join(dir, "data-app.html"))
	assert.Equal(t, "<html>app</html>", string(html))
	queries, _ := os.ReadFile(filepath.Join(dir, "data-app.queries.json"))
	assert.Contains(t, string(queries), `"name": "revenue"`)
	state, _ := os.ReadFile(filepath.Join(dir, ".bruin-data-app.json"))
	assert.JSONEq(t, `{"id":42,"version":"v1"}`, string(state))
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsPullKeepsLocalChangesWithoutForce(t *testing.T) {
	fakeDataAppAPI(t, map[string]string{"GET /data-apps/42": dataAppResponse})
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data-app.html"), []byte("<html>my edit</html>"), 0o600))

	_, err := runDataApps(t, "pull", "--data-app-id", "42", "--dir", dir, "--output", "plain")
	require.Error(t, err)
	html, _ := os.ReadFile(filepath.Join(dir, "data-app.html"))
	assert.Equal(t, "<html>my edit</html>", string(html))

	_, err = runDataApps(t, "pull", "--data-app-id", "42", "--dir", dir, "--force", "--output", "plain")
	require.NoError(t, err)
	html, _ = os.ReadFile(filepath.Join(dir, "data-app.html"))
	assert.Equal(t, "<html>app</html>", string(html))
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsPushFromADirSendsThePulledVersion(t *testing.T) {
	seen := fakeDataAppAPI(t, map[string]string{
		"POST /data-apps/42": `{"id":42,"version":"v2","warnings":["This data app has no agent yet."]}`,
	})
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data-app.html"), []byte("<html>new</html>"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data-app.queries.json"), []byte(`[{"name":"q"}]`), 0o600))
	require.NoError(t, writeDataAppState(dir, dataAppState{ID: 42, Version: "v1"}))

	out, err := runDataApps(t, "push", "--data-app-id", "42", "--dir", dir, "--output", "plain")
	require.NoError(t, err)

	body := (*seen)[0].Body
	assert.Equal(t, "<html>new</html>", body["code"])
	assert.Equal(t, []any{map[string]any{"name": "q"}}, body["queries"])
	assert.NotContains(t, body, "http_requests", "a manifest file that isn't there is left as stored")
	assert.Equal(t, "v1", body["expected_version"])
	assert.Contains(t, out, "warning: This data app has no agent yet.")

	state, _ := readDataAppState(dir)
	assert.Equal(t, "v2", state.Version, "the next push builds on the version this one saved")
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsPushForceSkipsTheVersionCheck(t *testing.T) {
	seen := fakeDataAppAPI(t, map[string]string{"POST /data-apps/42": `{"id":42,"version":"v2"}`})
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data-app.html"), []byte("<html>new</html>"), 0o600))
	require.NoError(t, writeDataAppState(dir, dataAppState{ID: 42, Version: "v1"}))

	_, err := runDataApps(t, "push", "--data-app-id", "42", "--dir", dir, "--force", "--output", "plain")
	require.NoError(t, err)
	assert.NotContains(t, (*seen)[0].Body, "expected_version")
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsPushExplainsAConflict(t *testing.T) {
	fakeDataAppAPI(t, map[string]string{
		"POST /data-apps/42": `409|{"message":"This data app changed since that version.","error":"version_conflict","version":"v9"}`,
	})
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data-app.html"), []byte("<html>new</html>"), 0o600))
	require.NoError(t, writeDataAppState(dir, dataAppState{ID: 42, Version: "v1"}))

	out, err := runDataApps(t, "push", "--data-app-id", "42", "--dir", dir, "--output", "plain")
	require.Error(t, err)
	assert.Contains(t, out, "pull again")
	state, _ := readDataAppState(dir)
	assert.Equal(t, "v1", state.Version, "a refused push doesn't move the recorded version")
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsPushRejectsADirFromAnotherApp(t *testing.T) {
	seen := fakeDataAppAPI(t, map[string]string{})
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data-app.html"), []byte("<html>x</html>"), 0o600))
	require.NoError(t, writeDataAppState(dir, dataAppState{ID: 7, Version: "v1"}))

	_, err := runDataApps(t, "push", "--data-app-id", "42", "--dir", dir, "--output", "plain")
	require.Error(t, err)
	assert.Empty(t, *seen)
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsPushChecksManifestsAreArrays(t *testing.T) {
	seen := fakeDataAppAPI(t, map[string]string{})
	file := filepath.Join(t.TempDir(), "q.json")
	require.NoError(t, os.WriteFile(file, []byte(`{"name":"q"}`), 0o600))

	_, err := runDataApps(t, "push", "--data-app-id", "42", "--queries", file, "--output", "plain")
	require.Error(t, err)
	assert.Empty(t, *seen)
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsUpdate(t *testing.T) {
	seen := fakeDataAppAPI(t, map[string]string{"PATCH /data-apps/42": `{"id":42,"title":"Renamed","agent_id":8,"visibility":"team"}`})

	_, err := runDataApps(t, "update", "--data-app-id", "42", "--title", "Renamed", "--agent-id", "8", "--output", "plain")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"title": "Renamed", "agent_id": float64(8)}, (*seen)[0].Body)

	_, err = runDataApps(t, "update", "--data-app-id", "42", "--output", "plain")
	require.Error(t, err, "nothing to update")
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsDelete(t *testing.T) {
	seen := fakeDataAppAPI(t, map[string]string{"DELETE /data-apps/42": `{"success":true}`})

	out, err := runDataApps(t, "delete", "--data-app-id", "42", "--output", "plain")
	require.NoError(t, err)
	assert.Equal(t, "DELETE", (*seen)[0].Method)
	assert.Contains(t, out, "Deleted data app 42.")
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsPullIgnoresAReformattedManifest(t *testing.T) {
	fakeDataAppAPI(t, map[string]string{"GET /data-apps/42": dataAppResponse})
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data-app.queries.json"), []byte(`[{"sql":"select 1","name":"revenue","connection":"pg"}]`), 0o600))

	_, err := runDataApps(t, "pull", "--data-app-id", "42", "--dir", dir, "--output", "plain")
	require.NoError(t, err, "same queries, different formatting: not a local change")
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsPullComparesLargeNumbersExactly(t *testing.T) {
	fakeDataAppAPI(t, map[string]string{
		"GET /data-apps/42": `{"id":42,"version":"v1","code":"<html>app</html>","queries":[{"name":"q","limit":9007199254740992}],"http_requests":[]}`,
	})
	dir := t.TempDir()
	// Equal as float64, different as integers: a real local edit.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data-app.queries.json"), []byte(`[{"name":"q","limit":9007199254740993}]`), 0o600))

	_, err := runDataApps(t, "pull", "--data-app-id", "42", "--dir", dir, "--output", "plain")
	require.Error(t, err)
	queries, _ := os.ReadFile(filepath.Join(dir, "data-app.queries.json"))
	assert.Contains(t, string(queries), "9007199254740993")
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsPullStopsOnAnUnreadableFile(t *testing.T) {
	if runtime.GOOS == osWindows {
		t.Skip("file modes don't make a file unreadable on Windows")
	}
	fakeDataAppAPI(t, map[string]string{"GET /data-apps/42": dataAppResponse})
	dir := t.TempDir()
	path := filepath.Join(dir, "data-app.html")
	require.NoError(t, os.WriteFile(path, []byte("<html>my edit</html>"), 0o200))

	_, err := runDataApps(t, "pull", "--data-app-id", "42", "--dir", dir, "--output", "plain")
	require.Error(t, err)
	require.NoError(t, os.Chmod(path, 0o600))
	html, _ := os.ReadFile(path)
	assert.Equal(t, "<html>my edit</html>", string(html))
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDataAppsPushRejectsANullManifest(t *testing.T) {
	seen := fakeDataAppAPI(t, map[string]string{})
	file := filepath.Join(t.TempDir(), "q.json")
	require.NoError(t, os.WriteFile(file, []byte("null"), 0o600))

	_, err := runDataApps(t, "push", "--data-app-id", "42", "--queries", file, "--output", "plain")
	require.Error(t, err)
	assert.Empty(t, *seen)
}
