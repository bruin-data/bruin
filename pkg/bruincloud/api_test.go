package bruincloud

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) *APIClient {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := NewAPIClient("test-api-key")
	client.baseURL = server.URL
	return client
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("failed to marshal test response: %v", err)
	}
	_, _ = w.Write(data)
}

func readJSON(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode request body: %v", err)
	}
	return body
}

func TestDoRequest_AuthHeader(t *testing.T) {
	t.Parallel()
	var gotAuth string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("[]"))
	})

	_, err := client.ListProjects(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "Bearer test-api-key", gotAuth)
}

func TestDoRequest_TeamHeader(t *testing.T) {
	t.Parallel()

	t.Run("sends X-Bruin-Team when set", func(t *testing.T) {
		t.Parallel()
		var got string
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Get("X-Bruin-Team")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("[]"))
		})
		client.SetTeam("acme")

		_, err := client.ListProjects(t.Context())
		require.NoError(t, err)
		assert.Equal(t, "acme", got)
	})

	t.Run("omits the header when unset", func(t *testing.T) {
		t.Parallel()
		var present bool
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			_, present = r.Header["X-Bruin-Team"]
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("[]"))
		})

		_, err := client.ListProjects(t.Context())
		require.NoError(t, err)
		assert.False(t, present)
	})
}

func TestDoRequest_ErrorParsing(t *testing.T) {
	t.Parallel()

	t.Run("401 error", func(t *testing.T) {
		t.Parallel()
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"Unauthenticated."}`))
		})

		_, err := client.ListProjects(t.Context())
		require.Error(t, err)
		var apiErr *APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, 401, apiErr.StatusCode)
		assert.Equal(t, "Unauthenticated.", apiErr.Message)
	})

	t.Run("422 validation error", func(t *testing.T) {
		t.Parallel()
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"message":"The given data was invalid.","errors":{"project":["The project field is required."]}}`))
		})

		_, err := client.ListProjects(t.Context())
		require.Error(t, err)
		var apiErr *APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, 422, apiErr.StatusCode)
		assert.Contains(t, apiErr.Error(), "project")
	})

	t.Run("500 error", func(t *testing.T) {
		t.Parallel()
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`Internal Server Error`))
		})

		_, err := client.ListProjects(t.Context())
		require.Error(t, err)
		var apiErr *APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, 500, apiErr.StatusCode)
	})

	t.Run("malformed JSON", func(t *testing.T) {
		t.Parallel()
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{malformed`))
		})

		_, err := client.ListProjects(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse response")
	})
}

func TestDoRequest_RetriesOn429(t *testing.T) {
	t.Parallel()

	t.Run("recovers after 429", func(t *testing.T) {
		t.Parallel()
		var attempts atomic.Int32
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			n := attempts.Add(1)
			if n < 3 {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("[]"))
		})

		_, err := client.ListProjects(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int32(3), attempts.Load())
	})

	t.Run("returns 429 error after exhausting retries", func(t *testing.T) {
		t.Parallel()
		var attempts atomic.Int32
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"message":"rate limited"}`))
		})

		_, err := client.ListProjects(t.Context())
		require.Error(t, err)
		var apiErr *APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, http.StatusTooManyRequests, apiErr.StatusCode)
		// NewAPIClient configures RetryMax=3, so 1 initial + 3 retries = 4 attempts.
		assert.Equal(t, int32(defaultRetryMax+1), attempts.Load())
	})

	t.Run("does not retry on 500", func(t *testing.T) {
		t.Parallel()
		var attempts atomic.Int32
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		})

		_, err := client.ListProjects(t.Context())
		require.Error(t, err)
		assert.Equal(t, int32(1), attempts.Load())
	})
}

func TestListProjects(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/projects", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, []Project{
			{ID: "1", Name: "test-project", Repo: Repo{URL: "https://github.com/test/repo", Branch: "main"}},
		})
	})

	projects, err := client.ListProjects(t.Context())
	require.NoError(t, err)
	require.Len(t, projects, 1)
	assert.Equal(t, "test-project", projects[0].Name)
	assert.Equal(t, "main", projects[0].Repo.Branch)
}

func TestListPipelines(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/pipelines", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		schedule := "0 * * * *"
		writeJSON(t, w, []Pipeline{
			{Name: "test-pipeline", Project: "proj", Schedule: &schedule, StartDate: "2026-01-01"},
		})
	})

	pipelines, err := client.ListPipelines(t.Context())
	require.NoError(t, err)
	require.Len(t, pipelines, 1)
	assert.Equal(t, "test-pipeline", pipelines[0].Name)
}

func TestGetPipeline(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/pipeline", r.URL.Path)
		assert.Equal(t, "my-project", r.URL.Query().Get("project"))
		assert.Equal(t, "my-pipeline", r.URL.Query().Get("name"))
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"data": Pipeline{Name: "my-pipeline", Project: "my-project", StartDate: "2026-01-01"},
		})
	})

	p, err := client.GetPipeline(t.Context(), "my-project", "my-pipeline")
	require.NoError(t, err)
	assert.Equal(t, "my-pipeline", p.Name)
}

func TestDeletePipeline(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "my-project", r.URL.Query().Get("project"))
		assert.Equal(t, "my-pipeline", r.URL.Query().Get("pipeline"))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`200`))
	})

	err := client.DeletePipeline(t.Context(), "my-project", "my-pipeline")
	require.NoError(t, err)
}

func TestEnableDisablePipeline(t *testing.T) {
	t.Parallel()

	t.Run("enable", func(t *testing.T) {
		t.Parallel()
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/enable-pipelines", r.URL.Path)
			assert.Equal(t, http.MethodPost, r.Method)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`200`))
		})
		err := client.EnablePipeline(t.Context(), "proj", "pipe")
		require.NoError(t, err)
	})

	t.Run("disable", func(t *testing.T) {
		t.Parallel()
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/disable-pipelines", r.URL.Path)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`200`))
		})
		err := client.DisablePipeline(t.Context(), "proj", "pipe")
		require.NoError(t, err)
	})
}

func TestListRuns(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/pipeline-runs", r.URL.Path)
		assert.Equal(t, "proj", r.URL.Query().Get("project"))
		assert.Equal(t, "pipe", r.URL.Query().Get("name"))
		assert.Equal(t, "10", r.URL.Query().Get("limit"))
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, []PipelineRun{
			{Project: "proj", Pipeline: "pipe", RunID: "run-1"},
		})
	})

	runs, err := client.ListRuns(t.Context(), "proj", "pipe", 10, 0)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, "run-1", runs[0].RunID)
}

func TestGetRun(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/pipeline-run", r.URL.Path)
		assert.Equal(t, "run-1", r.URL.Query().Get("run_id"))
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"data": PipelineRun{Project: "proj", Pipeline: "pipe", RunID: "run-1"},
		})
	})

	run, err := client.GetRun(t.Context(), "proj", "pipe", "run-1")
	require.NoError(t, err)
	assert.Equal(t, "run-1", run.RunID)
}

func TestTriggerRun(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/trigger-pipeline-run", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		body := readJSON(t, r)
		assert.Equal(t, "proj", body["project"])
		assert.Equal(t, "pipe", body["pipeline"])
		assert.Equal(t, "2026-01-01", body["start_date"])
		assert.Equal(t, "2026-01-02", body["end_date"])
		// A plain run omits split/asset fields entirely.
		assert.NotContains(t, body, "split")
		assert.NotContains(t, body, "assets")

		w.WriteHeader(http.StatusCreated)
		writeJSON(t, w, TriggerRunResult{
			Message:  "Run triggered successfully",
			Project:  "proj",
			Pipeline: "pipe",
			RunID:    "run-123",
		})
	})

	result, err := client.TriggerRun(t.Context(), "proj", "pipe", "2026-01-01", "2026-01-02", TriggerRunOptions{})
	require.NoError(t, err)
	assert.Equal(t, "run-123", result.RunID)
	assert.Equal(t, "proj", result.Project)
	assert.Equal(t, "pipe", result.Pipeline)
}

func TestTriggerRunWithSplit(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/trigger-pipeline-run", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		body := readJSON(t, r)
		assert.Equal(t, "proj", body["project"])
		assert.Equal(t, "pipe", body["pipeline"])
		assert.Equal(t, "2026-01-01", body["start_date"])
		assert.Equal(t, "2026-04-01", body["end_date"])
		assert.Equal(t, "month", body["split"])
		assert.EqualValues(t, 2, body["chunk_size"])

		w.WriteHeader(http.StatusCreated)
		writeJSON(t, w, TriggerRunResult{
			Message:          "Backfill triggered successfully",
			Project:          "proj",
			Pipeline:         "pipe",
			MultipleActionID: "backfill-123",
			Split:            "month",
			ChunkSize:        2,
			URL:              "https://cloud.getbruin.com/acme/projects/proj/pipelines/pipe/backfills/backfill-123",
		})
	})

	result, err := client.TriggerRun(t.Context(), "proj", "pipe", "2026-01-01", "2026-04-01", TriggerRunOptions{Split: "month", ChunkSize: 2})
	require.NoError(t, err)
	assert.Equal(t, "backfill-123", result.MultipleActionID)
	assert.Equal(t, "month", result.Split)
	assert.Equal(t, 2, result.ChunkSize)
	assert.Contains(t, result.URL, "/backfills/backfill-123")
}

func TestTriggerRunSplitDefaultsChunkSize(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body := readJSON(t, r)
		assert.Equal(t, "month", body["split"])
		// Split set with the zero-value ChunkSize must still send a valid chunk size.
		assert.EqualValues(t, 1, body["chunk_size"])

		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"multiple_action_id":"m1"}`))
	})

	_, err := client.TriggerRun(t.Context(), "proj", "pipe", "2026-01-01", "2026-04-01", TriggerRunOptions{Split: "month"})
	require.NoError(t, err)
}

func TestTriggerRunWithOptions(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/trigger-pipeline-run", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		body := readJSON(t, r)
		assert.Equal(t, []any{"raw_events"}, body["assets"])
		assert.Equal(t, true, body["full_refresh"])
		vars := body["variables"].(map[string]any)
		assert.Equal(t, "bar", vars["foo"])

		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"run_id":"run-1"}`))
	})

	opts := TriggerRunOptions{
		Assets:      []string{"raw_events"},
		FullRefresh: true,
		Variables:   map[string]any{"foo": "bar"},
	}
	_, err := client.TriggerRun(t.Context(), "proj", "pipe", "2026-01-01", "2026-01-31", opts)
	require.NoError(t, err)
}

func TestTriggerRunWithDownstream(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/trigger-pipeline-run", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		body := readJSON(t, r)
		assert.Equal(t, []any{"raw_events"}, body["assets"])
		assert.Equal(t, true, body["downstream"])

		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"run_id":"run-1"}`))
	})

	opts := TriggerRunOptions{
		Assets:     []string{"raw_events"},
		Downstream: true,
	}
	_, err := client.TriggerRun(t.Context(), "proj", "pipe", "2026-01-01", "2026-01-31", opts)
	require.NoError(t, err)
}

func TestRerunRun(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/rerun-pipeline-runs", r.URL.Path)

		body := readJSON(t, r)
		assert.Equal(t, true, body["only_failed"])

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`200`))
	})

	err := client.RerunRun(t.Context(), "proj", "pipe", "run-1", true)
	require.NoError(t, err)
}

func TestMarkRunStatus(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/mark-pipeline-runs-status", r.URL.Path)

		body := readJSON(t, r)
		assert.Equal(t, "success", body["status"])

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`200`))
	})

	err := client.MarkRunStatus(t.Context(), "proj", "pipe", "run-1", "success")
	require.NoError(t, err)
}

func TestListAssets(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/pipeline-assets", r.URL.Path)
		assert.Equal(t, "proj", r.URL.Query().Get("project"))
		assert.Equal(t, "pipe", r.URL.Query().Get("name"))
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, []Asset{
			{Project: "proj", Pipeline: "pipe", ID: "1", Name: "my_asset", Type: "bq.sql"},
		})
	})

	assets, err := client.ListAssets(t.Context(), "proj", "pipe")
	require.NoError(t, err)
	require.Len(t, assets, 1)
	assert.Equal(t, "my_asset", assets[0].Name)
}

func TestGetAsset(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/asset", r.URL.Path)
		assert.Equal(t, "proj", r.URL.Query().Get("project"))
		assert.Equal(t, "pipe", r.URL.Query().Get("pipeline"))
		assert.Equal(t, "my_asset", r.URL.Query().Get("asset"))
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, Asset{Project: "proj", Pipeline: "pipe", ID: "1", Name: "my_asset", Type: "bq.sql"})
	})

	a, err := client.GetAsset(t.Context(), "proj", "pipe", "my_asset")
	require.NoError(t, err)
	assert.Equal(t, "my_asset", a.Name)
}

func TestListAgents(t *testing.T) {
	t.Parallel()
	desc := "A test agent"
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/agents", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"agents": []Agent{
				{ID: 1, Name: "test-agent", Description: &desc},
			},
		})
	})

	agents, err := client.ListAgents(t.Context())
	require.NoError(t, err)
	require.Len(t, agents, 1)
	assert.Equal(t, "test-agent", agents[0].Name)
}

func TestListAgentConnections(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/agents/7/connections", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"connections": []AgentConnection{
				{Name: "warehouse_prod", Type: "snowflake"},
				{Name: "djamila-dev", Type: "google_cloud_platform"},
			},
		})
	})

	connections, err := client.ListAgentConnections(t.Context(), 7)
	require.NoError(t, err)
	require.Len(t, connections, 2)
	assert.Equal(t, "warehouse_prod", connections[0].Name)
	assert.Equal(t, "google_cloud_platform", connections[1].Type)
}

func TestAddAgentConnection(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/agents/7/connections", r.URL.Path)

		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "warehouse", body["name"])
		assert.Equal(t, "postgres", body["type"])
		cfg, _ := body["config"].(map[string]any)
		assert.Equal(t, "secret", cfg["password"])

		w.WriteHeader(http.StatusCreated)
		writeJSON(t, w, map[string]any{
			"connections": []AgentConnection{{Name: "warehouse", Type: "postgres"}},
		})
	})

	connections, err := client.AddAgentConnection(t.Context(), 7, "postgres", "warehouse", map[string]any{"password": "secret"})
	require.NoError(t, err)
	require.Len(t, connections, 1)
	assert.Equal(t, "warehouse", connections[0].Name)
	assert.Equal(t, "postgres", connections[0].Type)
}

func TestListConnectionSets(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/connection-sets", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"connection_sets": []ConnectionSet{{ID: 1, Name: "prod"}, {ID: 2, Name: "dev"}},
		})
	})

	sets, err := client.ListConnectionSets(t.Context())
	require.NoError(t, err)
	require.Len(t, sets, 2)
	assert.Equal(t, "prod", sets[0].Name)
	assert.Equal(t, 2, sets[1].ID)
}

func TestListConnectionSetConnections(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/connection-sets/5/connections", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"connections": []Connection{{Name: "pg", Type: "postgres"}},
		})
	})

	conns, err := client.ListConnectionSetConnections(t.Context(), 5)
	require.NoError(t, err)
	require.Len(t, conns, 1)
	assert.Equal(t, "pg", conns[0].Name)
	assert.Equal(t, "postgres", conns[0].Type)
}

func TestCreateConnectionSet(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/connection-sets", r.URL.Path)

		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "prod", body["name"])
		assert.Equal(t, true, body["skip_validation"])
		conns, _ := body["connections"].([]any)
		assert.Len(t, conns, 1)
		first, _ := conns[0].(map[string]any)
		assert.Equal(t, "postgres", first["type"])
		cfg, _ := first["config"].(map[string]any)
		assert.Equal(t, "secret", cfg["password"])

		w.WriteHeader(http.StatusCreated)
		writeJSON(t, w, ConnectionSet{ID: 9, Name: "prod"})
	})

	set, err := client.CreateConnectionSet(t.Context(), "prod", []ConnectionSetInput{
		{Type: "postgres", Name: "pg", Config: map[string]any{"password": "secret"}},
	}, true)
	require.NoError(t, err)
	assert.Equal(t, 9, set.ID)
	assert.Equal(t, "prod", set.Name)
}

func TestUpdateConnectionSet(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/connection-sets/9", r.URL.Path)

		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		conns, _ := body["connections"].([]any)
		assert.Len(t, conns, 1)

		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{"success": true})
	})

	err := client.UpdateConnectionSet(t.Context(), 9, []ConnectionSetInput{
		{Type: "postgres", Name: "pg", Config: map[string]any{"password": "secret"}},
	}, false)
	require.NoError(t, err)
}

func TestDeleteConnectionSet(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/connection-sets/9", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{"success": true})
	})

	require.NoError(t, client.DeleteConnectionSet(t.Context(), 9))
}

func TestListSkills(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/skills", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{"skills": []Skill{{ID: 1, Name: "reporting", AgentIDs: []int{5}}}})
	})

	skills, err := client.ListSkills(t.Context())
	require.NoError(t, err)
	require.Len(t, skills, 1)
	assert.Equal(t, "reporting", skills[0].Name)
	assert.Equal(t, []int{5}, skills[0].AgentIDs)
}

func TestCreateSkill(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/skills", r.URL.Path)
		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "sql_helper", body["name"])
		w.WriteHeader(http.StatusCreated)
		writeJSON(t, w, map[string]any{"skill": Skill{ID: 3, Name: "sql_helper"}})
	})

	skill, err := client.CreateSkill(t.Context(), map[string]any{"name": "sql_helper", "description": "d", "body": "b"})
	require.NoError(t, err)
	assert.Equal(t, 3, skill.ID)
}

func TestUpdateSkill(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)
		assert.Equal(t, "/skills/3", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{"skill": Skill{ID: 3, Name: "renamed"}})
	})

	skill, err := client.UpdateSkill(t.Context(), 3, map[string]any{"name": "renamed", "description": "d", "body": "b"})
	require.NoError(t, err)
	assert.Equal(t, "renamed", skill.Name)
}

func TestDeleteSkill(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/skills/3", r.URL.Path)
		writeJSON(t, w, map[string]bool{"deleted": true})
	})

	require.NoError(t, client.DeleteSkill(t.Context(), 3))
}

func TestSetSkillAgents(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/skills/3/agents", r.URL.Path)
		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, []any{float64(5), float64(7)}, body["agent_ids"])
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{"agent_ids": []int{5, 7}})
	})

	ids, err := client.SetSkillAgents(t.Context(), 3, []int{5, 7})
	require.NoError(t, err)
	assert.Equal(t, []int{5, 7}, ids)
}

func TestCreateAgent(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/agents", r.URL.Path)

		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "new-agent", body["name"])
		assert.Equal(t, "private", body["visibility"])
		// empty optional fields are omitted so the server applies its defaults
		_, hasDesc := body["description"]
		assert.False(t, hasDesc)

		w.WriteHeader(http.StatusCreated)
		writeJSON(t, w, Agent{ID: 7, Name: "new-agent", Visibility: "private"})
	})

	agent, err := client.CreateAgent(t.Context(), "new-agent", "", "", "private")
	require.NoError(t, err)
	assert.Equal(t, 7, agent.ID)
	assert.Equal(t, "private", agent.Visibility)
}

func TestGetAgent(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/agents/7", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, Agent{ID: 7, Name: "an-agent", Visibility: "team"})
	})

	agent, err := client.GetAgent(t.Context(), 7)
	require.NoError(t, err)
	assert.Equal(t, "an-agent", agent.Name)
}

func TestUpdateAgent(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)
		assert.Equal(t, "/agents/7", r.URL.Path)

		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "renamed", body["name"])
		// empty fields are omitted so the server leaves them unchanged
		_, hasVisibility := body["visibility"]
		assert.False(t, hasVisibility)

		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, Agent{ID: 7, Name: "renamed", Visibility: "team"})
	})

	agent, err := client.UpdateAgent(t.Context(), 7, map[string]any{"name": "renamed"})
	require.NoError(t, err)
	assert.Equal(t, "renamed", agent.Name)
}

func TestDeleteAgent(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/agents/7", r.URL.Path)
		writeJSON(t, w, map[string]bool{"success": true})
	})

	require.NoError(t, client.DeleteAgent(t.Context(), 7))
}

func TestGetAgentMemory(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/agents/7/memory", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		mem := "remembered thing"
		writeJSON(t, w, AgentMemory{ID: 7, Name: "an-agent", Memory: &mem})
	})

	memory, err := client.GetAgentMemory(t.Context(), 7)
	require.NoError(t, err)
	require.NotNil(t, memory.Memory)
	assert.Equal(t, "remembered thing", *memory.Memory)
}

func TestSetAgentMemory(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/agents/7/memory", r.URL.Path)

		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "new memory", body["memory"])

		w.WriteHeader(http.StatusOK)
		mem := "new memory"
		writeJSON(t, w, AgentMemory{ID: 7, Name: "an-agent", Memory: &mem})
	})

	mem := "new memory"
	memory, err := client.SetAgentMemory(t.Context(), 7, &mem)
	require.NoError(t, err)
	require.NotNil(t, memory.Memory)
	assert.Equal(t, "new memory", *memory.Memory)
}

func TestSetAgentMemoryClear(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/agents/7/memory", r.URL.Path)

		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		// A nil memory clears it: the field is present and JSON null.
		val, present := body["memory"]
		assert.True(t, present)
		assert.Nil(t, val)

		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, AgentMemory{ID: 7, Name: "an-agent", Memory: nil})
	})

	memory, err := client.SetAgentMemory(t.Context(), 7, nil)
	require.NoError(t, err)
	assert.Nil(t, memory.Memory)
}

func TestExportThread(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/agents/7/threads/42/export", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"export_version": "1.0",
			"thread":         map[string]any{"id": 42},
			"messages":       []any{},
		})
	})

	export, err := client.ExportThread(t.Context(), 7, 42)
	require.NoError(t, err)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(export, &parsed))
	assert.Equal(t, "1.0", parsed["export_version"])
}

func TestListAgentMcpServers(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/agents/7/mcp-servers", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"mcp_integrations": []AgentMcpServer{
				{Kind: "linear", ConnectionName: "my-linear"},
			},
			"mcp_kinds":               map[string]string{"linear": "Linear", "github": "GitHub"},
			"connections_by_mcp_kind": map[string][]string{"linear": {"my-linear"}, "github": {}},
		})
	})

	resp, err := client.ListAgentMcpServers(t.Context(), 7)
	require.NoError(t, err)
	require.Len(t, resp.MCPIntegrations, 1)
	assert.Equal(t, "linear", resp.MCPIntegrations[0].Kind)
	assert.Equal(t, "Linear", resp.MCPKinds["linear"])
	assert.Equal(t, []string{"my-linear"}, resp.ConnectionsByMcpKind["linear"])
}

func TestSetAgentMcpServers(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)
		assert.Equal(t, "/agents/7", r.URL.Path)

		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		integrations, ok := body["mcp_integrations"].([]any)
		assert.True(t, ok)
		assert.Len(t, integrations, 1)

		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, Agent{ID: 7, Name: "a", Visibility: "team"})
	})

	agent, err := client.SetAgentMcpServers(t.Context(), 7, []AgentMcpServer{
		{Kind: "linear", ConnectionName: "my-linear"},
	})
	require.NoError(t, err)
	assert.Equal(t, 7, agent.ID)
}

func TestListAgentThreads(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/agents/1/threads", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"threads": []AgentThread{
				{ID: 10, AgentID: 1, CreatedAt: "2026-01-01", UpdatedAt: "2026-01-02"},
			},
		})
	})

	threads, err := client.ListAgentThreads(t.Context(), 1, 0, 0, false)
	require.NoError(t, err)
	require.Len(t, threads, 1)
	assert.Equal(t, 10, threads[0].ID)
}

func TestListAgentThreadsArchived(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/agents/1/threads", r.URL.Path)
		assert.Equal(t, "true", r.URL.Query().Get("archived"))
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{"threads": []AgentThread{}})
	})

	_, err := client.ListAgentThreads(t.Context(), 1, 0, 0, true)
	require.NoError(t, err)
}

func TestUpdateAgentThread(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)
		assert.Equal(t, "/agents/1/threads/10", r.URL.Path)

		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "Renamed", body["title"])

		w.WriteHeader(http.StatusOK)
		title := "Renamed"
		writeJSON(t, w, AgentThread{ID: 10, AgentID: 1, Title: &title})
	})

	thread, err := client.UpdateAgentThread(t.Context(), 1, 10, map[string]any{"title": "Renamed"})
	require.NoError(t, err)
	assert.Equal(t, "Renamed", *thread.Title)
}

func TestDeleteAgentThread(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/agents/1/threads/10", r.URL.Path)
		writeJSON(t, w, map[string]bool{"success": true})
	})

	require.NoError(t, client.DeleteAgentThread(t.Context(), 1, 10))
}

func TestListAgentMessages(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/agents/1/threads/10/messages", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"messages": []AgentMessage{
				{ID: 100, Status: "completed", CreatedAt: "2026-01-01", UpdatedAt: "2026-01-02"},
			},
		})
	})

	messages, err := client.ListAgentMessages(t.Context(), 1, 10, 0, 0)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	assert.Equal(t, "completed", messages[0].Status)
}

func TestGetAgentMessageStatus(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/agents/1/threads/10/messages/100", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"data": AgentMessage{ID: 100, Status: "completed", CreatedAt: "2026-01-01", UpdatedAt: "2026-01-02"},
		})
	})

	msg, err := client.GetAgentMessageStatus(t.Context(), 1, 10, 100)
	require.NoError(t, err)
	assert.Equal(t, "completed", msg.Status)
}

func TestListInstances(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/asset-instances-for-run", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"name":"instance1"}]`))
	})

	result, err := client.ListInstances(t.Context(), "proj", "pipe", "run-1")
	require.NoError(t, err)
	assert.Contains(t, string(result), "instance1")
}

func TestGetInstance(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/asset-instance-details", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"name":"instance1","status":"success"}`))
	})

	result, err := client.GetInstance(t.Context(), "proj", "pipe", "run-1", "my_asset")
	require.NoError(t, err)
	assert.Contains(t, string(result), "instance1")
}

func TestGetInstanceLogs(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/asset-instance-logs", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"log":"hello"}]`))
	})

	result, err := client.GetInstanceLogs(t.Context(), "proj", "pipe", "run-1", "step-1", 1)
	require.NoError(t, err)
	assert.Contains(t, string(result), "hello")
}

func TestListGlossaryEntities(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/glossary-entities", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"name":"entity1"}]`))
	})

	result, err := client.ListGlossaryEntities(t.Context())
	require.NoError(t, err)
	assert.Contains(t, string(result), "entity1")
}

func TestGetGlossaryEntity(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/glossary-entity-details", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"name":"entity1","description":"test"}`))
	})

	result, err := client.GetGlossaryEntity(t.Context(), "proj", "entity1")
	require.NoError(t, err)
	assert.Contains(t, string(result), "entity1")
}

func TestGetPipelineErrors(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/pipeline-validation-errors", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"error":"something wrong"}]`))
	})

	errors, err := client.GetPipelineErrors(t.Context())
	require.NoError(t, err)
	require.Len(t, errors, 1)
}

func TestSendAgentMessage(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/agents/1/messages", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		body := readJSON(t, r)
		assert.Equal(t, "hello", body["message"])

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"thread_id":10,"message_id":100}`))
	})

	result, err := client.SendAgentMessage(t.Context(), 1, "hello", nil)
	require.NoError(t, err)
	assert.Contains(t, string(result), "thread_id")
}

func TestSendAgentMessage_WithThreadID(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body := readJSON(t, r)
		assert.Equal(t, "hello", body["message"])
		assert.InDelta(t, float64(10), body["thread_id"], 0.01)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"thread_id":10,"message_id":101}`))
	})

	threadID := 10
	result, err := client.SendAgentMessage(t.Context(), 1, "hello", &threadID)
	require.NoError(t, err)
	assert.Contains(t, string(result), "message_id")
}

func TestEncodeRunNote(t *testing.T) {
	t.Parallel()
	assert.Empty(t, encodeRunNote("", nil))
	assert.JSONEq(t, `{"note":"hi","tags":[]}`, encodeRunNote("hi", nil))
	assert.JSONEq(t, `{"note":"","tags":["a","b"]}`, encodeRunNote("", []string{"a", "b"}))
	assert.JSONEq(t, `{"note":"hi","tags":["a"]}`, encodeRunNote("hi", []string{"a"}))
}

func TestTriggerRunWithTags(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body := readJSON(t, r)
		// Note + tags are carried together inside the note field as JSON (the Cloud RunNote format).
		assert.JSONEq(t, `{"note":"Q1 backfill","tags":["nightly","manual"]}`, body["note"].(string))
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"message":"Run triggered successfully","run_id":"run-123"}`))
	})

	result, err := client.TriggerRun(t.Context(), "p", "pipe", "2026-01-01", "2026-01-02",
		TriggerRunOptions{Note: "Q1 backfill", Tags: []string{"nightly", "manual"}})
	require.NoError(t, err)
	assert.Equal(t, "run-123", result.RunID)
}

func TestTriggerRunBackfillReturnsURL(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body := readJSON(t, r)
		assert.Equal(t, "day", body["split"])
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"message":"Backfill triggered successfully","multiple_action_id":"abc","split":"day","chunk_size":1,"url":"https://cloud.getbruin.com/acme/projects/p/pipelines/pipe/backfills/abc"}`))
	})

	result, err := client.TriggerRun(t.Context(), "p", "pipe", "2026-01-01", "2026-01-03",
		TriggerRunOptions{Split: "day", ChunkSize: 1})
	require.NoError(t, err)
	assert.Equal(t, "https://cloud.getbruin.com/acme/projects/p/pipelines/pipe/backfills/abc", result.URL)
	assert.Equal(t, "abc", result.MultipleActionID)
}

func TestListBackfills(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/backfills", r.URL.Path)
		assert.Equal(t, "proj", r.URL.Query().Get("project"))
		assert.Equal(t, "pipe", r.URL.Query().Get("pipeline"))
		assert.Equal(t, "5", r.URL.Query().Get("limit"))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"id":"m1","project":"proj","pipeline":"pipe","interval_start":"2026-01-01T00:00:00.000Z","interval_end":"2026-01-03T00:00:00.000Z","created_at":"2026-01-01T00:00:00.000Z","runs":[{"run_id":"m1__a"},{"run_id":"m1__b"}]}]`))
	})

	backfills, err := client.ListBackfills(t.Context(), "proj", "pipe", 5)
	require.NoError(t, err)
	require.Len(t, backfills, 1)
	assert.Equal(t, "m1", backfills[0].ID)
	assert.Len(t, backfills[0].Runs, 2)
}

func TestGetBackfillRuns(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/backfills/m1/runs", r.URL.Path)
		assert.Equal(t, "30", r.URL.Query().Get("limit"))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"project":"proj","pipeline":"pipe","run_id":"m1__a","interval_start":"2026-01-01T00:00:00.000Z","interval_end":"2026-01-02T00:00:00.000Z","created_at":"2026-01-01T00:00:00.000Z","note":null}]`))
	})

	runs, err := client.GetBackfillRuns(t.Context(), "m1", 30, 0)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, "m1__a", runs[0].RunID)
}

func TestListTeams(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/teams", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"teams": []map[string]any{
				{"id": 1, "name": "Acme", "company_prefix": "acme"},
				{"id": 2, "name": "Globex", "company_prefix": "globex"},
			},
		})
	})

	teams, err := client.ListTeams(t.Context())
	require.NoError(t, err)
	require.Len(t, teams, 2)
	assert.Equal(t, "Acme", teams[0].Name)
	assert.Equal(t, "acme", teams[0].CompanyPrefix)
	assert.Equal(t, 2, teams[1].ID)
}

func TestListDashboards(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/dashboards", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		// Match the API's snake_case shape exactly, so the tag mapping is covered.
		writeJSON(t, w, map[string]any{
			"dashboards": []map[string]any{
				{"id": 3, "title": "Revenue", "visibility": "team", "updated_at": "2026-07-06T10:00:00+00:00"},
			},
		})
	})

	dashboards, err := client.ListDashboards(t.Context())
	require.NoError(t, err)
	require.Len(t, dashboards, 1)
	assert.Equal(t, "Revenue", *dashboards[0].Title)
	require.NotNil(t, dashboards[0].UpdatedAt)
	assert.Equal(t, "2026-07-06T10:00:00+00:00", *dashboards[0].UpdatedAt)
}

func TestGetDashboard(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/dashboards/3", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"id":         3,
			"title":      "Revenue",
			"visibility": "team",
			"state":      map[string]any{"widgets": []any{}},
		})
	})

	dashboard, err := client.GetDashboard(t.Context(), 3)
	require.NoError(t, err)
	assert.Equal(t, 3, dashboard.ID)
	assert.JSONEq(t, `{"widgets":[]}`, string(dashboard.State))
}

func TestCreateDashboard(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/dashboards", r.URL.Path)

		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "New Dash", body["title"])
		assert.Equal(t, "private", body["visibility"])
		assert.EqualValues(t, 7, body["agent_id"])
		assert.NotNil(t, body["state"])

		w.WriteHeader(http.StatusCreated)
		title := "New Dash"
		boundAgent := 7
		writeJSON(t, w, Dashboard{ID: 9, Title: &title, Visibility: "private", URL: "https://cloud.getbruin.com/acme/dashboards/9?mode=edit", AgentID: &boundAgent})
	})

	dashboard, err := client.CreateDashboard(t.Context(), "New Dash", "private", 7, map[string]any{"widgets": []any{}})
	require.NoError(t, err)
	assert.Equal(t, 9, dashboard.ID)
	assert.Equal(t, "https://cloud.getbruin.com/acme/dashboards/9?mode=edit", dashboard.URL)
	// The bound agent comes back on the response so the CLI can warn when none resolved.
	require.NotNil(t, dashboard.AgentID)
	assert.Equal(t, 7, *dashboard.AgentID)
}

func TestCreateDashboardOmitsAgentIDWhenZero(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		// No agent id given, so the field is omitted and the server applies its
		// token-agent fallback rather than clearing the binding.
		_, hasAgentID := body["agent_id"]
		assert.False(t, hasAgentID)

		w.WriteHeader(http.StatusCreated)
		title := "New Dash"
		writeJSON(t, w, Dashboard{ID: 9, Title: &title, Visibility: "team"})
	})

	_, err := client.CreateDashboard(t.Context(), "New Dash", "", 0, nil)
	require.NoError(t, err)
}

func TestUpdateDashboard(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)
		assert.Equal(t, "/dashboards/9", r.URL.Path)

		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "Renamed", body["title"])
		// omitted fields are left out so the server leaves them unchanged
		_, hasVisibility := body["visibility"]
		assert.False(t, hasVisibility)

		w.WriteHeader(http.StatusOK)
		title := "Renamed"
		writeJSON(t, w, Dashboard{ID: 9, Title: &title, Visibility: "team", URL: "https://cloud.getbruin.com/acme/dashboards/9?mode=edit"})
	})

	dashboard, err := client.UpdateDashboard(t.Context(), 9, map[string]any{"title": "Renamed"})
	require.NoError(t, err)
	assert.Equal(t, 9, dashboard.ID)
	assert.Equal(t, "Renamed", *dashboard.Title)
}

func TestDeleteDashboard(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/dashboards/9", r.URL.Path)

		writeJSON(t, w, map[string]bool{"success": true})
	})

	require.NoError(t, client.DeleteDashboard(t.Context(), 9))
}

func TestListScheduledAgents(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/scheduled-agents", r.URL.Path)

		w.WriteHeader(http.StatusOK)
		title := "Nightly"
		cron := "0 0 * * *"
		writeJSON(t, w, map[string]any{
			"scheduled_agents": []ScheduledAgent{{ID: 3, Title: &title, IsActive: true, ScheduleCron: &cron}},
		})
	})

	runs, err := client.ListScheduledAgents(t.Context())
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, 3, runs[0].ID)
	assert.True(t, runs[0].IsActive)
}

func TestGetScheduledAgent(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/scheduled-agents/5", r.URL.Path)

		w.WriteHeader(http.StatusOK)
		title := "Nightly"
		cron := "0 0 * * *"
		writeJSON(t, w, ScheduledAgent{ID: 5, Title: &title, IsActive: true, ScheduleCron: &cron})
	})

	run, err := client.GetScheduledAgent(t.Context(), 5)
	require.NoError(t, err)
	assert.Equal(t, 5, run.ID)
	assert.Equal(t, "Nightly", *run.Title)
	assert.Equal(t, "0 0 * * *", *run.ScheduleCron)
}

func TestGetScheduledAgent_NotFound(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		writeJSON(t, w, map[string]any{"message": "Scheduled agent not found", "error": "not_found"})
	})

	// A 404 surfaces as an error, not a zero-value run.
	_, err := client.GetScheduledAgent(t.Context(), 999)
	require.Error(t, err)
}

func TestCreateScheduledAgent(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/scheduled-agents", r.URL.Path)

		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.EqualValues(t, 7, body["agent_id"])
		assert.Equal(t, "Daily", body["title"])

		w.WriteHeader(http.StatusCreated)
		title := "Daily"
		writeJSON(t, w, ScheduledAgent{ID: 11, Title: &title, IsActive: false})
	})

	run, err := client.CreateScheduledAgent(t.Context(), map[string]any{"agent_id": 7, "title": "Daily"})
	require.NoError(t, err)
	assert.Equal(t, 11, run.ID)
	// Created as a draft — never active from the API.
	assert.False(t, run.IsActive)
}

func TestUpdateScheduledAgent(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)
		assert.Equal(t, "/scheduled-agents/11", r.URL.Path)

		var body map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "Renamed", body["title"])

		w.WriteHeader(http.StatusOK)
		title := "Renamed"
		writeJSON(t, w, ScheduledAgent{ID: 11, Title: &title})
	})

	run, err := client.UpdateScheduledAgent(t.Context(), 11, map[string]any{"title": "Renamed"})
	require.NoError(t, err)
	assert.Equal(t, "Renamed", *run.Title)
}

func TestTriggerScheduledAgent(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/scheduled-agents/11/trigger", r.URL.Path)

		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, ScheduledAgentExecution{ExecutionID: 42, ThreadID: 99})
	})

	execution, err := client.TriggerScheduledAgent(t.Context(), 11)
	require.NoError(t, err)
	assert.Equal(t, 42, execution.ExecutionID)
	assert.Equal(t, 99, execution.ThreadID)
}

func TestDeleteScheduledAgent(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/scheduled-agents/11", r.URL.Path)

		writeJSON(t, w, map[string]bool{"success": true})
	})

	require.NoError(t, client.DeleteScheduledAgent(t.Context(), 11))
}

func TestGetCostExplorerSchema(t *testing.T) {
	t.Parallel()
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.RequestURI()
		writeJSON(t, w, CostExplorerSchema{
			Platform:           "bigquery",
			AvailablePlatforms: []string{"bigquery"},
			Dimensions:         []CostDimension{{Key: "pipeline_id", Label: "Pipeline"}},
			Filters:            []CostFilterField{{Field: "pipeline_id", Op: "in", Multiple: true}},
			TimeDimensions:     []string{"day", "week", "month"},
		})
	})

	schema, err := client.GetCostExplorerSchema(t.Context(), "databricks")
	require.NoError(t, err)
	assert.Equal(t, "/cost-explorer/schema?platform=databricks", gotPath)
	assert.Equal(t, "bigquery", schema.Platform)
	assert.Equal(t, []string{"day", "week", "month"}, schema.TimeDimensions)
	require.Len(t, schema.Dimensions, 1)
	assert.Equal(t, "pipeline_id", schema.Dimensions[0].Key)
}

func TestGetCostExplorer(t *testing.T) {
	t.Parallel()
	var gotBody map[string]any
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		gotBody = readJSON(t, r)
		next := 2
		writeJSON(t, w, CostExplorerResponse{
			Platform:     "bigquery",
			TotalRows:    3,
			ReturnedRows: 2,
			Offset:       0,
			Truncated:    true,
			NextOffset:   &next,
			Rows:         []map[string]any{{"pipeline_id": "daily-etl", "total_cost_usd": 42.5}},
		})
	})

	resp, err := client.GetCostExplorer(t.Context(), CostExplorerRequest{
		StartDate: "2026-07-01",
		EndDate:   "2026-07-31",
		Dimension: "pipeline_id",
		Filters:   []CostFilter{{Field: "pipeline_id", Op: "in", Value: []string{"daily-etl"}}},
		Limit:     2,
	})
	require.NoError(t, err)
	assert.Equal(t, "2026-07-01", gotBody["start_date"])
	assert.Equal(t, "pipeline_id", gotBody["dimension"])
	assert.Equal(t, 3, resp.TotalRows)
	require.NotNil(t, resp.NextOffset)
	assert.Equal(t, 2, *resp.NextOffset)
}

func TestListAuditLogs(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/audit-logs", r.URL.Path)
		q := r.URL.Query()
		assert.Equal(t, []string{"login", "new_conn"}, q["types[]"])
		assert.Equal(t, []string{"7"}, q["userIds[]"])
		assert.Equal(t, "2026-07-01", q.Get("startDate"))
		assert.Equal(t, "50", q.Get("limit"))
		assert.Equal(t, "10", q.Get("offset"))
		w.WriteHeader(http.StatusOK)
		writeJSON(t, w, map[string]any{
			"auditLogs": []AuditLog{
				{Type: "login", UserIdentifier: "alice@example.com"},
			},
			"total": 1,
		})
	})

	logs, err := client.ListAuditLogs(t.Context(), AuditLogListOptions{
		Types:     []string{"login", "new_conn"},
		UserIDs:   []string{"7"},
		StartDate: "2026-07-01",
		Limit:     50,
		Offset:    10,
	})
	require.NoError(t, err)
	require.Len(t, logs, 1)
	assert.Equal(t, "login", logs[0].Type)
	assert.Equal(t, "alice@example.com", logs[0].UserIdentifier)
}

func TestListRunStates(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/scheduled-agents/7/run-states", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"run_states":[{"name":"a.md","content":"x"},{"name":"b.md","content":"y"}]}`))
	})

	states, err := client.ListRunStates(t.Context(), 7)
	require.NoError(t, err)
	require.Len(t, states, 2)
	assert.Equal(t, "a.md", states[0].Name)
	assert.Equal(t, "x", states[0].Content)
}

func TestGetRunState(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/scheduled-agents/7/run-states/notes.md", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"name":"notes.md","content":"hello"}`))
	})

	state, err := client.GetRunState(t.Context(), 7, "notes.md")
	require.NoError(t, err)
	assert.Equal(t, "notes.md", state.Name)
	assert.Equal(t, "hello", state.Content)
}

func TestSetRunState(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/scheduled-agents/7/run-states/notes.md", r.URL.Path)
		assert.Equal(t, http.MethodPut, r.Method)
		body := readJSON(t, r)
		assert.Equal(t, "new content", body["content"])
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"name":"notes.md","content":"new content"}`))
	})

	state, err := client.SetRunState(t.Context(), 7, "notes.md", "new content")
	require.NoError(t, err)
	assert.Equal(t, "new content", state.Content)
}

// A name with characters that need percent-encoding must be escaped in the path
// so it can't break out of the run-states segment.
func TestSetRunStateEscapesName(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/scheduled-agents/7/run-states/my%20notes.md", r.URL.EscapedPath())
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"name":"my notes.md","content":"x"}`))
	})

	_, err := client.SetRunState(t.Context(), 7, "my notes.md", "x")
	require.NoError(t, err)
}

func TestDeleteRunState(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/scheduled-agents/7/run-states/gone.md", r.URL.Path)
		assert.Equal(t, http.MethodDelete, r.Method)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	})

	err := client.DeleteRunState(t.Context(), 7, "gone.md")
	require.NoError(t, err)
}
