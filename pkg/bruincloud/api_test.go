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
		assert.Equal(t, "/trigger-pipeline-runs", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		body := readJSON(t, r)
		pipelines := body["pipelines"].([]any)
		assert.Len(t, pipelines, 1)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`200`))
	})

	err := client.TriggerRun(t.Context(), "proj", "pipe", "2026-01-01", "2026-01-02")
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

	threads, err := client.ListAgentThreads(t.Context(), 1, 0, 0)
	require.NoError(t, err)
	require.Len(t, threads, 1)
	assert.Equal(t, 10, threads[0].ID)
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
