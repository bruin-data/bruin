package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	bigquery2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestExistingJobRef(t *testing.T) {
	t.Parallel()

	conflict := func(msg string) error {
		return &googleapi.Error{Code: http.StatusConflict, Message: msg}
	}

	tests := []struct {
		name   string
		err    error
		want   jobRef
		wantOK bool
	}{
		{
			name:   "regional job",
			err:    conflict("Already Exists: Job my-project:europe-west1.bruin_main-abc123"),
			want:   jobRef{projectID: "my-project", location: "europe-west1", jobID: "bruin_main-abc123"},
			wantOK: true,
		},
		{
			name:   "domain-scoped project",
			err:    conflict("Already Exists: Job example.com:my-project:US.job_1"),
			want:   jobRef{projectID: "example.com:my-project", location: "US", jobID: "job_1"},
			wantOK: true,
		},
		{
			name:   "trailing punctuation",
			err:    conflict("Already Exists: Job my-project:US.job_1, try again"),
			want:   jobRef{projectID: "my-project", location: "US", jobID: "job_1"},
			wantOK: true,
		},
		{
			name:   "no location",
			err:    conflict("Already Exists: Job my-project:job_1"),
			want:   jobRef{projectID: "my-project", jobID: "job_1"},
			wantOK: true,
		},
		{
			name:   "wrapped",
			err:    fmt.Errorf("wrapped: %w", conflict("Already Exists: Job my-project:US.job_1")),
			want:   jobRef{projectID: "my-project", location: "US", jobID: "job_1"},
			wantOK: true,
		},
		{name: "409 without job ref", err: conflict("Already Exists")},
		{name: "other API error", err: &googleapi.Error{Code: http.StatusBadRequest, Message: "Job my-project:US.job_1"}},
		{name: "non-API error", err: errors.New("Already Exists: Job my-project:US.job_1")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := existingJobRef(tt.err)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

// When the first jobs.insert reaches BigQuery but its response is lost, the
// client library retries with the same job ID and gets 409. The run must attach
// to the job named in the error instead of failing.
func TestClient_RunQueryWithoutResult_AttachesToExistingJobOnRetry(t *testing.T) {
	t.Parallel()

	projectID := "test-project"

	var (
		mu         sync.Mutex
		insertedID string
		inserts    atomic.Int32
		attachedAt atomic.Value
	)

	writeJSON := func(w http.ResponseWriter, v any) {
		resp, err := json.Marshal(v)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(resp)
	}
	jobID := func() string {
		mu.Lock()
		defer mu.Unlock()
		return insertedID
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/projects/"+projectID+"/jobs":
			var body bigquery2.Job
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			if insertedID != "" && insertedID != body.JobReference.JobId {
				mu.Unlock()
				http.Error(w, "retry used a different job ID", http.StatusBadRequest)
				return
			}
			insertedID = body.JobReference.JobId
			mu.Unlock()

			if inserts.Add(1) == 1 {
				// The job is created, but the response never makes it back.
				w.WriteHeader(http.StatusServiceUnavailable)
				writeJSON(w, map[string]any{"error": map[string]any{"code": 503, "message": "backend error"}})
				return
			}
			w.WriteHeader(http.StatusConflict)
			writeJSON(w, map[string]any{"error": map[string]any{
				"code":    409,
				"message": fmt.Sprintf("Already Exists: Job %s:europe-west1.%s", projectID, body.JobReference.JobId),
				"errors":  []map[string]any{{"reason": "duplicate"}},
			}})
			return

		case r.Method == http.MethodGet && r.URL.Path == "/projects/"+projectID+"/jobs/"+jobID():
			attachedAt.Store(r.URL.Query().Get("location"))
			writeJSON(w, &bigquery2.Job{
				Configuration: &bigquery2.JobConfiguration{
					Query: &bigquery2.JobConfigurationQuery{Query: "select 1"},
				},
				JobReference: &bigquery2.JobReference{JobId: jobID(), ProjectId: projectID, Location: "US"},
				Status:       &bigquery2.JobStatus{State: "DONE"},
			})
			return

		case r.Method == http.MethodGet && r.URL.Path == "/projects/"+projectID+"/queries/"+jobID():
			writeJSON(w, &bigquery2.GetQueryResultsResponse{
				JobReference: &bigquery2.JobReference{JobId: jobID(), ProjectId: projectID, Location: "US"},
				JobComplete:  true,
				Schema:       &bigquery2.TableSchema{},
				TotalRows:    0,
			})
			return
		}

		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("unexpected request: " + r.Method + " " + r.URL.Path))
	}))
	defer server.Close()

	client, err := bigquery.NewClient(
		context.Background(),
		projectID,
		option.WithEndpoint(server.URL),
		option.WithCredentials(&google.Credentials{
			ProjectID:   projectID,
			TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "some-token"}),
		}),
	)
	require.NoError(t, err)

	d := Client{client: client}
	ctx := query.WithQueryType(context.Background(), query.QueryTypeMain)

	err = d.RunQueryWithoutResult(ctx, &query.Query{Query: "select 1"})
	require.NoError(t, err)

	assert.Equal(t, int32(2), inserts.Load(), "the library should have retried the insert once")
	assert.Equal(t, "europe-west1", attachedAt.Load(), "the run should attach to the job named in the 409, in its location")
	assert.True(t, strings.HasPrefix(jobID(), "bruin_main-"))
}
