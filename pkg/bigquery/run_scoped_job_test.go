package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	bigquery2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

// cloudQuery builds the main query of an asset the way the operator does in a Bruin Cloud run.
func cloudQuery(t *testing.T, sql, runID, asset string, try int) *query.Query {
	t.Helper()
	annotations := fmt.Sprintf(`{"asset":%q,"run_id":%q,"try_number":%d,"type":"main"}`, asset, runID, try)
	q, err := ansisql.AddAnnotationJSONComment(&query.Query{Query: sql}, annotations)
	require.NoError(t, err)
	return q
}

func runScopedJobID(ctx context.Context, q *query.Query) (string, bool) {
	bqQuery := &bigquery.Query{}
	applyJobIDPrefix(ctx, bqQuery)
	applyRunScopedJobID(ctx, bqQuery, q)
	return bqQuery.JobID, bqQuery.AddJobIDSuffix
}

func TestApplyRunScopedJobID(t *testing.T) {
	t.Parallel()

	mainCtx := query.WithQueryType(t.Context(), query.QueryTypeMain)
	base, suffix := runScopedJobID(mainCtx, cloudQuery(t, "select 1", "run-1", "a.b", 1))
	assert.Regexp(t, "^bruin_main-[0-9a-f]{32}$", base)
	assert.False(t, suffix)

	retry, _ := runScopedJobID(mainCtx, cloudQuery(t, "select 1", "run-1", "a.b", 2))
	assert.Equal(t, base, retry, "a retry of the asset in the same run must get the same job ID")

	for name, q := range map[string]*query.Query{
		"another run":   cloudQuery(t, "select 1", "run-2", "a.b", 1),
		"another asset": cloudQuery(t, "select 1", "run-1", "a.c", 1),
		"another query": cloudQuery(t, "select 2", "run-1", "a.b", 1),
	} {
		id, _ := runScopedJobID(mainCtx, q)
		assert.NotEqual(t, base, id, name)
	}

	// Left to the library without a run ID, for other query types and in read-only mode.
	id, suffix := runScopedJobID(mainCtx, &query.Query{Query: "select 1"})
	assert.Equal(t, "bruin_main", id)
	assert.True(t, suffix)

	sensorCtx := query.WithQueryType(t.Context(), query.QueryTypeSensor)
	id, suffix = runScopedJobID(sensorCtx, cloudQuery(t, "select 1", "run-1", "a.b", 1))
	assert.Equal(t, "bruin_sensor", id)
	assert.True(t, suffix)

	readOnly := &bigquery.Query{}
	applyRunScopedJobID(mainCtx, readOnly, cloudQuery(t, "select 1", "run-1", "a.b", 1))
	assert.Empty(t, readOnly.JobID)
}

func TestClient_RunQueryWithoutResult_EarlierAttemptJob(t *testing.T) {
	t.Parallel()

	type fakeJob struct {
		state   string
		created time.Time
		labels  map[string]string
	}
	earlierAttempt := map[string]string{attemptLabel: "earlier"}

	tests := []struct {
		name string
		// earlier is the job an earlier attempt left under the run-scoped ID, if any.
		earlier *fakeJob
		// loseFirstResponse creates the job on the first insert but answers 503.
		loseFirstResponse bool
		wantInserts       int
		wantJobs          int
	}{
		{
			name:        "earlier attempt still running: wait for it",
			earlier:     &fakeJob{state: "RUNNING", created: time.Now().Add(-time.Hour), labels: earlierAttempt},
			wantInserts: 1,
			wantJobs:    1,
		},
		{
			name:        "earlier attempt finished: run the query again",
			earlier:     &fakeJob{state: "DONE", created: time.Now().Add(-time.Hour), labels: earlierAttempt},
			wantInserts: 2,
			wantJobs:    2,
		},
		{
			name:        "earlier attempt finished a moment ago: run the query again",
			earlier:     &fakeJob{state: "DONE", created: time.Now(), labels: earlierAttempt},
			wantInserts: 2,
			wantJobs:    2,
		},
		{
			name:              "own insert's response lost: follow the job even if it finished",
			loseFirstResponse: true,
			wantInserts:       2,
			wantJobs:          1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			projectID := "test-project"
			ctx := query.WithQueryType(t.Context(), query.QueryTypeMain)
			runScopedID, _ := runScopedJobID(ctx, cloudQuery(t, "select 1", "run-1", "a.b", 1))

			var mu sync.Mutex
			jobs := map[string]*fakeJob{}
			if tt.earlier != nil {
				jobs[runScopedID] = tt.earlier
			}
			var inserted []string

			writeJSON := func(w http.ResponseWriter, status int, v any) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(v)
			}
			apiError := func(w http.ResponseWriter, code int, reason, message string) {
				writeJSON(w, code, map[string]any{"error": map[string]any{
					"code": code, "message": message, "errors": []map[string]any{{"reason": reason, "message": message}},
				}})
			}
			bqJob := func(id string) *bigquery2.Job {
				return &bigquery2.Job{
					Configuration: &bigquery2.JobConfiguration{
						Query:  &bigquery2.JobConfigurationQuery{Query: "select 1"},
						Labels: jobs[id].labels,
					},
					JobReference: &bigquery2.JobReference{JobId: id, ProjectId: projectID, Location: "europe-west1"},
					Status:       &bigquery2.JobStatus{State: jobs[id].state},
					Statistics:   &bigquery2.JobStatistics{CreationTime: jobs[id].created.UnixMilli()},
				}
			}

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				jobsPath := "/projects/" + projectID + "/jobs"
				switch {
				case r.Method == http.MethodPost && r.URL.Path == jobsPath:
					var body bigquery2.Job
					_ = json.NewDecoder(r.Body).Decode(&body)
					id := body.JobReference.JobId
					inserted = append(inserted, id)
					if jobs[id] != nil {
						apiError(w, http.StatusConflict, "duplicate", "Already Exists: Job "+projectID+":europe-west1."+id)
						return
					}
					jobs[id] = &fakeJob{state: "DONE", created: time.Now(), labels: body.Configuration.Labels}
					if tt.loseFirstResponse && len(inserted) == 1 {
						apiError(w, http.StatusServiceUnavailable, "backendError", "Backend error")
						return
					}
					writeJSON(w, http.StatusOK, bqJob(id))
				case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, jobsPath+"/"):
					id := strings.TrimPrefix(r.URL.Path, jobsPath+"/")
					if jobs[id] == nil {
						apiError(w, http.StatusNotFound, "notFound", "Not found")
						return
					}
					writeJSON(w, http.StatusOK, bqJob(id))
				case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/projects/"+projectID+"/queries/"):
					id := strings.TrimPrefix(r.URL.Path, "/projects/"+projectID+"/queries/")
					writeJSON(w, http.StatusOK, &bigquery2.GetQueryResultsResponse{
						JobReference: &bigquery2.JobReference{JobId: id, ProjectId: projectID, Location: "europe-west1"},
						JobComplete:  true,
					})
				default:
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte("unexpected request: " + r.Method + " " + r.URL.Path))
				}
			}))
			defer server.Close()

			client, err := bigquery.NewClient(
				t.Context(),
				projectID,
				option.WithEndpoint(server.URL),
				option.WithCredentials(&google.Credentials{
					ProjectID:   projectID,
					TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "some-token"}),
				}),
			)
			require.NoError(t, err)
			d := Client{client: client}

			err = d.RunQueryWithoutResult(ctx, cloudQuery(t, "select 1", "run-1", "a.b", 2))
			require.NoError(t, err)

			mu.Lock()
			defer mu.Unlock()
			require.Len(t, inserted, tt.wantInserts)
			assert.Equal(t, runScopedID, inserted[0], "the retry must use the run-scoped job ID")
			assert.Len(t, jobs, tt.wantJobs)
			if tt.wantJobs == 2 {
				assert.True(t, strings.HasPrefix(inserted[1], runScopedID+"-"), "the new job should get a fresh suffix")
			}
		})
	}
}
