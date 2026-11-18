package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	bigquery2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const jobBackendErrorMessage = "The job encountered an error during execution. Retrying the job may solve the problem."

func TestHasTransientReason(t *testing.T) {
	t.Parallel()

	withReason := func(code int, reason string) error {
		return &googleapi.Error{Code: code, Message: "message", Errors: []googleapi.ErrorItem{{Reason: reason}}}
	}
	tests := map[string]struct {
		err  error
		want bool
	}{
		"job failed inside BigQuery":    {err: withReason(400, "jobBackendError"), want: true},
		"job internal error":            {err: withReason(400, "jobInternalError"), want: true},
		"rate limit":                    {err: withReason(403, "rateLimitExceeded"), want: true},
		"server error":                  {err: &googleapi.Error{Code: 503}, want: true},
		"too many requests":             {err: &googleapi.Error{Code: 429}, want: true},
		"wrapped":                       {err: fmt.Errorf("query failed: %w", withReason(500, "backendError")), want: true},
		"job status error":              {err: &bigquery.Error{Reason: "jobInternalError"}, want: true},
		"invalid query":                 {err: withReason(400, "invalidQuery"), want: false},
		"missing table":                 {err: withReason(404, "notFound"), want: false},
		"no access":                     {err: withReason(403, "accessDenied"), want: false},
		"quota":                         {err: withReason(403, "quotaExceeded"), want: false},
		"not implemented":               {err: &googleapi.Error{Code: 501}, want: false},
		"job status error, not retried": {err: &bigquery.Error{Reason: "invalidQuery"}, want: false},
		"plain error":                   {err: errors.New("boom"), want: false},
	}
	for name, tt := range tests {
		assert.Equal(t, tt.want, hasTransientReason(tt.err), name)
	}
}

func TestQueryError(t *testing.T) {
	t.Parallel()

	transient := queryError(&googleapi.Error{Code: 400, Message: jobBackendErrorMessage, Errors: []googleapi.ErrorItem{{Reason: "jobBackendError"}}})
	require.EqualError(t, transient, jobBackendErrorMessage, "reads like formatError's output")
	assert.True(t, isTransient(transient))

	invalid := queryError(&googleapi.Error{Code: 400, Message: "Syntax error", Errors: []googleapi.ErrorItem{{Reason: "invalidQuery"}}})
	require.EqualError(t, invalid, "Syntax error")
	assert.False(t, isTransient(invalid))

	serverErr := &googleapi.Error{Code: 503, Message: "unavailable"}
	var apiErr *googleapi.Error
	require.ErrorAs(t, queryError(serverErr), &apiErr, "errors kept as googleapi errors stay reachable")
	assert.Same(t, serverErr, apiErr)
}

// fakeSensorBigQuery answers each query job with the next of the given getQueryResults answers:
// an error reason, or "" for a result of 1.
func fakeSensorBigQuery(t *testing.T, answers ...string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var jobs atomic.Int32
	write := func(w http.ResponseWriter, status int, body any) {
		w.WriteHeader(status)
		assert.NoError(t, json.NewEncoder(w).Encode(body))
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/projects/"+testProjectID+"/jobs"):
			n := jobs.Add(1)
			write(w, http.StatusOK, &bigquery2.Job{
				JobReference: &bigquery2.JobReference{JobId: fmt.Sprintf("job-%d", n), ProjectId: testProjectID, Location: "US"},
				Configuration: &bigquery2.JobConfiguration{Query: &bigquery2.JobConfigurationQuery{
					DestinationTable: &bigquery2.TableReference{ProjectId: testProjectID, DatasetId: "anon"},
				}},
				Status: &bigquery2.JobStatus{State: "DONE"},
			})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/projects/"+testProjectID+"/queries/job-"):
			var n int
			if _, err := fmt.Sscanf(r.URL.Path, "/projects/"+testProjectID+"/queries/job-%d", &n); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if reason := answers[n-1]; reason != "" {
				write(w, http.StatusBadRequest, map[string]any{"error": googleapi.Error{
					Code: http.StatusBadRequest, Message: jobBackendErrorMessage, Errors: []googleapi.ErrorItem{{Reason: reason, Message: jobBackendErrorMessage}},
				}})
				return
			}
			write(w, http.StatusOK, &bigquery2.GetQueryResultsResponse{
				JobReference: &bigquery2.JobReference{JobId: fmt.Sprintf("job-%d", n), ProjectId: testProjectID},
				JobComplete:  true,
				Schema:       &bigquery2.TableSchema{Fields: []*bigquery2.TableFieldSchema{{Name: "ok", Type: "INTEGER"}}},
				Rows:         []*bigquery2.TableRow{{F: []*bigquery2.TableCell{{V: "1"}}}},
				TotalRows:    1,
			})
		default:
			http.Error(w, "unexpected request: "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
		}
	}))
	t.Cleanup(server.Close)
	return server, &jobs
}

func runQuerySensor(t *testing.T, server *httptest.Server, sensorMode string) (string, error) {
	t.Helper()
	client, err := bigquery.NewClient(
		t.Context(), testProjectID,
		option.WithEndpoint(server.URL),
		option.WithCredentials(&google.Credentials{ProjectID: testProjectID, TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "token"})}),
	)
	require.NoError(t, err)
	client.Location = "US"

	conns := &mockConnectionGetter{}
	conns.On("GetConnection", "bq").Return(&Client{client: client})
	extractor := &mockExtractor{}
	extractor.On("ExtractQueriesFromString", "select 1").Return([]*query.Query{{Query: "select 1"}}, nil)
	asset := &pipeline.Asset{
		Name:       "sensor",
		Type:       pipeline.AssetTypeBigqueryQuerySensor,
		Connection: "bq",
		Parameters: pipeline.ParameterMap{"query": "select 1", "poke_interval": "0"},
	}

	var out bytes.Buffer
	ctx := t.Context()
	ctx = context.WithValue(ctx, executor.KeyPrinter, &out)
	err = NewQuerySensor(conns, extractor, sensorMode).RunTask(ctx, &pipeline.Pipeline{}, asset)
	return out.String(), err
}

func TestQuerySensor_PokesAgainAfterTransientError(t *testing.T) {
	t.Parallel()

	server, jobs := fakeSensorBigQuery(t, "jobBackendError", "")
	out, err := runQuerySensor(t, server, "wait")

	require.NoError(t, err)
	assert.Equal(t, int32(2), jobs.Load())
	assert.Contains(t, out, "Sensor query failed with a transient BigQuery error, trying again in 0 seconds: "+jobBackendErrorMessage)
}

func TestQuerySensor_FailsOnOtherErrors(t *testing.T) {
	t.Parallel()

	server, jobs := fakeSensorBigQuery(t, "invalidQuery", "")
	_, err := runQuerySensor(t, server, "wait")

	require.EqualError(t, err, jobBackendErrorMessage)
	assert.Equal(t, int32(1), jobs.Load())
}

func TestQuerySensor_OnceFailsOnTransientError(t *testing.T) {
	t.Parallel()

	server, jobs := fakeSensorBigQuery(t, "jobBackendError", "")
	_, err := runQuerySensor(t, server, "once")

	require.EqualError(t, err, jobBackendErrorMessage)
	assert.Equal(t, int32(1), jobs.Load())
}
