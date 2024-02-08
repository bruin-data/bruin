package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestDB_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		response   any
		statusCode int
		want       bool
		err        error
	}{
		{
			name:  "bad request",
			query: "select * from users",
			response: map[string]interface{}{
				"error": googleapi.Error{
					Code:    400,
					Message: `Syntax error: Expected "(" or keyword SELECT or keyword WITH but got identifier "sselect" at [3:1], invalidQuery`,
				},
			},
			statusCode: http.StatusBadRequest,
			err:        errors.New(`Syntax error: Expected "(" or keyword SELECT or keyword WITH but got identifier "sselect" at [3:1], invalidQuery`),
		},
		{
			name:  "some validation errors returned",
			query: "select * from users",
			response: &bigquery2.Job{
				JobReference: &bigquery2.JobReference{
					JobId: "job-id",
				},
				Status: &bigquery2.JobStatus{
					ErrorResult: &bigquery2.ErrorProto{
						DebugInfo: "Some debug info",
						Location:  "some location",
						Message:   "some message",
						Reason:    "some reason",
					},
					State:           "DONE",
					ForceSendFields: nil,
					NullFields:      nil,
				},
			},
			statusCode: http.StatusOK,
			err: &bigquery.Error{
				Location: "some location",
				Message:  "some message",
				Reason:   "some reason",
			},
		},
		{
			name:  "Google API returns 404",
			query: "select * from users",
			response: map[string]interface{}{
				"error": googleapi.Error{
					Code:    404,
					Message: "not found: Table project:schema.table was not found in location ABC",
				},
			},
			statusCode: http.StatusNotFound,
			err:        errors.New("not found: Table project:schema.table was not found in location ABC"),
		},
		{
			name:  "no error returned",
			query: "select * from users",
			response: &bigquery2.Job{
				JobReference: &bigquery2.JobReference{
					JobId: "job-id",
				},
				Status: &bigquery2.JobStatus{
					State:  "DONE",
					Errors: nil,
				},
			},
			statusCode: http.StatusOK,
			want:       true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				response, err := json.Marshal(tt.response)
				require.NoError(t, err)

				w.WriteHeader(tt.statusCode)
				_, err = w.Write(response)
				require.NoError(t, err)
			}))
			defer server.Close()

			client, err := bigquery.NewClient(
				context.Background(),
				"some-project-id",
				option.WithEndpoint(server.URL),
				option.WithCredentials(&google.Credentials{
					ProjectID: "some-project-id",
					TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
						AccessToken: "some-token",
					}),
				}),
			)
			require.NoError(t, err)
			client.Location = "US"

			d := Client{client: client}

			got, err := d.IsValid(context.Background(), &query.Query{Query: tt.query})
			if tt.err == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.err.Error())
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDB_RunQueryWithoutResult(t *testing.T) {
	t.Parallel()

	projectID := "test-project"
	jobID := "test-job"

	tests := []struct {
		name                string
		query               string
		jobSubmitResponse   jobSubmitResponse
		queryResultResponse queryResultResponse
		err                 error
	}{
		{
			name:  "bad request",
			query: "select * from users",
			jobSubmitResponse: jobSubmitResponse{
				response: map[string]interface{}{
					"error": googleapi.Error{
						Code:    400,
						Message: `Syntax error: Expected "(" or keyword SELECT or keyword WITH but got identifier "sselect" at [3:1], invalidQuery`,
					},
				},
				statusCode: http.StatusBadRequest,
			},
			err: errors.New(`Syntax error: Expected "(" or keyword SELECT or keyword WITH but got identifier "sselect" at [3:1], invalidQuery`),
		},
		{
			name:  "Google API returns 404",
			query: "select * from users",
			jobSubmitResponse: jobSubmitResponse{
				response: map[string]interface{}{
					"error": googleapi.Error{
						Code:    404,
						Message: "not found: Table project:schema.table was not found in location ABC",
					},
				},
				statusCode: http.StatusNotFound,
			},
			err: errors.New("not found: Table project:schema.table was not found in location ABC"),
		},
		{
			name:  "no error returned",
			query: "select * from users",
			jobSubmitResponse: jobSubmitResponse{
				response: &bigquery2.Job{
					Configuration: &bigquery2.JobConfiguration{
						Query: &bigquery2.JobConfigurationQuery{
							Query: "select * from users",
							DestinationTable: &bigquery2.TableReference{
								ProjectId: projectID,
								DatasetId: "test-dataset",
							},
						},
					},
					JobReference: &bigquery2.JobReference{
						JobId:     jobID,
						ProjectId: projectID,
					},
					Status: &bigquery2.JobStatus{
						State:  "DONE",
						Errors: nil,
					},
				},
				statusCode: http.StatusOK,
			},
			queryResultResponse: queryResultResponse{
				response: &bigquery2.GetQueryResultsResponse{
					JobReference: &bigquery2.JobReference{
						JobId: "job-id",
					},
					JobComplete: true,
				},
				statusCode: http.StatusOK,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(mockBqHandler(t, projectID, jobID, tt.jobSubmitResponse, tt.queryResultResponse))
			defer server.Close()

			client, err := bigquery.NewClient(
				context.Background(),
				projectID,
				option.WithEndpoint(server.URL),
				option.WithCredentials(&google.Credentials{
					ProjectID: projectID,
					TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
						AccessToken: "some-token",
					}),
				}),
			)
			require.NoError(t, err)
			client.Location = "US"

			d := Client{client: client}

			err = d.RunQueryWithoutResult(context.Background(), &query.Query{Query: tt.query})
			if tt.err == nil {
				require.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.err.Error())
			}
		})
	}
}

type jobSubmitResponse struct {
	response   any
	statusCode int
}

type queryResultResponse struct {
	response   *bigquery2.GetQueryResultsResponse
	statusCode int
}

func mockBqHandler(t *testing.T, projectID, jobID string, jsr jobSubmitResponse, qrr queryResultResponse) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.HasPrefix(r.RequestURI, fmt.Sprintf("/projects/%s/queries/%s?", projectID, jobID)) {
			w.WriteHeader(qrr.statusCode)

			response, err := json.Marshal(qrr.response)
			require.NoError(t, err)

			_, err = w.Write(response)
			require.NoError(t, err)
			return
		} else if r.Method == "POST" && strings.HasPrefix(r.RequestURI, fmt.Sprintf("/projects/%s/queries", projectID)) {
			w.WriteHeader(jsr.statusCode)

			response, err := json.Marshal(jsr.response)
			require.NoError(t, err)

			_, err = w.Write(response)
			require.NoError(t, err)
			return
		}

		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("there is no test definition found for the given request"))
		require.NoError(t, err)
	})
}

func TestDB_Select(t *testing.T) {
	t.Parallel()

	projectID := "test-project"
	jobID := "test-job"

	tests := []struct {
		name                string
		query               string
		jobSubmitResponse   jobSubmitResponse
		queryResultResponse queryResultResponse
		want                [][]interface{}
		err                 error
	}{
		{
			name:  "bad request",
			query: "select * from users",
			jobSubmitResponse: jobSubmitResponse{
				response: map[string]interface{}{
					"error": googleapi.Error{
						Code:    400,
						Message: `Syntax error: Expected "(" or keyword SELECT or keyword WITH but got identifier "sselect" at [3:1], invalidQuery`,
					},
				},
				statusCode: http.StatusBadRequest,
			},
			err: errors.New(`Syntax error: Expected "(" or keyword SELECT or keyword WITH but got identifier "sselect" at [3:1], invalidQuery`),
		},
		{
			name:  "Google API returns 404",
			query: "select * from users",
			jobSubmitResponse: jobSubmitResponse{
				response: map[string]interface{}{
					"error": googleapi.Error{
						Code:    404,
						Message: "not found: Table project:schema.table was not found in location ABC",
					},
				},
				statusCode: http.StatusNotFound,
			},
			err: errors.New("not found: Table project:schema.table was not found in location ABC"),
		},
		{
			name:  "no error returned",
			query: "select * from users",
			jobSubmitResponse: jobSubmitResponse{
				response: &bigquery2.Job{
					Configuration: &bigquery2.JobConfiguration{
						Query: &bigquery2.JobConfigurationQuery{
							Query: "select * from users",
							DestinationTable: &bigquery2.TableReference{
								ProjectId: projectID,
								DatasetId: "test-dataset",
							},
						},
					},
					JobReference: &bigquery2.JobReference{
						JobId:     jobID,
						ProjectId: projectID,
					},
					Status: &bigquery2.JobStatus{
						State:  "DONE",
						Errors: nil,
					},
				},
				statusCode: http.StatusOK,
			},
			queryResultResponse: queryResultResponse{
				response: &bigquery2.GetQueryResultsResponse{
					JobReference: &bigquery2.JobReference{
						JobId: "job-id",
					},
					JobComplete: true,
					Schema: &bigquery2.TableSchema{
						Fields: []*bigquery2.TableFieldSchema{
							{
								Name: "first_name",
								Type: "STRING",
							},
							{
								Name: "last_name",
								Type: "STRING",
							},
							{
								Name: "age",
								Type: "INTEGER",
							},
						},
					},
					Rows: []*bigquery2.TableRow{
						{
							F: []*bigquery2.TableCell{
								{
									V: "jane",
								},
								{
									V: "doe",
								},
								{
									V: "30",
								},
							},
						},
						{
							F: []*bigquery2.TableCell{
								{
									V: "joe",
								},
								{
									V: "doe",
								},
								{
									V: "28",
								},
							},
						},
					},
				},
				statusCode: http.StatusOK,
			},
			want: [][]interface{}{
				{"jane", "doe", int64(30)},
				{"joe", "doe", int64(28)},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(mockBqHandler(t, projectID, jobID, tt.jobSubmitResponse, tt.queryResultResponse))
			defer server.Close()

			client, err := bigquery.NewClient(
				context.Background(),
				projectID,
				option.WithEndpoint(server.URL),
				option.WithCredentials(&google.Credentials{
					ProjectID: projectID,
					TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
						AccessToken: "some-token",
					}),
				}),
			)
			require.NoError(t, err)
			client.Location = "US"

			d := Client{client: client}

			got, err := d.Select(context.Background(), &query.Query{Query: tt.query})
			if tt.err == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.err.Error())
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
