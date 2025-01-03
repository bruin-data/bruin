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

const testProjectID = "test-project"

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
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				response, err := json.Marshal(tt.response)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError) // Handle error
					return
				}

				w.WriteHeader(tt.statusCode)
				_, err = w.Write(response)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError) // Handle error
					return
				}
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
		if r.Method == http.MethodGet && strings.HasPrefix(r.RequestURI, fmt.Sprintf("/projects/%s/queries/%s?", projectID, jobID)) {
			w.WriteHeader(qrr.statusCode)

			response, err := json.Marshal(qrr.response)
			if err != nil {
				t.Fatal(err)
			}

			_, err = w.Write(response)
			if err != nil {
				t.Fatal(err)
			}
			return
		} else if r.Method == http.MethodPost && strings.HasPrefix(r.RequestURI, fmt.Sprintf("/projects/%s/queries", projectID)) {
			w.WriteHeader(jsr.statusCode)

			response, err := json.Marshal(jsr.response)
			if err != nil {
				t.Fatal(err)
			} // Updated error handling

			_, err = w.Write(response)
			if err != nil {
				t.Fatal(err)
			} // Updated error handling
			return
		}

		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("there is no test definition found for the given request: " + r.Method + " " + r.RequestURI))
		if err != nil {
			t.Fatal(err)
		} // Updated error handling
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

func TestDB_UpdateTableMetadataIfNotExists(t *testing.T) {
	t.Parallel()

	projectID := "test-project"
	schema := "myschema"
	table := "mytable"
	assetName := fmt.Sprintf("%s.%s", schema, table)

	tests := []struct {
		name          string
		asset         *pipeline.Asset
		tableResponse *bigquery2.Table
		err           error
	}{
		{
			name:  "asset has no metadata",
			asset: &pipeline.Asset{},
			err:   NoMetadataUpdatedError{},
		},
		{
			name: "asset has description",
			asset: &pipeline.Asset{
				Name:        assetName,
				Description: "test123",
			},
			tableResponse: &bigquery2.Table{
				Description: "some old description",
			},
		},
		{
			name: "asset has description",
			asset: &pipeline.Asset{
				Name:        assetName,
				Description: "test123",
				Columns: []pipeline.Column{
					{
						Name:        "col1",
						Description: "first col",
						PrimaryKey:  true,
					},
					{
						Name:        "col2",
						Description: "second col",
						PrimaryKey:  true,
					},
					{
						Name:        "col3",
						Description: "third col",
					},
					{
						Name:        "some missing column", // this one should not be put into the patch request
						Description: "fourth col",
					},
				},
			},
			tableResponse: &bigquery2.Table{
				Description: "some old description",
				Schema: &bigquery2.TableSchema{
					Fields: []*bigquery2.TableFieldSchema{
						{
							Name:        "col1",
							Description: "old description",
						},
						{
							Name:        "col2",
							Description: "second old description",
						},
						{
							Name:        "col3",
							Description: "second old description",
						},
						{
							Name:        "an_existing_but_not_documented_column",
							Description: "some old description",
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasPrefix(r.RequestURI, fmt.Sprintf("/projects/%s/datasets/%s/tables/%s", projectID, schema, table)) {
					w.WriteHeader(http.StatusInternalServerError)
					_, err := w.Write([]byte("there is no test definition found for the given request: " + r.Method + " " + r.RequestURI))
					assert.NoError(t, err)
					return
				}

				switch r.Method {
				// this is the request that fetches the table metadata
				case http.MethodGet:
					w.WriteHeader(http.StatusOK)

					response, err := json.Marshal(tt.tableResponse)
					assert.NoError(t, err)

					_, err = w.Write(response)
					assert.NoError(t, err)
					return

				// this is the request that updates the table metadata with the new details
				case http.MethodPatch:
					w.WriteHeader(http.StatusOK)

					// read the body
					var table bigquery2.Table
					err := json.NewDecoder(r.Body).Decode(&table)
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError) // Handle error
						return
					}

					colsByName := make(map[string]*pipeline.Column, len(tt.asset.Columns))
					for _, col := range tt.asset.Columns {
						colsByName[col.Name] = &col
					}

					// ensure the asset description is saved
					assert.Equal(t, tt.asset.Description, table.Description)

					if table.Schema != nil {
						// ensure the column description is saved
						for _, col := range table.Schema.Fields {
							if c, ok := colsByName[col.Name]; ok {
								assert.Equal(t, c.Description, col.Description)
							}
						}

						// ensure we didn't drop any columns that we didn't have documented
						assert.Equal(t, len(tt.tableResponse.Schema.Fields), len(table.Schema.Fields))

						// ensure the primary keys are set correctly
						primaryKeys := tt.asset.ColumnNamesWithPrimaryKey()
						assert.Equal(t, primaryKeys, table.TableConstraints.PrimaryKey.Columns)
					} else {
						assert.Nil(t, tt.tableResponse.Schema)
					}

					response, err := json.Marshal(tt.tableResponse)
					assert.NoError(t, err)

					_, err = w.Write(response)
					assert.NoError(t, err)
					return
				}

				w.WriteHeader(http.StatusInternalServerError)
				_, err := w.Write([]byte("there is no test definition found for the given request: " + r.Method + " " + r.RequestURI))
				assert.NoError(t, err)
			}))
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

			err = d.UpdateTableMetadataIfNotExist(context.Background(), tt.asset)
			if tt.err == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.err.Error())
			}
		})
	}
}

func TestDB_SelectWithSchema(t *testing.T) {
	t.Parallel()

	projectID := "test-project"
	jobID := "test-job"

	tests := []struct {
		name                string
		query               string
		jobSubmitResponse   jobSubmitResponse
		queryResultResponse queryResultResponse
		want                *query.QueryResult
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
			err: errors.New(`failed to initiate query read: googleapi: Error 400: Syntax error: Expected "(" or keyword SELECT or keyword WITH but got identifier "sselect" at [3:1], invalidQuery`),
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
			err: errors.New("failed to initiate query read: googleapi: Error 404: not found: Table project:schema.table was not found in location ABC"),
		},
		{
			name:  "successful query with schema",
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
			want: &query.QueryResult{
				Columns: []string{"first_name", "last_name", "age"},
				Rows: [][]interface{}{
					{"jane", "doe", int64(30)},
					{"joe", "doe", int64(28)},
				},
			},
		},
	}

	for _, tt := range tests {
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

			got, err := d.SelectWithSchema(context.Background(), &query.Query{Query: tt.query})
			if tt.err == nil {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			} else {
				assert.EqualError(t, err, tt.err.Error())
			}
		})
	}
}

func TestClient_getTableRef(t *testing.T) {
	t.Parallel()

	projectID := testProjectID
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tests := []struct {
		name        string
		tableName   string
		wantErr     bool
		errContains string
	}{
		{
			name:      "valid two-part table name",
			tableName: "dataset.table",
			wantErr:   false,
		},
		{
			name:      "valid three-part table name",
			tableName: "project.dataset.table",
			wantErr:   false,
		},
		{
			name:        "invalid one-part table name",
			tableName:   "table",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "invalid four-part table name",
			tableName:   "a.b.c.d",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "empty table name",
			tableName:   "",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "invalid trailing dot",
			tableName:   "dataset.table.",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "invalid leading dot",
			tableName:   ".dataset.table",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "invalid consecutive dots",
			tableName:   "project..table",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "only dots",
			tableName:   "..",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "three dots",
			tableName:   "...",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := bigquery.NewClient(
				context.Background(),
				projectID,
				option.WithEndpoint(srv.URL),
				option.WithCredentials(&google.Credentials{
					ProjectID: projectID,
					TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
						AccessToken: "some-token",
					}),
				}),
			)
			require.NoError(t, err)

			d := Client{client: client}

			tableRef, err := d.getTableRef(tt.tableName)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Nil(t, tableRef)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, tableRef)

			// For two-part names, verify the table and dataset
			if strings.Count(tt.tableName, ".") == 1 {
				parts := strings.Split(tt.tableName, ".")
				assert.Equal(t, parts[0], tableRef.DatasetID)
				assert.Equal(t, parts[1], tableRef.TableID)
				assert.Equal(t, projectID, tableRef.ProjectID)
			}

			// For three-part names, verify project, dataset and table
			if strings.Count(tt.tableName, ".") == 2 {
				parts := strings.Split(tt.tableName, ".")
				assert.Equal(t, parts[0], tableRef.ProjectID)
				assert.Equal(t, parts[1], tableRef.DatasetID)
				assert.Equal(t, parts[2], tableRef.TableID)
			}
		})
	}
}

func TestClient_getTableRef_TableNameValidation(t *testing.T) {
	t.Parallel()

	projectID := "test-project"
	client := &Client{
		client: &bigquery.Client{},
		config: &Config{
			ProjectID: projectID,
		},
	}

	tests := []struct {
		name        string
		tableName   string
		wantErr     bool
		errContains string
	}{
		{
			name:      "valid two-part table name",
			tableName: "dataset.table",
			wantErr:   false,
		},
		{
			name:      "valid three-part table name",
			tableName: "project.dataset.table",
			wantErr:   false,
		},
		{
			name:        "invalid one-part table name",
			tableName:   "table",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "invalid four-part table name",
			tableName:   "a.b.c.d",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "empty table name",
			tableName:   "",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "invalid trailing dot",
			tableName:   "dataset.table.",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "invalid leading dot",
			tableName:   ".dataset.table",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "invalid consecutive dots",
			tableName:   "project..table",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "only dots",
			tableName:   "..",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "three dots",
			tableName:   "...",
			wantErr:     true,
			errContains: "must be in dataset.table or project.dataset.table format",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tableRef, err := client.getTableRef(tt.tableName)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Nil(t, tableRef)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, tableRef)

			// For two-part names, verify the table and dataset
			if strings.Count(tt.tableName, ".") == 1 {
				parts := strings.Split(tt.tableName, ".")
				assert.Equal(t, parts[0], tableRef.DatasetID)
				assert.Equal(t, parts[1], tableRef.TableID)
				assert.Equal(t, projectID, tableRef.ProjectID)
			}

			// For three-part names, verify project, dataset and table
			if strings.Count(tt.tableName, ".") == 2 {
				parts := strings.Split(tt.tableName, ".")
				assert.Equal(t, parts[0], tableRef.ProjectID)
				assert.Equal(t, parts[1], tableRef.DatasetID)
				assert.Equal(t, parts[2], tableRef.TableID)
			}
		})
	}
}

func TestIsSamePartitioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		meta     *bigquery.TableMetadata
		asset    *pipeline.Asset
		expected bool
	}{
		{
			name: "matching time partitioning",
			meta: &bigquery.TableMetadata{
				TimePartitioning: &bigquery.TimePartitioning{
					Field: "date_field",
				},
			},
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					PartitionBy: "date_field",
				},
			},
			expected: true,
		},
		{
			name: "mismatched time partitioning",
			meta: &bigquery.TableMetadata{
				TimePartitioning: &bigquery.TimePartitioning{
					Field: "date_field",
				},
			},
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					PartitionBy: "other_field",
				},
			},
			expected: false,
		},
		{
			name: "matching range partitioning",
			meta: &bigquery.TableMetadata{
				RangePartitioning: &bigquery.RangePartitioning{
					Field: "id_field",
				},
			},
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					PartitionBy: "id_field",
				},
			},
			expected: true,
		},
		{
			name: "mismatched range partitioning",
			meta: &bigquery.TableMetadata{
				RangePartitioning: &bigquery.RangePartitioning{
					Field: "id_field",
				},
			},
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					PartitionBy: "other_field",
				},
			},
			expected: false,
		},
		{
			name: "no partitioning in metadata",
			meta: &bigquery.TableMetadata{},
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					PartitionBy: "some_field",
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := IsSamePartitioning(tt.meta, tt.asset)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsSameClustering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		meta     *bigquery.TableMetadata
		asset    *pipeline.Asset
		expected bool
	}{
		{
			name: "matching single field clustering",
			meta: &bigquery.TableMetadata{
				Clustering: &bigquery.Clustering{
					Fields: []string{"field1"},
				},
			},
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					ClusterBy: []string{"field1"},
				},
			},
			expected: true,
		},
		{
			name: "matching multiple fields clustering",
			meta: &bigquery.TableMetadata{
				Clustering: &bigquery.Clustering{
					Fields: []string{"field1", "field2", "field3"},
				},
			},
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					ClusterBy: []string{"field1", "field2", "field3"},
				},
			},
			expected: true,
		},
		{
			name: "different number of clustering fields",
			meta: &bigquery.TableMetadata{
				Clustering: &bigquery.Clustering{
					Fields: []string{"field1", "field2"},
				},
			},
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					ClusterBy: []string{"field1"},
				},
			},
			expected: false,
		},
		{
			name: "different field order",
			meta: &bigquery.TableMetadata{
				Clustering: &bigquery.Clustering{
					Fields: []string{"field1", "field2"},
				},
			},
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					ClusterBy: []string{"field2", "field1"},
				},
			},
			expected: false,
		},
		{
			name: "different field names",
			meta: &bigquery.TableMetadata{
				Clustering: &bigquery.Clustering{
					Fields: []string{"field1", "field2"},
				},
			},
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					ClusterBy: []string{"field1", "field3"},
				},
			},
			expected: false,
		},
		{
			name: "empty clustering fields in both",
			meta: &bigquery.TableMetadata{
				Clustering: &bigquery.Clustering{
					Fields: []string{},
				},
			},
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					ClusterBy: []string{},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := IsSameClustering(tt.meta, tt.asset)
			assert.Equal(t, tt.expected, result)
		})
	}
}
