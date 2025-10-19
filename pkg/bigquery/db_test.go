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
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/diff"
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
				testProjectID,
				option.WithEndpoint(server.URL),
				option.WithCredentials(&google.Credentials{
					ProjectID: testProjectID,
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

	projectID := testProjectID
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

type mockEndpoint struct {
	pattern string
	handler func(http.ResponseWriter, *http.Request, map[string]string)
}

func mockBqSummaryHandler(t *testing.T, projectID string, datasetTables map[string]map[string][]string) http.Handler {
	writeJSON := func(w http.ResponseWriter, data interface{}) {
		resp, err := json.Marshal(data)
		if err != nil {
			t.Logf("failed to marshal response: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, err = w.Write(resp)
		if err != nil {
			t.Logf("failed to write response: %v", err)
		}
	}

	writeError := func(w http.ResponseWriter, code int, message string) {
		w.WriteHeader(code)
		writeJSON(w, map[string]interface{}{
			"error": map[string]interface{}{
				"code":    code,
				"message": message,
			},
		})
	}

	endpoints := []mockEndpoint{
		{
			fmt.Sprintf("/projects/%s/datasets", projectID),
			func(w http.ResponseWriter, r *http.Request, params map[string]string) {
				datasets := make([]*bigquery2.DatasetListDatasets, 0, len(datasetTables))
				for ds := range datasetTables {
					datasets = append(datasets, &bigquery2.DatasetListDatasets{
						DatasetReference: &bigquery2.DatasetReference{ProjectId: projectID, DatasetId: ds},
					})
				}
				writeJSON(w, &bigquery2.DatasetList{Datasets: datasets})
			},
		},
		{
			fmt.Sprintf("/projects/%s/datasets/{datasetID}", projectID),
			func(w http.ResponseWriter, r *http.Request, params map[string]string) {
				datasetID := params["datasetID"]
				if tables, exists := datasetTables[datasetID]; exists && tables != nil {
					writeJSON(w, &bigquery2.Dataset{
						DatasetReference: &bigquery2.DatasetReference{
							ProjectId: projectID,
							DatasetId: datasetID,
						},
					})
				} else {
					writeError(w, 404, fmt.Sprintf("Dataset %s:%s was not found", projectID, datasetID))
				}
			},
		},
		{
			fmt.Sprintf("/projects/%s/datasets/{datasetID}/tables", projectID),
			func(w http.ResponseWriter, r *http.Request, params map[string]string) {
				datasetID := params["datasetID"]
				tables := datasetTables[datasetID]
				tableEntries := make([]*bigquery2.TableListTables, 0, len(tables))
				for tbl := range tables {
					tableEntries = append(tableEntries, &bigquery2.TableListTables{
						TableReference: &bigquery2.TableReference{ProjectId: projectID, DatasetId: datasetID, TableId: tbl},
					})
				}
				writeJSON(w, &bigquery2.TableList{Tables: tableEntries})
			},
		},
		{
			fmt.Sprintf("/projects/%s/datasets/{datasetID}/tables/{tableID}", projectID),
			func(w http.ResponseWriter, r *http.Request, params map[string]string) {
				datasetID := params["datasetID"]
				tableID := params["tableID"]
				if tables, datasetExists := datasetTables[datasetID]; datasetExists && tables != nil {
					if cols, tableExists := tables[tableID]; tableExists {
						fields := make([]*bigquery2.TableFieldSchema, 0, len(cols))
						for _, c := range cols {
							fields = append(fields, &bigquery2.TableFieldSchema{Name: c, Type: "STRING", Mode: "NULLABLE"})
						}
						writeJSON(w, &bigquery2.Table{Schema: &bigquery2.TableSchema{Fields: fields}})
						return
					}
				}
				w.WriteHeader(http.StatusNotFound)
			},
		},
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		for _, endpoint := range endpoints {
			if params := matchPath(endpoint.pattern, r.URL.Path); params != nil {
				endpoint.handler(w, r, params)
				return
			}
		}

		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("no handler for " + r.Method + " " + r.URL.Path))
		if err != nil {
			t.Logf("failed to write error response: %v", err)
		}
	})
}

func matchPath(pattern, path string) map[string]string {
	patternParts := strings.Split(pattern, "/")
	pathParts := strings.Split(path, "/")

	if len(patternParts) != len(pathParts) {
		return nil
	}

	params := make(map[string]string)
	for i, part := range patternParts {
		if strings.HasPrefix(part, "{") && strings.HasSuffix(part, "}") {
			paramName := part[1 : len(part)-1]
			params[paramName] = pathParts[i]
		} else if part != pathParts[i] {
			return nil
		}
	}
	return params
}

func TestDB_Select(t *testing.T) {
	t.Parallel()

	projectID := testProjectID
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

	projectID := testProjectID
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
						assert.Len(t, table.Schema.Fields, len(tt.tableResponse.Schema.Fields))

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

			d := Client{
				client: client,
				config: &Config{
					ProjectID: projectID,
				},
			}

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

	projectID := testProjectID
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
				ColumnTypes: []string{"STRING", "STRING", "INTEGER"},
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

			d := Client{
				client: client,
				config: &Config{
					ProjectID: projectID,
				},
			}

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

func TestClient_GetDatabaseSummary(t *testing.T) {
	t.Parallel()

	projectID := testProjectID

	datasetTables := map[string]map[string][]string{
		"dataset1": {
			"table1": {"col1", "col2"},
			"table2": {"col1"},
		},
		"dataset2": {
			"table3": {"colA"},
		},
	}

	srv := httptest.NewServer(mockBqSummaryHandler(t, projectID, datasetTables))
	defer srv.Close()

	client, err := bigquery.NewClient(
		context.Background(),
		projectID,
		option.WithEndpoint(srv.URL),
		option.WithCredentials(&google.Credentials{
			ProjectID:   projectID,
			TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "token"}),
		}),
	)
	require.NoError(t, err)
	client.Location = "US"

	c := Client{client: client, config: &Config{ProjectID: projectID}}

	got, err := c.GetDatabaseSummary(context.Background())
	require.NoError(t, err)

	want := &ansisql.DBDatabase{
		Name: projectID,
		Schemas: []*ansisql.DBSchema{
			{
				Name: "dataset1",
				Tables: []*ansisql.DBTable{
					{
						Name: "table1",
						Columns: []*ansisql.DBColumn{
							{Name: "col1", Type: "STRING", Nullable: true},
							{Name: "col2", Type: "STRING", Nullable: true},
						},
					},
					{
						Name: "table2",
						Columns: []*ansisql.DBColumn{
							{Name: "col1", Type: "STRING", Nullable: true},
						},
					},
				},
			},
			{
				Name: "dataset2",
				Tables: []*ansisql.DBTable{
					{
						Name: "table3",
						Columns: []*ansisql.DBColumn{
							{Name: "colA", Type: "STRING", Nullable: true},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, want, got)
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
			name: "no partitioning in metadata but asset wants it",
			meta: &bigquery.TableMetadata{},
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					PartitionBy: "some_field",
				},
			},
			expected: false,
		},
		{
			name: "no partitioning in metadata and asset doesn't want it",
			meta: &bigquery.TableMetadata{},
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					PartitionBy: "",
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
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
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := IsSameClustering(tt.meta, tt.asset)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClient_IsMaterializationTypeMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		asset            *pipeline.Asset
		meta             *bigquery.TableMetadata
		expectedMismatch bool
	}{
		{
			name: "asset has no materialization type",
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeNone},
			},
			meta:             &bigquery.TableMetadata{Type: "TABLE"},
			expectedMismatch: false,
		},
		{
			name: "materialization type matches",
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{Type: "TABLE"},
			},
			meta:             &bigquery.TableMetadata{Type: "TABLE"},
			expectedMismatch: false,
		},
		{
			name: "materialization type mismatch",
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{Type: "VIEW"},
			},
			meta:             &bigquery.TableMetadata{Type: "TABLE"},
			expectedMismatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := Client{}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			mismatch := d.IsMaterializationTypeMismatch(ctx, tt.meta, tt.asset)
			assert.Equal(t, tt.expectedMismatch, mismatch)
		})
	}
}

func TestBuildTableExistsQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		client      *Client
		tableName   string
		wantQuery   string
		wantErr     bool
		errContains string
	}{
		{
			name:      "valid dataset.table format",
			client:    &Client{config: &Config{ProjectID: "test-project"}},
			tableName: "dataset.table",
			wantQuery: "SELECT COUNT(*) FROM `test-project.dataset.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'table'",
			wantErr:   false,
		},
		{
			name:      "valid project.dataset.table format",
			client:    &Client{config: &Config{ProjectID: "test-project"}},
			tableName: "other-project.dataset.table",
			wantQuery: "SELECT COUNT(*) FROM `other-project.dataset.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'table'",
			wantErr:   false,
		},
		{
			name:        "invalid empty component",
			client:      &Client{config: &Config{ProjectID: "test-project"}},
			tableName:   "dataset..table",
			wantErr:     true,
			errContains: "table name must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "invalid format - too few components",
			client:      &Client{config: &Config{ProjectID: "test-project"}},
			tableName:   "single",
			wantErr:     true,
			errContains: "table name must be in dataset.table or project.dataset.table format",
		},
		{
			name:        "invalid format - too many components",
			client:      &Client{config: &Config{ProjectID: "test-project"}},
			tableName:   "a.b.c.d",
			wantErr:     true,
			errContains: "table name must be in dataset.table or project.dataset.table format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotQuery, err := tt.client.BuildTableExistsQuery(tt.tableName)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantQuery, gotQuery)
		})
	}
}

func TestParseDateTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    interface{}
		wantTime *time.Time
		wantErr  bool
	}{
		{
			name:     "time.Time input",
			input:    time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC),
			wantTime: timePtr(time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "RFC3339 string",
			input:    "2023-01-15T10:30:00Z",
			wantTime: timePtr(time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "RFC3339 without timezone",
			input:    "2017-11-11T07:04:52",
			wantTime: timePtr(time.Date(2017, 11, 11, 7, 4, 52, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "RFC3339Nano string",
			input:    "2023-01-15T10:30:00.123456789Z",
			wantTime: timePtr(time.Date(2023, 1, 15, 10, 30, 0, 123456789, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "standard datetime string",
			input:    "2023-01-15 10:30:00",
			wantTime: timePtr(time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "datetime with microseconds",
			input:    "2023-01-15 10:30:00.123456",
			wantTime: timePtr(time.Date(2023, 1, 15, 10, 30, 0, 123456000, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "date only string",
			input:    "2023-01-15",
			wantTime: timePtr(time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "time only string",
			input:    "10:30:00",
			wantTime: timePtr(time.Date(0, 1, 1, 10, 30, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "actual bigquery example",
			input:    "2000-01-01 00:00:00 +0000 UTC",
			wantTime: timePtr(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "empty string",
			input:    "",
			wantTime: nil,
			wantErr:  true,
		},
		{
			name:     "invalid datetime string",
			input:    "invalid-datetime",
			wantTime: nil,
			wantErr:  true,
		},
		{
			name:     "nil input",
			input:    nil,
			wantTime: nil,
			wantErr:  true,
		},
		{
			name:     "int input",
			input:    12345,
			wantTime: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotTime, err := diff.ParseDateTime(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, gotTime)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, gotTime)
			assert.Equal(t, tt.wantTime, gotTime)
		})
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func mockGetTablesHandler(t *testing.T, projectID string, datasets map[string][]string) http.Handler {
	handleDatasetMetadata := func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasPrefix(r.URL.Path, fmt.Sprintf("/projects/%s/datasets/", projectID)) ||
			strings.HasSuffix(r.URL.Path, "/tables") {
			return false
		}

		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 5 {
			return false
		}

		datasetID := parts[4]
		if _, exists := datasets[datasetID]; !exists {
			w.WriteHeader(http.StatusNotFound)
			errorResp := map[string]interface{}{
				"error": map[string]interface{}{
					"code":    404,
					"message": fmt.Sprintf("Dataset %s:%s was not found", projectID, datasetID),
				},
			}
			resp, err := json.Marshal(errorResp)
			if err != nil {
				t.Logf("failed to marshal error response: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return true
			}
			_, err = w.Write(resp)
			if err != nil {
				t.Logf("failed to write error response: %v", err)
			}
			return true
		}

		dataset := &bigquery2.Dataset{
			DatasetReference: &bigquery2.DatasetReference{
				ProjectId: projectID,
				DatasetId: datasetID,
			},
		}
		resp, err := json.Marshal(dataset)
		if err != nil {
			t.Logf("failed to marshal dataset response: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return true
		}
		w.WriteHeader(http.StatusOK)
		_, err = w.Write(resp)
		if err != nil {
			t.Logf("failed to write dataset response: %v", err)
		}
		return true
	}

	handleTableListing := func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasPrefix(r.URL.Path, fmt.Sprintf("/projects/%s/datasets/", projectID)) ||
			!strings.HasSuffix(r.URL.Path, "/tables") {
			return false
		}

		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 6 {
			return false
		}

		datasetID := parts[4]
		tables, exists := datasets[datasetID]
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			return true
		}

		tableEntries := make([]*bigquery2.TableListTables, 0, len(tables))
		for _, tableID := range tables {
			tableEntries = append(tableEntries, &bigquery2.TableListTables{
				TableReference: &bigquery2.TableReference{
					ProjectId: projectID,
					DatasetId: datasetID,
					TableId:   tableID,
				},
			})
		}

		tableList := &bigquery2.TableList{Tables: tableEntries}
		resp, err := json.Marshal(tableList)
		if err != nil {
			t.Logf("failed to marshal table list response: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return true
		}
		w.WriteHeader(http.StatusOK)
		_, err = w.Write(resp)
		if err != nil {
			t.Logf("failed to write table list response: %v", err)
		}
		return true
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if handleDatasetMetadata(w, r) {
			return
		}

		if handleTableListing(w, r) {
			return
		}

		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("no handler for " + r.Method + " " + r.URL.Path))
		if err != nil {
			t.Logf("failed to write error response: %v", err)
		}
	})
}

func TestClient_GetTables(t *testing.T) {
	t.Parallel()

	projectID := testProjectID

	tests := []struct {
		name         string
		databaseName string
		datasets     map[string][]string
		want         []string
		wantErr      bool
		errContains  string
	}{
		{
			name:         "empty database name",
			databaseName: "",
			want:         nil,
			wantErr:      true,
			errContains:  "database name cannot be empty",
		},
		{
			name:         "non-existent dataset",
			databaseName: "nonexistent",
			datasets: map[string][]string{
				"dataset1": {"table1"},
			},
			want:        nil,
			wantErr:     true,
			errContains: "dataset 'nonexistent' does not exist",
		},
		{
			name:         "dataset exists but has no tables",
			databaseName: "empty_dataset",
			datasets: map[string][]string{
				"empty_dataset": {},
			},
			want:    nil,
			wantErr: false,
		},
		{
			name:         "dataset exists with single table",
			databaseName: "dataset1",
			datasets: map[string][]string{
				"dataset1": {"table1"},
			},
			want:    []string{"table1"},
			wantErr: false,
		},
		{
			name:         "dataset exists with multiple tables (should be sorted)",
			databaseName: "dataset2",
			datasets: map[string][]string{
				"dataset2": {"zebra_table", "alpha_table", "beta_table"},
			},
			want:    []string{"alpha_table", "beta_table", "zebra_table"},
			wantErr: false,
		},
	}

	// Run regular tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(mockGetTablesHandler(t, projectID, tt.datasets))
			defer srv.Close()

			client, err := bigquery.NewClient(
				context.Background(),
				projectID,
				option.WithEndpoint(srv.URL),
				option.WithCredentials(&google.Credentials{
					ProjectID:   projectID,
					TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "token"}),
				}),
			)
			require.NoError(t, err)
			client.Location = "US"

			c := Client{client: client, config: &Config{ProjectID: projectID}}

			got, err := c.GetTables(context.Background(), tt.databaseName)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Nil(t, got)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
