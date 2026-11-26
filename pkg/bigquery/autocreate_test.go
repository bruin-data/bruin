package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	bigquery2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

var _ autocreate.RelationChecker = (*Client)(nil)

type fakeTable struct {
	kind    string
	iceberg bool
}

type fakeDataset struct {
	caseInsensitive bool
	tables          map[string]*fakeTable
}

// fakeBigQuery serves the BigQuery API calls that RelationExists and a run of a
// table asset make, against datasets it keeps in memory. Like BigQuery's API,
// it compares names exactly.
type fakeBigQuery struct {
	mu sync.Mutex
	// projects maps project IDs to their datasets by ID.
	projects map[string]map[string]*fakeDataset
	// failures answers a "METHOD /path" request with an error of the given status.
	failures map[string]int
	// target is the table that queries other than CREATE OR REPLACE TABLE load into,
	// so they fail like on BigQuery while it does not exist.
	target string

	requests   []string
	tableViews []string
	queries    []string
}

var createOrReplaceTable = regexp.MustCompile("CREATE OR REPLACE TABLE\\s+`?([\\w.-]+)`?")

func (f *fakeBigQuery) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /projects/{project}/datasets", f.listDatasets)
	mux.HandleFunc("POST /projects/{project}/datasets", f.createDataset)
	mux.HandleFunc("GET /projects/{project}/datasets/{dataset}", f.getDataset)
	mux.HandleFunc("GET /projects/{project}/datasets/{dataset}/tables", f.listTables)
	mux.HandleFunc("GET /projects/{project}/datasets/{dataset}/tables/{table}", f.getTable)
	mux.HandleFunc("POST /projects/{project}/jobs", f.insertJob)
	mux.HandleFunc("GET /projects/{project}/jobs/{job}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, fakeJob(r.PathValue("project"), r.PathValue("job"), ""))
	})
	mux.HandleFunc("GET /projects/{project}/queries/{job}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, &bigquery2.GetQueryResultsResponse{
			JobReference: &bigquery2.JobReference{ProjectId: r.PathValue("project"), JobId: r.PathValue("job"), Location: "US"},
			JobComplete:  true,
		})
	})
	// Unknown requests must not look like a missing table or dataset.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeAPIError(w, http.StatusInternalServerError, "unexpected request: "+r.Method+" "+r.URL.Path)
	})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()

		request := r.Method + " " + r.URL.Path
		f.requests = append(f.requests, request)
		if status, ok := f.failures[request]; ok {
			writeAPIError(w, status, "injected failure")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (f *fakeBigQuery) listDatasets(w http.ResponseWriter, r *http.Request) {
	project := r.PathValue("project")
	datasets, ok := f.projects[project]
	if !ok {
		writeAPIError(w, http.StatusNotFound, "Not found: Project "+project)
		return
	}

	ids := make([]string, 0, len(datasets))
	for id := range datasets {
		if strings.HasPrefix(id, "_") && r.URL.Query().Get("all") != "true" {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	list := &bigquery2.DatasetList{}
	for _, id := range ids {
		list.Datasets = append(list.Datasets, &bigquery2.DatasetListDatasets{
			DatasetReference: &bigquery2.DatasetReference{ProjectId: project, DatasetId: id},
		})
	}
	writeJSON(w, http.StatusOK, list)
}

func (f *fakeBigQuery) createDataset(w http.ResponseWriter, r *http.Request) {
	var body bigquery2.Dataset
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}

	project := r.PathValue("project")
	if f.projects[project] == nil {
		f.projects[project] = map[string]*fakeDataset{}
	}
	f.projects[project][body.DatasetReference.DatasetId] = &fakeDataset{tables: map[string]*fakeTable{}}
	writeJSON(w, http.StatusOK, &body)
}

func (f *fakeBigQuery) getDataset(w http.ResponseWriter, r *http.Request) {
	project, datasetID := r.PathValue("project"), r.PathValue("dataset")
	dataset, ok := f.projects[project][datasetID]
	if !ok {
		writeAPIError(w, http.StatusNotFound, "Not found: Dataset "+project+":"+datasetID)
		return
	}

	writeJSON(w, http.StatusOK, &bigquery2.Dataset{
		DatasetReference:  &bigquery2.DatasetReference{ProjectId: project, DatasetId: datasetID},
		IsCaseInsensitive: dataset.caseInsensitive,
	})
}

func (f *fakeBigQuery) listTables(w http.ResponseWriter, r *http.Request) {
	project, datasetID := r.PathValue("project"), r.PathValue("dataset")
	dataset, ok := f.projects[project][datasetID]
	if !ok {
		writeAPIError(w, http.StatusNotFound, "Not found: Dataset "+project+":"+datasetID)
		return
	}

	list := &bigquery2.TableList{}
	for id, table := range dataset.tables {
		list.Tables = append(list.Tables, &bigquery2.TableListTables{
			TableReference: &bigquery2.TableReference{ProjectId: project, DatasetId: datasetID, TableId: id},
			Type:           table.kind,
		})
	}
	writeJSON(w, http.StatusOK, list)
}

func (f *fakeBigQuery) getTable(w http.ResponseWriter, r *http.Request) {
	f.tableViews = append(f.tableViews, r.URL.Query().Get("view"))

	project, datasetID, tableID := r.PathValue("project"), r.PathValue("dataset"), r.PathValue("table")
	table := f.table(project, datasetID, tableID)
	if table == nil {
		writeAPIError(w, http.StatusNotFound, "Not found: Table "+project+":"+datasetID+"."+tableID)
		return
	}

	response := &bigquery2.Table{
		TableReference: &bigquery2.TableReference{ProjectId: project, DatasetId: datasetID, TableId: tableID},
		Type:           table.kind,
	}
	if table.iceberg {
		response.BiglakeConfiguration = &bigquery2.BigLakeConfiguration{TableFormat: "ICEBERG", FileFormat: "PARQUET"}
	}
	writeJSON(w, http.StatusOK, response)
}

func (f *fakeBigQuery) insertJob(w http.ResponseWriter, r *http.Request) {
	var body bigquery2.Job
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}

	sql := body.Configuration.Query.Query
	f.queries = append(f.queries, sql)
	if status, message := f.runQuery(sql); status != http.StatusOK {
		writeAPIError(w, status, message)
		return
	}
	writeJSON(w, http.StatusOK, fakeJob(r.PathValue("project"), body.JobReference.JobId, sql))
}

func (f *fakeBigQuery) table(project, dataset, table string) *fakeTable {
	if ds, ok := f.projects[project][dataset]; ok {
		return ds.tables[table]
	}
	return nil
}

// runQuery applies a CREATE OR REPLACE TABLE statement and fails statements that
// load into a missing target table.
func (f *fakeBigQuery) runQuery(sql string) (int, string) {
	if match := createOrReplaceTable.FindStringSubmatch(sql); match != nil {
		parts := strings.Split(match[1], ".")
		project, dataset, table := testProjectID, parts[0], parts[1]
		if len(parts) == 3 {
			project, dataset, table = parts[0], parts[1], parts[2]
		}
		ds, ok := f.projects[project][dataset]
		if !ok {
			return http.StatusNotFound, "Not found: Dataset " + project + ":" + dataset
		}
		ds.tables[table] = &fakeTable{kind: "TABLE", iceberg: strings.Contains(sql, "table_format = 'ICEBERG'")}
		return http.StatusOK, ""
	}

	if f.target != "" {
		parts := strings.Split(f.target, ".")
		if f.table(testProjectID, parts[0], parts[1]) == nil {
			return http.StatusNotFound, "Not found: Table " + testProjectID + ":" + f.target
		}
	}
	return http.StatusOK, ""
}

func (f *fakeBigQuery) snapshot() (requests, tableViews, queries []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...), append([]string(nil), f.tableViews...), append([]string(nil), f.queries...)
}

func fakeJob(project, id, sql string) *bigquery2.Job {
	return &bigquery2.Job{
		JobReference:  &bigquery2.JobReference{ProjectId: project, JobId: id, Location: "US"},
		Configuration: &bigquery2.JobConfiguration{Query: &bigquery2.JobConfigurationQuery{Query: sql}},
		Status:        &bigquery2.JobStatus{State: "DONE"},
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func writeAPIError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{
		"code": status, "message": message, "errors": []map[string]any{{"reason": "test", "message": message}},
	}})
}

func newFakeBigQueryClient(t *testing.T, fake *fakeBigQuery, projectID string) *Client {
	t.Helper()

	server := httptest.NewServer(fake.handler())
	t.Cleanup(server.Close)

	client, err := bigquery.NewClient(
		t.Context(),
		testProjectID,
		option.WithEndpoint(server.URL),
		option.WithCredentials(&google.Credentials{
			ProjectID:   testProjectID,
			TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "some-token"}),
		}),
	)
	require.NoError(t, err)
	client.Location = "US"

	return &Client{client: client, config: &Config{ProjectID: projectID}}
}

func fakeTables(kinds map[string]string) map[string]*fakeTable {
	result := make(map[string]*fakeTable, len(kinds))
	for name, kind := range kinds {
		result[name] = &fakeTable{kind: kind}
	}
	return result
}

func relationFixture() map[string]map[string]*fakeDataset {
	return map[string]map[string]*fakeDataset{
		testProjectID: {
			"analytics": {tables: fakeTables(map[string]string{
				"orders":          "TABLE",
				"orders_view":     "VIEW",
				"orders_mv":       "MATERIALIZED_VIEW",
				"orders_external": "EXTERNAL",
				"orders_snapshot": "SNAPSHOT",
				"Customers":       "TABLE",
			})},
			"Marts":    {caseInsensitive: true, tables: fakeTables(map[string]string{"Revenue": "TABLE"})},
			"_Staging": {caseInsensitive: true, tables: fakeTables(map[string]string{"Events": "TABLE"})},
			"Raw":      {tables: fakeTables(map[string]string{"events": "TABLE"})},
		},
		"other-project": {
			"sales": {tables: fakeTables(map[string]string{"deals": "TABLE"})},
		},
	}
}

func TestClient_RelationExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table string
		want  bool
	}{
		{name: "dataset.table in the connection's project", table: "analytics.orders", want: true},
		{name: "project.dataset.table", table: "other-project.sales.deals", want: true},
		{name: "the connection's project spelled out", table: "test-project.analytics.orders", want: true},
		{name: "project IDs are lower-case", table: "Other-Project.sales.deals", want: true},
		{name: "view", table: "analytics.orders_view", want: true},
		{name: "materialized view", table: "analytics.orders_mv", want: true},
		{name: "external table", table: "analytics.orders_external", want: true},
		{name: "snapshot", table: "analytics.orders_snapshot", want: true},
		{name: "missing table", table: "analytics.missing", want: false},
		{name: "table in another project", table: "sales.deals", want: false},
		{name: "case-sensitive dataset keeps names apart", table: "analytics.customers", want: false},
		{name: "case-insensitive dataset matches another case", table: "Marts.revenue", want: true},
		{name: "missing table in a case-insensitive dataset", table: "Marts.costs", want: false},
		{name: "case-insensitive dataset name in another case", table: "marts.REVENUE", want: true},
		{name: "missing table in a case-insensitive dataset named in another case", table: "marts.costs", want: false},
		{name: "hidden case-insensitive dataset", table: "_staging.events", want: true},
		{name: "case-sensitive dataset named in another case", table: "raw.events", want: false},
		{name: "missing dataset", table: "nowhere.orders", want: false},
		{name: "missing dataset in another project", table: "other-project.nowhere.deals", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeBigQuery{projects: relationFixture()}
			client := newFakeBigQueryClient(t, fake, testProjectID)

			got, err := client.RelationExists(t.Context(), tt.table)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			requests, tableViews, queries := fake.snapshot()
			assert.Empty(t, queries, "the check must not run billed queries")
			assert.Equal(t, []string{"BASIC"}, tableViews)
			for _, request := range requests {
				assert.True(t, strings.HasPrefix(request, http.MethodGet+" "), "the check must only read metadata, got %s", request)
			}
		})
	}
}

func TestClient_RelationExistsErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		table    string
		failures map[string]int
		wantErr  string
	}{
		{
			name:     "table lookup fails",
			table:    "analytics.orders",
			failures: map[string]int{"GET /projects/test-project/datasets/analytics/tables/orders": http.StatusForbidden},
			wantErr:  "failed to check whether table 'analytics.orders' exists",
		},
		{
			name:     "dataset lookup fails",
			table:    "analytics.missing",
			failures: map[string]int{"GET /projects/test-project/datasets/analytics": http.StatusForbidden},
			wantErr:  "failed to check whether dataset 'test-project.analytics' exists",
		},
		{
			name:     "listing a case-insensitive dataset fails",
			table:    "Marts.revenue",
			failures: map[string]int{"GET /projects/test-project/datasets/Marts/tables": http.StatusForbidden},
			wantErr:  "failed to list the tables in dataset 'test-project.Marts'",
		},
		{
			name:     "listing datasets fails",
			table:    "nowhere.orders",
			failures: map[string]int{"GET /projects/test-project/datasets": http.StatusForbidden},
			wantErr:  "failed to list the datasets in project 'test-project'",
		},
		{
			name:     "reading a dataset named in another case fails",
			table:    "marts.revenue",
			failures: map[string]int{"GET /projects/test-project/datasets/Marts": http.StatusForbidden},
			wantErr:  "failed to read dataset 'test-project.Marts'",
		},
		{
			name:    "missing project",
			table:   "missing-project.analytics.orders",
			wantErr: "failed to list the datasets in project 'missing-project'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeBigQuery{projects: relationFixture(), failures: tt.failures}
			client := newFakeBigQueryClient(t, fake, testProjectID)

			_, err := client.RelationExists(t.Context(), tt.table)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestClient_RelationExistsInvalidNames(t *testing.T) {
	t.Parallel()

	fake := &fakeBigQuery{projects: relationFixture()}
	client := newFakeBigQueryClient(t, fake, testProjectID)

	for _, name := range []string{"", "orders", "a.b.c.d", "analytics..orders", ".analytics.orders", "analytics.orders.", "`analytics.orders`", "analytics.`orders`", "analytics. orders", " analytics.orders"} {
		_, err := client.RelationExists(t.Context(), name)
		require.ErrorContains(t, err, "must be in dataset.table or project.dataset.table format", name)
	}

	withoutProject := newFakeBigQueryClient(t, fake, "")
	_, err := withoutProject.RelationExists(t.Context(), "analytics.orders")
	require.ErrorContains(t, err, "the connection has no project_id")

	got, err := withoutProject.RelationExists(t.Context(), "test-project.analytics.orders")
	require.NoError(t, err)
	assert.True(t, got)

	requests, _, _ := fake.snapshot()
	assert.Len(t, requests, 1, "invalid names must not reach the API")
}

type fakeConnections map[string]any

func (c fakeConnections) GetConnection(name string) any {
	return c[name]
}

// TestAutoCreateFirstRun runs incremental assets twice through the wrapped
// operator: the first run has to create the missing table, the second has to
// load into it.
func TestAutoCreateFirstRun(t *testing.T) {
	t.Parallel()

	columns := []pipeline.Column{
		{Name: "id", Type: "INT64", PrimaryKey: true},
		{Name: "label", Type: "STRING", UpdateOnMerge: true},
		{Name: "updated_at", Type: "TIMESTAMP"},
	}
	enabled := true

	tests := []struct {
		name              string
		materialization   pipeline.Materialization
		bigQuery          pipeline.BigQueryConfig
		refreshRestricted bool
		firstRun          []string
		secondRun         string
	}{
		{
			name:            "append",
			materialization: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyAppend},
			firstRun:        []string{"CREATE OR REPLACE TABLE autocreate_append.items"},
			secondRun:       "INSERT INTO autocreate_append.items",
		},
		{
			name:            "merge",
			materialization: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyMerge},
			firstRun:        []string{"CREATE OR REPLACE TABLE autocreate_merge.items"},
			secondRun:       "MERGE autocreate_merge.items target",
		},
		{
			name:            "delete_insert",
			materialization: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyDeleteInsert, IncrementalKey: "id"},
			firstRun:        []string{"CREATE OR REPLACE TABLE autocreate_delete_insert.items"},
			secondRun:       "DELETE FROM autocreate_delete_insert.items WHERE id in unnest(",
		},
		{
			name:            "time_interval",
			materialization: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyTimeInterval, IncrementalKey: "updated_at", TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp},
			firstRun:        []string{"CREATE OR REPLACE TABLE autocreate_time_interval.items"},
			secondRun:       "DELETE FROM autocreate_time_interval.items WHERE updated_at BETWEEN '2024-01-01",
		},
		{
			name:            "truncate_insert",
			materialization: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyTruncateInsert},
			firstRun:        []string{"CREATE OR REPLACE TABLE autocreate_truncate_insert.items"},
			secondRun:       "TRUNCATE TABLE autocreate_truncate_insert.items",
		},
		{
			name:            "scd2_by_column",
			materialization: pipeline.Materialization{Strategy: pipeline.MaterializationStrategySCD2ByColumn},
			firstRun:        []string{"CREATE OR REPLACE TABLE `autocreate_scd2_by_column.items`", "PARTITION BY DATE(_valid_from)", "CLUSTER BY _is_current, id"},
			secondRun:       "MERGE INTO `autocreate_scd2_by_column.items` AS target",
		},
		{
			name:            "scd2_by_time",
			materialization: pipeline.Materialization{Strategy: pipeline.MaterializationStrategySCD2ByTime, IncrementalKey: "updated_at"},
			firstRun:        []string{"CREATE OR REPLACE TABLE `autocreate_scd2_by_time.items`", "CAST (updated_at AS TIMESTAMP) AS _valid_from"},
			secondRun:       "MERGE INTO `autocreate_scd2_by_time.items` AS target",
		},
		{
			name: "partitioned_merge",
			materialization: pipeline.Materialization{
				Strategy:    pipeline.MaterializationStrategyMerge,
				PartitionBy: "DATE(updated_at)",
				ClusterBy:   []string{"id"},
			},
			bigQuery:  pipeline.BigQueryConfig{RequirePartitionFilter: &enabled, PartitionKeyImmutable: &enabled},
			firstRun:  []string{"CREATE OR REPLACE TABLE autocreate_partitioned_merge.items PARTITION BY DATE(updated_at) CLUSTER BY id OPTIONS (require_partition_filter = TRUE)"},
			secondRun: "MERGE autocreate_partitioned_merge.items target\nUSING __bruin_merge_source_",
		},
		{
			name:            "iceberg_merge",
			materialization: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyMerge, TableFormat: pipeline.MaterializationTableFormatIceberg},
			bigQuery:        pipeline.BigQueryConfig{StorageURI: "gs://bucket/items"},
			firstRun:        []string{"CREATE OR REPLACE TABLE autocreate_iceberg_merge.items", "table_format = 'ICEBERG'"},
			secondRun:       "MERGE autocreate_iceberg_merge.items target",
		},
		{
			name:              "refresh_restricted",
			materialization:   pipeline.Materialization{Strategy: pipeline.MaterializationStrategyAppend},
			refreshRestricted: true,
			firstRun:          []string{"CREATE OR REPLACE TABLE autocreate_refresh_restricted.items"},
			secondRun:         "INSERT INTO autocreate_refresh_restricted.items",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// The dataset is missing, so the first run also has to create it.
			dataset := "autocreate_" + tt.name
			datasetNameCache.Delete(testProjectID + "." + dataset)
			name := dataset + ".items"
			fake := &fakeBigQuery{projects: map[string]map[string]*fakeDataset{testProjectID: {}}, target: name}
			client := newFakeBigQueryClient(t, fake, testProjectID)

			mat := tt.materialization
			mat.Type = pipeline.MaterializationTypeTable
			asset := &pipeline.Asset{
				Name:              name,
				Type:              pipeline.AssetTypeBigqueryQuery,
				Connection:        "bq",
				Materialization:   mat,
				BigQuery:          tt.bigQuery,
				RefreshRestricted: &tt.refreshRestricted,
				Columns:           columns,
				ExecutableFile: pipeline.ExecutableFile{
					Content: "SELECT 1 AS id, 'a' AS label, TIMESTAMP '2024-01-01 10:00:00' AS updated_at",
				},
			}

			out := runAutoCreate(t, client, asset)
			assert.Contains(t, out, "Table '"+name+"' does not exist yet, creating it from the query.")
			_, _, queries := fake.snapshot()
			require.Len(t, queries, 1)
			for _, want := range tt.firstRun {
				assert.Contains(t, queries[0], want)
			}

			out = runAutoCreate(t, client, asset)
			assert.NotContains(t, out, "does not exist yet")
			_, _, queries = fake.snapshot()
			require.Len(t, queries, 2)
			assert.NotContains(t, queries[1], "CREATE OR REPLACE TABLE")
			assert.Contains(t, queries[1], tt.secondRun)
		})
	}
}

func runAutoCreate(t *testing.T, client *Client, asset *pipeline.Asset) string {
	t.Helper()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	var out bytes.Buffer
	ctx := context.WithValue(t.Context(), executor.KeyPrinter, &out)
	ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, start)
	ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, end)
	ctx = context.WithValue(ctx, pipeline.RunConfigExecutionDate, start)
	ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "run")
	ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, false)

	connections := fakeConnections{"bq": client}
	extractor := &query.WholeFileExtractor{
		Fs:       afero.NewMemMapFs(),
		Renderer: jinja.NewRendererWithStartEndDates(&start, &end, &start, "p", "run", nil),
	}
	op := autocreate.Wrap(NewBasicOperator(connections, extractor, NewMaterializer(false), nil), connections)

	ti := &scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{Name: "p"}, Asset: asset}
	require.NoError(t, op.Run(ctx, ti))

	return out.String()
}
