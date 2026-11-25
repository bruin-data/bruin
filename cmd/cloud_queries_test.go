package cmd

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cloudQueriesPage = `{"queries":[
 {"message_pair_id":81,"agent":{"id":3,"name":"Sales"},"thread":{"id":7,"title":"Findings: Improvements","type":"scheduled_execution"},"scheduled_run":{"id":5,"title":"Findings: Improvements","kind":"scout"},"started_by":null,"query":"select id\n  from orders","connection":"warehouse","success":true,"timestamp":"2026-11-25T09:00:04+00:00","rows":[[9007199254740993]],"row_count":1},
 {"message_pair_id":80,"agent":{"id":4,"name":"Finance"},"thread":{"id":9,"title":"Refunds last week","type":"chat"},"scheduled_run":null,"started_by":{"id":2,"name":"Dana Reyes"},"query":"select sum(amount) from refunds","connection":"warehouse","success":false,"error":"boom","timestamp":"2026-11-25T08:00:02+00:00"}
],"next_before":80}`

func serveCloudQueries(t *testing.T, check func(*http.Request)) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/queries", r.URL.Path)
		if check != nil {
			check(r)
		}
		_, _ = io.WriteString(w, cloudQueriesPage)
	}))
	t.Cleanup(server.Close)
	t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudQueriesListTable(t *testing.T) {
	serveCloudQueries(t, func(r *http.Request) {
		assert.Equal(t, "3", r.URL.Query().Get("agent_id"))
		assert.Equal(t, "2026-11-01", r.URL.Query().Get("since"))
	})

	data, err := runCloudCLI(t, "queries", "list", "--api-key", "k", "--agent-id", "3", "--since", "2026-11-01")
	require.NoError(t, err)
	assert.Contains(t, data, "Findings: Improvements")
	assert.Contains(t, data, "Refunds last week")
	assert.Contains(t, data, "Dana Reyes")
	assert.Contains(t, data, "select id from orders")
	assert.Contains(t, data, "More: --before 80")
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudQueriesListJSONKeepsLargeIntegers(t *testing.T) {
	serveCloudQueries(t, nil)

	data, err := runCloudCLI(t, "queries", "list", "--api-key", "k", "--output", "json")
	require.NoError(t, err)
	var page struct {
		Queries []struct {
			Rows [][]json.RawMessage `json:"rows"`
		} `json:"queries"`
		NextBefore int `json:"next_before"`
	}
	require.NoError(t, json.Unmarshal([]byte(data), &page))
	assert.Equal(t, "9007199254740993", string(page.Queries[0].Rows[0][0]))
	assert.Equal(t, 80, page.NextBefore)
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudQueriesListRejectsNonPositiveFilters(t *testing.T) {
	for _, flag := range []string{"--agent-id", "--thread-id", "--limit", "--before"} {
		t.Run(flag, func(t *testing.T) {
			serveCloudQueries(t, func(*http.Request) { t.Error("no request should be made") })

			data, err := runCloudCLI(t, "queries", "list", "--api-key", "k", flag, "0")
			require.Error(t, err)
			assert.Contains(t, data, "must be a positive integer")
		})
	}
}
