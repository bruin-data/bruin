package athena

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/require"
)

// fakeAthena serves the subset of the Athena JSON API used by the endpoint connector.
type fakeAthena struct {
	mu        sync.Mutex
	queries   []string
	state     string
	reason    string
	operation []string
}

func (f *fakeAthena) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	op := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AmazonAthena.")
	f.operation = append(f.operation, op)

	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	var resp any
	switch op {
	case "StartQueryExecution":
		f.queries = append(f.queries, body["QueryString"].(string))
		resp = map[string]any{"QueryExecutionId": "qid-1"}
	case "GetQueryExecution":
		resp = map[string]any{"QueryExecution": map[string]any{
			"QueryExecutionId": "qid-1",
			"StatementType":    "DML",
			"Status":           map[string]any{"State": f.state, "StateChangeReason": f.reason},
		}}
	case "GetQueryResults":
		resp = map[string]any{"ResultSet": map[string]any{
			"ResultSetMetadata": map[string]any{"ColumnInfo": []map[string]any{
				{"Name": "id", "Type": "integer"},
				{"Name": "name", "Type": "varchar"},
			}},
			"Rows": []map[string]any{
				{"Data": []map[string]any{{"VarCharValue": "id"}, {"VarCharValue": "name"}}},
				{"Data": []map[string]any{{"VarCharValue": "1"}, {"VarCharValue": "bruin"}}},
			},
		}}
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func newEndpointTestDB(t *testing.T, fake *fakeAthena) *DB {
	t.Helper()
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)

	return NewDB(&Config{
		OutputBucket:    "s3://results/path",
		Region:          "us-east-1",
		AccessID:        "test",
		SecretAccessKey: "test",
		Database:        "some_db",
		Endpoint:        server.URL,
	})
}

func TestDB_CustomEndpointSelect(t *testing.T) {
	t.Parallel()

	fake := &fakeAthena{state: "SUCCEEDED"}
	db := newEndpointTestDB(t, fake)

	result, err := db.Select(t.Context(), &query.Query{Query: "SELECT id, name FROM t"})
	require.NoError(t, err)
	require.Equal(t, [][]any{{int32(1), "bruin"}}, result)
	require.Equal(t, []string{"SELECT id, name FROM t"}, fake.queries)
	require.Equal(t, []string{"StartQueryExecution", "GetQueryExecution", "GetQueryResults"}, fake.operation)
}

func TestDB_CustomEndpointQueryFailure(t *testing.T) {
	t.Parallel()

	fake := &fakeAthena{state: "FAILED", reason: "TABLE_NOT_FOUND: t"}
	db := newEndpointTestDB(t, fake)

	err := db.RunQueryWithoutResult(t.Context(), &query.Query{Query: "SELECT * FROM t"})
	require.EqualError(t, err, "TABLE_NOT_FOUND: t")
}

func TestParseEndpointURL(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"https://athena.example.com",
		"https://athena.example.com:8443/",
		"http://localhost:4566",
	} {
		_, err := parseEndpointURL(raw)
		require.NoError(t, err, raw)
	}

	for _, raw := range []string{
		"athena.example.com",
		"ftp://athena.example.com",
		"file:///etc/passwd",
		"https://",
		"https://user:pass@athena.example.com",
		"https://athena.example.com/path",
		"https://athena.example.com?x=1",
		"https://athena.example.com#frag",
	} {
		_, err := parseEndpointURL(raw)
		require.Error(t, err, raw)
	}
}

func TestCheckEndpointIP(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		ip        string
		plainHTTP bool
		allowed   bool
	}{
		{ip: "127.0.0.1", plainHTTP: true, allowed: true},
		{ip: "::1", plainHTTP: true, allowed: true},
		{ip: "10.0.0.5", allowed: true},
		{ip: "203.0.113.10", allowed: true},
		{ip: "10.0.0.5", plainHTTP: true},
		{ip: "203.0.113.10", plainHTTP: true},
		{ip: "169.254.169.254"},
		{ip: "::ffff:169.254.169.254"},
		{ip: "fd00:ec2::254"},
		{ip: "fe80::1"},
		{ip: "100.100.100.200"},
		{ip: "0.0.0.0"},
		{ip: "224.0.0.1"},
	} {
		err := checkEndpointIP(net.ParseIP(tt.ip), tt.plainHTTP)
		if tt.allowed {
			require.NoError(t, err, tt.ip)
		} else {
			require.Error(t, err, tt.ip)
		}
	}
}

func TestDB_CustomEndpointRequiresStaticCredentials(t *testing.T) {
	t.Parallel()

	db := NewDB(&Config{
		OutputBucket: "s3://results/path",
		Region:       "us-east-1",
		Endpoint:     "https://athena.example.com",
	})

	err := db.Ping(t.Context())
	require.ErrorContains(t, err, "endpoint_url requires access_key_id and secret_access_key")
}

func TestDB_CustomEndpointDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()

	var redirectTargetHit atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectTargetHit.Store(true)
	}))
	t.Cleanup(target.Close)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)

	db := NewDB(&Config{
		OutputBucket:    "s3://results/path",
		Region:          "us-east-1",
		AccessID:        "test",
		SecretAccessKey: "test",
		Endpoint:        redirector.URL,
	})

	require.Error(t, db.RunQueryWithoutResult(t.Context(), &query.Query{Query: "SELECT 1"}))
	require.False(t, redirectTargetHit.Load())
}

func TestEndpointHTTPClientRejectsBlockedAddressAtDial(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(server.Close)

	// Plain http to loopback is allowed; a metadata address is refused before any bytes are sent.
	get := func(client *http.Client, url string) error {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		return resp.Body.Close()
	}

	require.NoError(t, get(newEndpointHTTPClient(true), server.URL))
	require.ErrorContains(t, get(newEndpointHTTPClient(false), "http://169.254.169.254/latest/meta-data/"), "not allowed")
}
