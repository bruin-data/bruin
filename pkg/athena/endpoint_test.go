package athena

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	// hangStatus makes GetQueryExecution wait until the client gives up on the request.
	hangStatus bool
}

func (f *fakeAthena) ops() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.operation)
}

func (f *fakeAthena) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	op := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AmazonAthena.")

	f.mu.Lock()
	f.operation = append(f.operation, op)
	hang := f.hangStatus && op == "GetQueryExecution"
	f.mu.Unlock()
	if hang {
		<-r.Context().Done()
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

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
	case "StopQueryExecution":
		resp = map[string]any{}
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

func TestDB_CustomEndpointStopsQueryOnCancel(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		fake *fakeAthena
	}{
		// ctx ends while waiting for the next poll.
		{name: "between polls", fake: &fakeAthena{state: "RUNNING"}},
		// ctx ends while a GetQueryExecution request is in flight.
		{name: "during status request", fake: &fakeAthena{hangStatus: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := newEndpointTestDB(t, tt.fake)
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()

			err := db.RunQueryWithoutResult(ctx, &query.Query{Query: "SELECT 1"})
			require.ErrorContains(t, err, context.DeadlineExceeded.Error())
			require.Contains(t, tt.fake.ops(), "StopQueryExecution")
		})
	}
}

func TestDB_CustomEndpointRejectsIngestr(t *testing.T) {
	t.Parallel()

	db := NewDB(&Config{Endpoint: "https://athena.example.com"})
	_, err := db.GetIngestrURI()
	require.ErrorContains(t, err, "endpoint_url is not supported for ingestr")
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
		{ip: "::ffff:127.0.0.1", plainHTTP: true, allowed: true},
		{ip: "10.0.0.5", allowed: true},
		{ip: "172.16.4.2", allowed: true},
		{ip: "192.168.1.10", allowed: true},
		{ip: "fd12:3456::1", allowed: true},
		{ip: "52.94.236.248", allowed: true},
		{ip: "2600:1f18::1", allowed: true},
		{ip: "10.0.0.5", plainHTTP: true},
		{ip: "52.94.236.248", plainHTTP: true},
		{ip: "169.254.169.254"},        // AWS, GCP, Azure metadata
		{ip: "::ffff:169.254.169.254"}, // IPv4-mapped form
		{ip: "fd00:ec2::254"},          // AWS IMDS over IPv6, inside the allowed fc00::/7
		{ip: "192.0.0.192"},            // Oracle Cloud metadata
		{ip: "100.100.100.200"},        // Alibaba Cloud metadata
		{ip: "100.64.0.1"},             // shared address space
		{ip: "fe80::1"},
		{ip: "0.0.0.0"},
		{ip: "0.1.2.3"},
		{ip: "224.0.0.1"},
		{ip: "240.0.0.1"},
		{ip: "255.255.255.255"},
		{ip: "198.18.0.1"},
		{ip: "64:ff9b::a9fe:a9fe"}, // NAT64 form of 169.254.169.254
	} {
		err := checkEndpointIP(netip.MustParseAddr(tt.ip), tt.plainHTTP)
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

func TestEndpointHTTPClientRejectsPlainHTTPOutsideLoopbackAtDial(t *testing.T) {
	t.Parallel()

	// A public address: the dial check fails before any packet is sent. The deadline makes a
	// regression fail fast instead of waiting for a real connect timeout.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://1.1.1.1/", nil)
	require.NoError(t, err)

	resp, err := newEndpointHTTPClient(true).Do(req)
	if resp != nil {
		require.NoError(t, resp.Body.Close())
	}
	require.ErrorContains(t, err, "http:// is only allowed for loopback addresses")
}
