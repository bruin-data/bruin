package bigquery

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/google"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

func TestReadOnlyBigQuery(t *testing.T) {
	t.Parallel()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	for _, fromFile := range []bool{false, true} {
		t.Run(fmt.Sprintf("file=%t", fromFile), func(t *testing.T) {
			t.Parallel()
			var tokens, queries, dryRuns atomic.Int32
			var expectedMaxBytes atomic.Int64
			expectedMaxBytes.Store(100)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/token" {
					if !assert.NoError(t, r.ParseForm()) {
						return
					}
					parts := strings.Split(r.Form.Get("assertion"), ".")
					if !assert.Len(t, parts, 3) {
						return
					}
					data, err := base64.RawURLEncoding.DecodeString(parts[1])
					if !assert.NoError(t, err) {
						return
					}
					var claims struct {
						Scope string `json:"scope"`
					}
					if !assert.NoError(t, json.Unmarshal(data, &claims)) {
						return
					}
					assert.Equal(t, "https://www.googleapis.com/auth/bigquery.readonly", claims.Scope)
					tokens.Add(1)
					_, _ = w.Write([]byte(`{"access_token":"readonly-token","token_type":"Bearer","expires_in":3600}`))
					return
				}
				assert.Equal(t, "Bearer readonly-token", r.Header.Get("Authorization"))
				if r.Method != http.MethodPost || r.URL.Path != "/projects/test-project/queries" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					http.Error(w, "unexpected API", http.StatusBadRequest)
					return
				}
				var req bigqueryapi.QueryRequest
				if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&req)) {
					return
				}
				assert.Equal(t, "EU", req.Location)
				assert.Equal(t, expectedMaxBytes.Load(), req.MaximumBytesBilled)
				if strings.HasPrefix(req.Query, "DELETE") {
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(`{"error":{"code":403,"message":"insufficient authentication scopes"}}`))
					return
				}
				if req.DryRun {
					dryRuns.Add(1)
					_, _ = w.Write([]byte(`{"jobComplete":true,"totalBytesProcessed":"10","schema":{"fields":[{"name":"value","type":"INTEGER"}]}}`))
					return
				}
				queries.Add(1)
				_, _ = w.Write([]byte(`{"jobComplete":true,"jobReference":{"projectId":"test-project","jobId":"readonly-job","location":"EU"},"schema":{"fields":[{"name":"value","type":"INTEGER"}]},"totalRows":"1","rows":[{"f":[{"v":"42"}]}]}`))
			}))
			defer server.Close()
			data, err := json.Marshal(map[string]string{
				"type": "service_account", "project_id": "test-project", "client_email": "test@test-project.iam.gserviceaccount.com",
				"private_key": string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})),
				"token_uri":   server.URL + "/token",
			})
			require.NoError(t, err)
			maxBytes := int64(100)
			cfg := &Config{ProjectID: "test-project", Location: "EU", CredentialsJSON: string(data), ReadOnly: true, MaxBillableBytes: &maxBytes}
			if fromFile {
				path := filepath.Join(t.TempDir(), "credentials.json")
				require.NoError(t, os.WriteFile(path, data, 0o600))
				cfg.CredentialsJSON = ""
				cfg.CredentialsFilePath = path
			}
			initialized, err := NewDB(cfg)
			require.NoError(t, err)
			require.NotNil(t, initialized.readOnlyService)
			_, err = initialized.GetIngestrURI()
			require.ErrorContains(t, err, "read_only connections cannot be used")
			_, err = initialized.NewDataTransferClient(t.Context())
			require.ErrorContains(t, err, "read_only connections cannot be used")
			require.Equal(t, "EU", initialized.client.Location)
			require.NoError(t, initialized.client.Close())
			opts, err := cfg.clientOptions(t.Context())
			require.NoError(t, err)
			opts = append(opts, option.WithEndpoint(server.URL))
			bq, err := bigquery.NewClient(t.Context(), cfg.ProjectID, opts...)
			require.NoError(t, err)
			defer bq.Close()
			bq.Location = cfg.Location
			service, err := bigqueryapi.NewService(t.Context(), opts...)
			require.NoError(t, err)
			client := &Client{client: bq, config: cfg, readOnlyService: service}
			ctx := query.WithQueryType(t.Context(), "test")
			q := &query.Query{Query: "SELECT 42 AS value"}
			rows, err := client.Select(ctx, q)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(42)}}, rows)
			result, err := client.SelectWithSchema(ctx, q)
			require.NoError(t, err)
			require.Equal(t, []string{"value"}, result.Columns)
			require.Equal(t, []string{"INTEGER"}, result.ColumnTypes)
			require.Equal(t, rows, result.Rows)
			require.NoError(t, client.RunQueryWithoutResult(ctx, q))
			valid, err := client.IsValid(ctx, q)
			require.NoError(t, err)
			require.True(t, valid)
			stats, err := client.QueryDryRun(ctx, q)
			require.NoError(t, err)
			require.Equal(t, int64(10), stats.TotalBytesProcessed)
			require.Len(t, stats.Schema, 1)
			_, err = client.Select(ctx, &query.Query{Query: "DELETE FROM dataset.items"})
			require.ErrorContains(t, err, "insufficient authentication scopes")
			require.Equal(t, int32(1), tokens.Load())
			require.Equal(t, int32(3), queries.Load())
			require.GreaterOrEqual(t, dryRuns.Load(), int32(5))
			maxBytes = 5
			expectedMaxBytes.Store(5)
			_, err = client.Select(ctx, q)
			require.ErrorContains(t, err, "exceeds max_billable_bytes")
			require.Equal(t, int32(3), queries.Load())
		})
	}
}

func TestReadOnlyRejectsUnsupportedCredentials(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"missing":         {},
		"ADC":             {UseApplicationDefaultCredentials: true},
		"token":           {AccessToken: "existing-token"},
		"credentials":     {Credentials: &google.Credentials{}},
		"ADC with JSON":   {UseApplicationDefaultCredentials: true, CredentialsJSON: `{"type":"service_account"}`},
		"token with JSON": {AccessToken: "existing-token", CredentialsJSON: `{"type":"service_account"}`},
		"authorized user": {CredentialsJSON: `{"type":"authorized_user"}`},
		"invalid JSON":    {CredentialsJSON: `invalid`},
		"missing file":    {CredentialsFilePath: filepath.Join(t.TempDir(), "missing.json")},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg.ReadOnly = true
			cfg.ProjectID = "test-project"
			_, err := NewDB(&cfg)
			require.Error(t, err)
			client := &Client{config: &cfg}
			require.Error(t, client.createClient(t.Context()))
			_, err = client.NewDataTransferClient(t.Context())
			require.ErrorContains(t, err, "read_only connections cannot be used")
			_, err = client.GetIngestrURI()
			require.ErrorContains(t, err, "read_only connections cannot be used")
		})
	}
}
