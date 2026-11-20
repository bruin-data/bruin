package failurereport

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

func googleErr(code int, reason string) error {
	err := &googleapi.Error{Code: code, Message: "message"}
	if reason != "" {
		err.Errors = []googleapi.ErrorItem{{Reason: reason}}
	}
	return err
}

func TestClassify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want Report
	}{
		{
			name: "missing connection",
			err:  fmt.Errorf("asset failed: %w", &config.MissingConnectionError{Name: "gcp-prod", SecretsBackend: "vault"}),
			want: Report{Class: MissingConnection, Code: "connection_missing", Source: "bruin", Connection: "gcp-prod"},
		},
		{
			name: "secrets backend couldn't be read",
			err: &config.MissingConnectionError{Name: "gcp-prod", SecretsBackend: "vault", Cause: fmt.Errorf("resolving: %w",
				&config.SecretsBackendUnavailableError{Err: errors.New("failed to read secret 'gcp-prod' from Vault: timeout")})},
			want: Report{Class: Infra, Code: "secrets_backend", Source: "secrets", Connection: "gcp-prod"},
		},
		{
			name: "secret isn't a valid connection",
			err:  &config.MissingConnectionError{Name: "gcp-prod", SecretsBackend: "vault", Cause: errors.New("secret 'gcp-prod' must contain both 'type' (non-empty string) and 'details' (object)")},
			want: Report{Class: Connection, Code: "invalid_connection", Source: "secrets", Connection: "gcp-prod"},
		},
		{
			name: "asset timeout",
			err:  &executor.AssetTimeoutError{AssetName: "a", Timeout: time.Hour},
			want: Report{Class: AssetTimeout, Code: "asset_timeout", Source: "bruin"},
		},
		{
			name: "sensor timeout",
			err:  &helpers.SensorTimeoutError{Timeout: 24 * time.Hour},
			want: Report{Class: DataNotReady, Code: "sensor_timeout", Source: "bruin"},
		},
		{
			name: "bigquery job failed inside BigQuery",
			err:  fmt.Errorf("failed to run query: %w", googleErr(400, "jobBackendError")),
			want: Report{Class: Transient, Code: "bq_retryable", Source: "google_api", Reason: "jobBackendError", HTTPStatus: 400},
		},
		{
			name: "bigquery rate limit",
			err:  googleErr(403, "rateLimitExceeded"),
			want: Report{Class: Transient, Code: "rate_limit", Source: "google_api", Reason: "rateLimitExceeded", HTTPStatus: 403},
		},
		{
			name: "bigquery billing disabled",
			err:  googleErr(403, "billingNotEnabled"),
			want: Report{Class: ProviderLimits, Code: "provider_billing_disabled", Source: "google_api", Reason: "billingNotEnabled", HTTPStatus: 403},
		},
		{
			name: "bigquery access denied",
			err:  googleErr(403, "accessDenied"),
			want: Report{Class: Permission, Code: "access_denied", Source: "google_api", Reason: "accessDenied", HTTPStatus: 403},
		},
		{
			name: "bigquery invalid query",
			err:  googleErr(400, "invalidQuery"),
			want: Report{Class: UserCode, Code: "sql_error", Source: "google_api", Reason: "invalidQuery", HTTPStatus: 400},
		},
		{
			name: "bigquery table not found",
			err:  googleErr(404, "notFound"),
			want: Report{Class: UserCode, Code: "missing_object", Source: "google_api", Reason: "notFound", HTTPStatus: 404},
		},
		{
			name: "google api server error without a reason",
			err:  googleErr(503, ""),
			want: Report{Class: Transient, Code: "http_5xx", Source: "google_api", HTTPStatus: 503},
		},
		{
			name: "google api too many requests",
			err:  googleErr(429, ""),
			want: Report{Class: Transient, Code: "rate_limit", Source: "google_api", HTTPStatus: 429},
		},
		{
			name: "google api error with another reason",
			err:  googleErr(400, "invalid"),
			want: Report{Class: Unknown, Source: "google_api", Reason: "invalid", HTTPStatus: 400},
		},
		{
			name: "bigquery job status error",
			err:  &bigquery.Error{Reason: "jobInternalError"},
			want: Report{Class: Transient, Code: "bq_retryable", Source: "bigquery", Reason: "jobInternalError"},
		},
		{
			name: "postgres connection lost",
			err:  &pgconn.PgError{Code: "08006"},
			want: Report{Class: Transient, Code: "connection_reset", Source: "postgres", SQLState: "08006"},
		},
		{
			name: "postgres deadlock",
			err:  &pgconn.PgError{Code: "40P01"},
			want: Report{Class: Transient, Code: "concurrent_update", Source: "postgres", SQLState: "40P01"},
		},
		{
			name: "postgres wrong password",
			err:  &pgconn.PgError{Code: "28P01"},
			want: Report{Class: Connection, Code: "invalid_credentials", Source: "postgres", SQLState: "28P01"},
		},
		{
			name: "postgres permission denied",
			err:  &pgconn.PgError{Code: "42501"},
			want: Report{Class: Permission, Code: "access_denied", Source: "postgres", SQLState: "42501"},
		},
		{
			name: "postgres table missing",
			err:  &pgconn.PgError{Code: "42P01"},
			want: Report{Class: UserCode, Code: "missing_object", Source: "postgres", SQLState: "42P01"},
		},
		{
			name: "postgres syntax error",
			err:  &pgconn.PgError{Code: "42601"},
			want: Report{Class: UserCode, Code: "sql_error", Source: "postgres", SQLState: "42601"},
		},
		{
			name: "postgres other error",
			err:  &pgconn.PgError{Code: "22012"},
			want: Report{Class: Unknown, Source: "postgres", SQLState: "22012"},
		},
		{
			name: "anything else",
			err:  fmt.Errorf("running asset: %w", errors.New("exit status 1")),
			want: Report{Class: Unknown, ErrorType: "*errors.errorString"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, Classify(tt.err))
		})
	}
}

func TestLine(t *testing.T) {
	t.Parallel()

	line := Line("analytics.daily", googleErr(400, "jobBackendError"))
	require.True(t, strings.HasPrefix(line, Prefix))
	assert.Equal(t, `bruin-failure: {"asset":"analytics.daily","class":"transient","code":"bq_retryable","source":"google_api","reason":"jobBackendError","http_status":400}`, line)

	var report Report
	require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, Prefix)), &report))
	assert.Equal(t, "analytics.daily", report.Asset)
	assert.NotContains(t, line, "message", "the error message isn't repeated")
}
