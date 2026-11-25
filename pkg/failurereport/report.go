// Package failurereport describes why an asset failed in one machine-readable line, so that Bruin
// Cloud can decide whether to retry it from the error's type rather than its message.
package failurereport

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"syscall"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/api/googleapi"
)

// Prefix starts the line that carries a Report.
const Prefix = "bruin-failure: "

// Classes of failures.
const (
	// Transient failures are usually fixed by running the asset again: server errors, rate limits.
	Transient = "transient"
	// Infra failures come from what Bruin runs on, such as the secrets backend.
	Infra = "infra"
	// MissingConnection means the asset uses a connection that doesn't exist.
	MissingConnection = "missing_connection"
	// Connection means a connection exists but its credentials are rejected.
	Connection = "connection"
	// Permission means the account lacks access in the warehouse or service.
	Permission = "permission"
	// ProviderLimits means the provider account needs action, such as enabling billing.
	ProviderLimits = "provider_limits"
	// UserCode means the asset's query or configuration has to change.
	UserCode = "user_code"
	// DataNotReady means a sensor's condition wasn't met before its timeout.
	DataNotReady = "data_not_ready"
	// AssetTimeout means the asset ran longer than its timeout.
	AssetTimeout = "asset_timeout"
	Unknown      = "unknown"
)

// Report says why an asset failed. Class and Code are the classification; the other fields carry
// what it was made from, so the reader can classify differently without a new Bruin release.
type Report struct {
	Asset      string `json:"asset"`
	Class      string `json:"class"`
	Code       string `json:"code,omitempty"`
	Source     string `json:"source,omitempty"`
	Reason     string `json:"reason,omitempty"`
	HTTPStatus int    `json:"http_status,omitempty"`
	SQLState   string `json:"sqlstate,omitempty"`
	Connection string `json:"connection,omitempty"`
	// ErrorType is the Go type of the innermost error, to find failures worth classifying.
	ErrorType string `json:"error_type,omitempty"`
}

// Line returns the report line for an asset that failed with err.
func Line(asset string, err error) string {
	report := Classify(err)
	report.Asset = asset
	encoded, marshalErr := json.Marshal(report)
	if marshalErr != nil {
		return Prefix + fmt.Sprintf(`{"asset":%q,"class":%q}`, asset, Unknown)
	}
	return Prefix + string(encoded)
}

// Classify sorts an asset's error into a class by its type.
func Classify(err error) Report {
	var missingConnection *config.MissingConnectionError
	if errors.As(err, &missingConnection) {
		var unavailable *config.SecretsBackendUnavailableError
		switch {
		case errors.As(missingConnection.Cause, &unavailable):
			return Report{Class: Infra, Code: "secrets_backend", Source: "secrets", Connection: missingConnection.Name}
		case missingConnection.Cause != nil:
			// The secret is there but isn't a valid connection.
			return Report{Class: Connection, Code: "invalid_connection", Source: "secrets", Connection: missingConnection.Name}
		}
		return Report{Class: MissingConnection, Code: "connection_missing", Source: "bruin", Connection: missingConnection.Name}
	}

	// A secrets backend that couldn't be read, or a connection that isn't in it, reached without a
	// MissingConnectionError, e.g. through ingestr's connection lookup.
	var unavailable *config.SecretsBackendUnavailableError
	if errors.As(err, &unavailable) {
		return Report{Class: Infra, Code: "secrets_backend", Source: "secrets"}
	}
	if errors.Is(err, config.ErrConnectionNotFound) {
		return Report{Class: MissingConnection, Code: "connection_missing", Source: "secrets"}
	}

	var assetTimeout *executor.AssetTimeoutError
	if errors.As(err, &assetTimeout) {
		return Report{Class: AssetTimeout, Code: "asset_timeout", Source: "bruin"}
	}

	var sensorTimeout *helpers.SensorTimeoutError
	if errors.As(err, &sensorTimeout) {
		return Report{Class: DataNotReady, Code: "sensor_timeout", Source: "bruin"}
	}

	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return classifyGoogleAPI(apiErr)
	}

	var bqErr *bigquery.Error
	if errors.As(err, &bqErr) {
		report := Report{Source: "bigquery", Reason: bqErr.Reason}
		report.Class, report.Code = bigQueryReason(bqErr.Reason, bqErr.Message)
		return report
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		report := Report{Source: "postgres", SQLState: pgErr.Code}
		report.Class, report.Code = sqlState(pgErr.Code)
		return report
	}

	if report, ok := classifyNetwork(err); ok {
		return report
	}

	return Report{Class: Unknown, ErrorType: innermostType(err)}
}

// classifyNetwork classifies a failure to reach a service, e.g. a database that doesn't answer.
func classifyNetwork(err error) (Report, bool) {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsNotFound {
			return Report{Class: Connection, Code: "host_not_found", Source: "network"}, true
		}
		return Report{Class: Transient, Code: "dns_error", Source: "network"}, true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return Report{Class: Transient, Code: "timeout", Source: "network"}, true
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.ErrUnexpectedEOF) {
		return Report{Class: Transient, Code: "connection_reset", Source: "network"}, true
	}
	return Report{}, false
}

func classifyGoogleAPI(apiErr *googleapi.Error) Report {
	report := Report{Source: "google_api", HTTPStatus: apiErr.Code}
	if len(apiErr.Errors) > 0 {
		report.Reason = apiErr.Errors[0].Reason
	}
	report.Class, report.Code = bigQueryReason(report.Reason, apiErr.Message)
	if report.Class != Unknown {
		return report
	}
	switch {
	case apiErr.Code == http.StatusTooManyRequests:
		report.Class, report.Code = Transient, "rate_limit"
	case apiErr.Code >= http.StatusInternalServerError && apiErr.Code != http.StatusNotImplemented:
		report.Class, report.Code = Transient, "http_5xx"
	}
	return report
}

// bigQueryReason maps BigQuery's error reasons, see
// https://cloud.google.com/bigquery/docs/error-messages.
func bigQueryReason(reason, message string) (string, string) {
	// A query reading a table it may not read fails as an invalid query, with a message such as
	// "Access Denied: Table p:d.t: User does not have permission to query table p:d.t, or perhaps
	// it does not exist."
	if reason == "invalidQuery" && strings.HasPrefix(message, "Access Denied:") {
		return Permission, "access_denied"
	}
	// Otherwise invalidQuery says little: BigQuery uses it for most errors while running a query,
	// a syntax error as well as a missing table or a transaction aborted by a concurrent update,
	// which a retry fixes. It is left unknown, with the reason, for the reader to decide.
	switch reason {
	case "backendError", "internalError", "jobBackendError", "jobInternalError":
		return Transient, "bq_retryable"
	case "rateLimitExceeded", "jobRateLimitExceeded":
		return Transient, "rate_limit"
	case "quotaExceeded":
		return Transient, "provider_quota_exceeded"
	case "duplicate":
		return Transient, "bq_job_exists"
	case "billingNotEnabled":
		return ProviderLimits, "provider_billing_disabled"
	case "accessDenied":
		return Permission, "access_denied"
	case "notFound":
		return UserCode, "missing_object"
	}
	return Unknown, ""
}

// sqlState maps Postgres error codes, see https://www.postgresql.org/docs/current/errcodes-appendix.html.
func sqlState(code string) (string, string) {
	switch {
	case strings.HasPrefix(code, "08"), slices.Contains([]string{"57P01", "57P02", "57P03"}, code):
		return Transient, "connection_reset"
	case code == "40001", code == "40P01":
		return Transient, "concurrent_update"
	case strings.HasPrefix(code, "28"):
		return Connection, "invalid_credentials"
	case code == "42501":
		return Permission, "access_denied"
	case code == "42P01":
		return UserCode, "missing_object"
	case strings.HasPrefix(code, "42"):
		return UserCode, "sql_error"
	}
	return Unknown, ""
}

// innermostType follows the error chain, taking the first error where several were joined.
func innermostType(err error) string {
	if err == nil {
		return ""
	}
	for {
		var next error
		switch e := err.(type) { //nolint:errorlint // walks the chain by hand
		case interface{ Unwrap() []error }:
			if joined := e.Unwrap(); len(joined) > 0 {
				next = joined[0]
			}
		default:
			next = errors.Unwrap(err)
		}
		if next == nil {
			return fmt.Sprintf("%T", err)
		}
		err = next
	}
}
