package bigquery

import (
	"errors"
	"net/http"
	"slices"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

// transientReasons are the BigQuery error reasons that BigQuery says to retry:
// https://cloud.google.com/bigquery/docs/error-messages
var transientReasons = []string{"backendError", "internalError", "jobBackendError", "jobInternalError", "rateLimitExceeded", "jobRateLimitExceeded"}

// transientError marks a query error that running the query again may not hit, such as a job
// that failed inside BigQuery. It reads exactly like the error it wraps.
type transientError struct {
	err error
}

func (e *transientError) Error() string { return e.err.Error() }

func (e *transientError) Unwrap() error { return e.err }

// isTransient reports whether a query error was marked as transient.
func isTransient(err error) bool {
	var transient *transientError
	return errors.As(err, &transient)
}

// queryError formats an error from the BigQuery client like formatError, and marks it if it is
// transient. formatError turns some errors into plain text, so the mark has to be made first.
func queryError(err error) error {
	formatted := formatError(err)
	if hasTransientReason(err) {
		return &transientError{err: formatted}
	}
	return formatted
}

// hasTransientReason reports whether an error from the BigQuery client carries a reason or
// status code that BigQuery says to retry.
func hasTransientReason(err error) bool {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway,
			http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		}
		return slices.ContainsFunc(apiErr.Errors, func(item googleapi.ErrorItem) bool {
			return slices.Contains(transientReasons, item.Reason)
		})
	}
	var bqErr *bigquery.Error
	if errors.As(err, &bqErr) {
		return slices.Contains(transientReasons, bqErr.Reason)
	}
	return false
}
