package bigquery

import (
	"context"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
)

const (
	sessionIDProperty   = "session_id"
	abortSessionTimeout = 30 * time.Second
)

type sessionIDContextKey struct{}

// withSessionID returns a context whose BigQuery jobs run in the given session, so temp tables
// and variables created by earlier jobs in the session stay visible.
func withSessionID(ctx context.Context, sessionID string) context.Context {
	return context.WithValue(ctx, sessionIDContextKey{}, sessionID)
}

func sessionIDFromContext(ctx context.Context) string {
	sessionID, _ := ctx.Value(sessionIDContextKey{}).(string)
	return sessionID
}

// StartSession runs q as the first job of a new BigQuery session and returns the session ID.
func (d *Client) StartSession(ctx context.Context, q *query.Query) (string, error) {
	if err := d.ensureClientInitialized(ctx); err != nil {
		return "", err
	}
	if d.isReadOnly() {
		return "", errors.New("cannot start a BigQuery session on a read-only connection")
	}
	if err := d.checkQueryLimits(ctx, q); err != nil {
		return "", err
	}

	bqQuery := d.queryForExecution(ctx, q.String())
	bqQuery.CreateSession = true
	job, err := d.runQueryJob(ctx, bqQuery)
	if err != nil {
		return "", formatError(err)
	}
	defer cancelJobOnContextCancellation(ctx, job)
	query.LogOrSinkQueryID(ctx, "BigQuery", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return "", formatError(err)
	}
	if err := status.Err(); err != nil {
		return "", formatError(err)
	}
	if status.Statistics == nil || status.Statistics.SessionInfo == nil || status.Statistics.SessionInfo.SessionID == "" {
		return "", errors.New("BigQuery did not return a session ID")
	}
	return status.Statistics.SessionInfo.SessionID, nil
}

// AbortSession ends a session started by StartSession. Failures are ignored: an abandoned
// session expires on its own.
func (d *Client) AbortSession(ctx context.Context, sessionID string) {
	if sessionID == "" || d.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortSessionTimeout)
	defer cancel()

	q := d.client.Query("CALL BQ.ABORT_SESSION();")
	q.ConnectionProperties = []*bigquery.ConnectionProperty{{Key: sessionIDProperty, Value: sessionID}}
	if job, err := q.Run(ctx); err == nil {
		_, _ = job.Wait(ctx)
	}
}
