package bigquery

import (
	"context"
	"net/http"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
)

// The client library retries jobs.insert on transient errors using the same
// job ID. When the first attempt reached BigQuery but its response was lost,
// the retry fails with 409 "Already Exists" even though the job is running.
// runQueryJob attaches to the job named in that error instead of failing.
func (d *Client) runQueryJob(ctx context.Context, q *bigquery.Query) (*bigquery.Job, error) {
	job, err := q.Run(ctx)
	if err == nil {
		return job, nil
	}

	ref, ok := existingJobRef(err)
	if !ok {
		return nil, err
	}
	if ref.projectID == "" {
		ref.projectID = q.ProjectID
	}
	if ref.projectID == "" {
		ref.projectID = d.client.Project()
	}
	if ref.location == "" {
		ref.location = q.Location
	}
	if ref.location == "" {
		ref.location = d.client.Location
	}

	existing, getErr := d.client.JobFromProject(ctx, ref.projectID, ref.jobID, ref.location)
	if getErr != nil {
		return nil, errors.Wrapf(err, "BigQuery job %s already exists but bruin could not attach to it: %v", ref.jobID, getErr)
	}
	return existing, nil
}

type jobRef struct {
	projectID string
	location  string
	jobID     string
}

// alreadyExistsJob captures the job reference from BigQuery's 409 message,
// "Already Exists: Job <project>:<location>.<jobID>".
var alreadyExistsJob = regexp.MustCompile(`Job ([^\s,]+)`)

// existingJobRef returns the job reference from a 409 "Already Exists" error.
// The project may be domain-scoped ("example.com:my-project"), so the job part
// is whatever follows the last colon; job IDs and locations never contain dots
// or colons.
func existingJobRef(err error) (jobRef, bool) {
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != http.StatusConflict {
		return jobRef{}, false
	}

	m := alreadyExistsJob.FindStringSubmatch(apiErr.Message)
	if m == nil {
		return jobRef{}, false
	}
	full := m[1]

	var ref jobRef
	if i := strings.LastIndex(full, ":"); i >= 0 {
		ref.projectID, full = full[:i], full[i+1:]
	}
	if i := strings.Index(full, "."); i >= 0 {
		ref.location, full = full[:i], full[i+1:]
	}
	ref.jobID = full
	if ref.jobID == "" {
		return jobRef{}, false
	}
	return ref, true
}
