package bigquery

import (
	"context"
	"net/http"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
)

// clockSkew allows for the difference between our clock and BigQuery's when telling a job
// created by this call apart from one created by an earlier attempt.
const clockSkew = 10 * time.Second

// runQueryJob starts q. A run-scoped job ID (see applyRunScopedJobID) may already belong to
// an earlier attempt of the asset: if that job is still running we wait for it instead of
// running the query a second time; if it has finished we run the query again under a fresh ID.
func (d *Client) runQueryJob(ctx context.Context, q *bigquery.Query) (*bigquery.Job, error) {
	started := time.Now()
	job, err := d.insertOrAttach(ctx, q)
	if err != nil || !createdBefore(job, started.Add(-clockSkew)) {
		return job, err
	}
	if job.LastStatus().State != bigquery.Done {
		return job, nil
	}
	q.AddJobIDSuffix = true
	return d.insertOrAttach(ctx, q)
}

func createdBefore(job *bigquery.Job, t time.Time) bool {
	status := job.LastStatus()
	return status != nil && status.Statistics != nil && status.Statistics.CreationTime.Before(t)
}

// The client library retries jobs.insert on transient errors using the same
// job ID. When the first attempt reached BigQuery but its response was lost,
// the retry fails with 409 "Already Exists" even though the job is running.
// insertOrAttach attaches to the job named in that error instead of failing.
func (d *Client) insertOrAttach(ctx context.Context, q *bigquery.Query) (*bigquery.Job, error) {
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
	if location, jobID, found := strings.Cut(full, "."); found {
		ref.location, full = location, jobID
	}
	ref.jobID = full
	if ref.jobID == "" {
		return jobRef{}, false
	}
	return ref, true
}
