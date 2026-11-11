package bigquery

import (
	"context"
	"crypto/rand"
	"maps"
	"net/http"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
)

// attemptLabel marks a run-scoped job with the call that started it, so a 409 on that ID can
// tell this call's own job (the library retried the insert) from an earlier attempt's job.
const attemptLabel = "bruin_attempt"

// runQueryJob starts q. A run-scoped job ID (see applyRunScopedJobID) may already belong to
// an earlier attempt of the asset: if that job is still running we wait for it instead of
// running the query a second time; if it has finished we run the query again under a fresh ID.
func (d *Client) runQueryJob(ctx context.Context, q *bigquery.Query) (*bigquery.Job, error) {
	if q.JobID == "" || q.AddJobIDSuffix {
		// A random job ID can only collide with this call's own insert.
		job, _, err := d.insertOrAttach(ctx, q)
		return job, err
	}
	attempt := strings.ToLower(rand.Text())
	labels := make(map[string]string, len(q.Labels)+1)
	maps.Copy(labels, q.Labels)
	labels[attemptLabel] = attempt
	q.Labels = labels

	job, attached, err := d.insertOrAttach(ctx, q)
	if err != nil || !attached || startedBy(job, attempt) {
		return job, err
	}
	if job.LastStatus().State != bigquery.Done {
		return job, nil
	}
	q.AddJobIDSuffix = true
	job, _, err = d.insertOrAttach(ctx, q)
	return job, err
}

func startedBy(job *bigquery.Job, attempt string) bool {
	config, err := job.Config()
	if err != nil {
		return false
	}
	qc, ok := config.(*bigquery.QueryConfig)
	return ok && qc.Labels[attemptLabel] == attempt
}

// The client library retries jobs.insert on transient errors using the same
// job ID. When the first attempt reached BigQuery but its response was lost,
// the retry fails with 409 "Already Exists" even though the job is running.
// insertOrAttach attaches to the job named in that error instead of failing, and
// reports whether it did.
func (d *Client) insertOrAttach(ctx context.Context, q *bigquery.Query) (*bigquery.Job, bool, error) {
	job, err := q.Run(ctx)
	if err == nil {
		return job, false, nil
	}

	ref, ok := existingJobRef(err)
	if !ok {
		return nil, false, err
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
		return nil, false, errors.Wrapf(err, "BigQuery job %s already exists but bruin could not attach to it: %v", ref.jobID, getErr)
	}
	return existing, true, nil
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
