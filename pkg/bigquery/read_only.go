package bigquery

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	bigqueryapi "google.golang.org/api/bigquery/v2"
)

func (d *Client) isReadOnly() bool {
	return d.config != nil && d.config.ReadOnly
}

func (d *Client) startQuery(ctx context.Context, q *bigquery.Query) (*bigquery.Job, *bigquery.RowIterator, error) {
	if !d.isReadOnly() {
		started := time.Now()
		job, err := q.Run(ctx)
		if err != nil && q.JobID != "" {
			// The library retries a failed insert with the same job ID, so the job can exist even
			// though Run failed (e.g. the first response was lost and the retry got 409 duplicate).
			// Follow that job instead of failing and running the query again.
			if existing, lookupErr := d.existingJob(ctx, q, started); lookupErr == nil {
				return existing, nil, nil
			}
		}
		return job, nil, err
	}
	rows, err := q.Read(ctx)
	if err != nil {
		return nil, nil, err
	}
	return rows.SourceJob(), rows, nil
}

// existingJob looks up the job q tried to create. A job in a single region can't be fetched
// without its location, so when none is configured the job is found among the jobs created
// since the insert started.
func (d *Client) existingJob(ctx context.Context, q *bigquery.Query, started time.Time) (*bigquery.Job, error) {
	location := q.Location
	if location == "" {
		location = d.client.Location
	}
	if location != "" {
		return d.client.JobFromIDLocation(ctx, q.JobID, location)
	}
	it := d.client.Jobs(ctx)
	it.MinCreationTime = started.Add(-time.Minute) // leeway for clock skew
	for {
		job, err := it.Next()
		if err != nil {
			return nil, err
		}
		if job.ID() == q.JobID {
			return d.client.JobFromIDLocation(ctx, job.ID(), job.Location())
		}
	}
}

func (d *Client) readOnlyDryRun(ctx context.Context, sql string) (*bigquery.QueryStatistics, error) {
	useLegacySQL := false
	req := &bigqueryapi.QueryRequest{
		Query:        sql,
		DryRun:       true,
		UseLegacySql: &useLegacySQL,
		Location:     d.config.Location,
	}
	if limit := d.config.bytesBilledLimit(); limit != nil {
		req.MaximumBytesBilled = *limit
	}
	resp, err := d.readOnlyService.Jobs.Query(d.config.ProjectID, req).Context(ctx).Do()
	if err != nil {
		return nil, formatError(err)
	}
	if len(resp.Errors) > 0 {
		return nil, errors.New(resp.Errors[0].Message)
	}
	stats := &bigquery.QueryStatistics{
		TotalBytesProcessed: resp.TotalBytesProcessed,
		TotalBytesBilled:    resp.TotalBytesBilled,
		CacheHit:            resp.CacheHit,
	}
	if resp.Schema != nil {
		data, err := json.Marshal(resp.Schema.Fields)
		if err != nil {
			return nil, err
		}
		stats.Schema, err = bigquery.SchemaFromJSON(data)
		if err != nil {
			return nil, err
		}
	}
	return stats, nil
}
