package bigquery

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"unicode"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	bigqueryapi "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
)

func (d *Client) isReadOnly() bool {
	return d.config != nil && d.config.ReadOnly
}

func (d *Client) startQuery(ctx context.Context, q *bigquery.Query) (*bigquery.Job, *bigquery.RowIterator, error) {
	if !d.isReadOnly() {
		job, err := q.Run(ctx)
		if id, location, ok := duplicateJob(err, q.JobID); ok {
			// The client library retries a failed insert with the same job ID. If the first
			// insert reached BigQuery, the retry gets 409 duplicate although our job exists,
			// so follow that job instead of failing and running the query again.
			job, err = d.client.JobFromIDLocation(ctx, id, location)
		}
		return job, nil, err
	}
	rows, err := q.Read(ctx)
	if err != nil {
		return nil, nil, err
	}
	return rows.SourceJob(), rows, nil
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

// duplicateJob returns the job a 409 "Already Exists: Job <project>:<location>.<id>" error
// refers to, if it is one of ours: the library appends a random suffix to our prefix, and
// only hands the generated ID back through this message. Anything else, including a message
// in a format we don't recognize, is left as the original error.
func duplicateJob(err error, prefix string) (id, location string, ok bool) {
	var apiErr *googleapi.Error
	if prefix == "" || !errors.As(err, &apiErr) || apiErr.Code != http.StatusConflict {
		return "", "", false
	}
	const marker = "Already Exists: Job "
	i := strings.Index(apiErr.Message, marker)
	if i < 0 {
		return "", "", false
	}
	ref := strings.FieldsFunc(apiErr.Message[i+len(marker):], func(r rune) bool { return r == ',' || unicode.IsSpace(r) })
	if len(ref) == 0 {
		return "", "", false
	}
	// The project may be domain-scoped ("example.com:project"), so split after its last colon.
	location, id, found := strings.Cut(ref[0][strings.LastIndex(ref[0], ":")+1:], ".")
	if !found || location == "" || !strings.HasPrefix(id, prefix+"-") {
		return "", "", false
	}
	return id, location, true
}
