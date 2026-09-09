package bigquery

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	bigqueryapi "google.golang.org/api/bigquery/v2"
)

func (d *Client) isReadOnly() bool {
	return d.config != nil && d.config.ReadOnly
}

func (d *Client) startQuery(ctx context.Context, q *bigquery.Query) (*bigquery.Job, *bigquery.RowIterator, error) {
	if !d.isReadOnly() {
		job, err := q.Run(ctx)
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
	if d.config.MaxBillableBytes != nil {
		req.MaximumBytesBilled = *d.config.MaxBillableBytes
	}
	resp, err := d.readOnlyService.Jobs.Query(d.config.ProjectID, req).Context(ctx).Do()
	if err != nil {
		return nil, formatError(err)
	}
	if len(resp.Errors) > 0 {
		return nil, errors.New(resp.Errors[0].Message)
	}
	stats := &bigquery.QueryStatistics{
		TotalBytesProcessed: int64(resp.TotalBytesProcessed),
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
