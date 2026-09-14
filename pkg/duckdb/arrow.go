//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/bruin-data/bruin/pkg/query"
)

// SelectArrow returns an owned record batch without the JSON-oriented conversions
// in SelectWithSchema. The caller must Release it. maxRows bounds buffered rows.
func (c *Client) SelectArrow(ctx context.Context, q *query.Query, maxRows int) (arrow.RecordBatch, error) {
	c.lockIfNeeded()
	defer c.unlockIfNeeded()
	e, ok := c.connection.(*EphemeralConnection)
	if !ok {
		return nil, fmt.Errorf("Arrow queries require an ADBC connection")
	}
	db, conn, err := e.openADBC(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	defer conn.Close()
	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(q.String()); err != nil {
		return nil, err
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, fmt.Errorf("input query did not return an Arrow schema")
	}
	defer reader.Release()
	var records []arrow.RecordBatch
	defer func() {
		for _, record := range records {
			record.Release()
		}
	}()
	var count int64
	for reader.Next() {
		record := reader.RecordBatch()
		count += record.NumRows()
		if count > int64(maxRows) {
			return nil, fmt.Errorf("inference input exceeds max_rows (%d)", maxRows)
		}
		record.Retain()
		records = append(records, record)
	}
	if err := reader.Err(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, reader.Schema())
		defer builder.Release()
		return builder.NewRecordBatch(), nil
	}
	columns := make([]arrow.Array, reader.Schema().NumFields())
	defer func() {
		for _, column := range columns {
			if column != nil {
				column.Release()
			}
		}
	}()
	for i := range columns {
		parts := make([]arrow.Array, len(records))
		for j, record := range records {
			parts[j] = record.Column(i)
		}
		columns[i], err = array.Concatenate(parts, memory.DefaultAllocator)
		if err != nil {
			return nil, err
		}
	}
	return array.NewRecordBatch(reader.Schema(), columns, count), nil
}
