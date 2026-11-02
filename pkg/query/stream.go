package query

import "context"

// RowHandler receives a streamed result; Schema is called once, before the first row.
type RowHandler interface {
	Schema(columns, columnTypes []string) error
	Row(row []interface{}) error
}

// RowStreamer hands each result row to a RowHandler as it is read; the returned result carries no rows.
type RowStreamer interface {
	StreamWithSchema(ctx context.Context, q *Query, h RowHandler) (*QueryResult, error)
}

type rowCollector struct {
	rows [][]interface{}
}

func (c *rowCollector) Schema(_, _ []string) error {
	return nil
}

func (c *rowCollector) Row(row []interface{}) error {
	c.rows = append(c.rows, row)
	return nil
}

// CollectRows runs a streaming query and gathers its rows into the result.
func CollectRows(ctx context.Context, s RowStreamer, q *Query) (*QueryResult, error) {
	collector := &rowCollector{rows: [][]interface{}{}}
	result, err := s.StreamWithSchema(ctx, q, collector)
	if err != nil {
		return nil, err
	}
	result.Rows = collector.rows
	return result, nil
}
