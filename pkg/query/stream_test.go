package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeStreamer struct {
	rows [][]interface{}
	err  error
}

func (f *fakeStreamer) StreamWithSchema(_ context.Context, _ *Query, h RowHandler) (*QueryResult, error) {
	for i, row := range f.rows {
		if i == 0 {
			if err := h.Schema([]string{"id"}, []string{"INT"}); err != nil {
				return nil, err
			}
		}
		if err := h.Row(row); err != nil {
			return nil, err
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return &QueryResult{Columns: []string{"id"}, ColumnTypes: []string{"INT"}}, nil
}

func TestCollectRows(t *testing.T) {
	t.Parallel()

	result, err := CollectRows(context.Background(), &fakeStreamer{rows: [][]interface{}{{1}, {2}}}, &Query{})
	require.NoError(t, err)
	assert.Equal(t, []string{"id"}, result.Columns)
	assert.Equal(t, [][]interface{}{{1}, {2}}, result.Rows)

	result, err = CollectRows(context.Background(), &fakeStreamer{}, &Query{})
	require.NoError(t, err)
	assert.NotNil(t, result.Rows)
	assert.Empty(t, result.Rows)

	_, err = CollectRows(context.Background(), &fakeStreamer{rows: [][]interface{}{{1}}, err: assert.AnError}, &Query{})
	require.ErrorIs(t, err, assert.AnError)
}
