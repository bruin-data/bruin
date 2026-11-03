package cmd

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func streamTestRows() [][]interface{} {
	return [][]interface{}{
		{int64(1), "plain", big.NewRat(12345, 100)},
		{int64(2), nil, nil},
		{int64(3), "needs \"quoting\", <html> & new\nline", big.NewRat(-1, 3)},
	}
}

func openStreamTestQueryLog(t *testing.T) *queryLogWriter {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, ".git"), 0o755))
	log := openQueryLog("SELECT n", "conn", QueryLogOptions{Asset: dir})
	require.NoError(t, log.err)
	// Windows cannot remove the temp dir while the log file is open.
	t.Cleanup(func() { _ = log.file.Close() })
	return log
}

// streamTo feeds rows through a streamHandler the way a RowStreamer does and finishes the sink.
func streamTo(t *testing.T, sink resultSink, result *query.QueryResult, rows [][]interface{}) *streamHandler {
	t.Helper()
	h := &streamHandler{sink: sink, log: openStreamTestQueryLog(t)}
	for i, row := range rows {
		if i == 0 {
			require.NoError(t, h.Schema(result.Columns, result.ColumnTypes))
		}
		require.NoError(t, h.Row(row))
	}
	require.NoError(t, sink.finish(result))
	return h
}

func TestJSONSinkMatchesBufferedOutput(t *testing.T) {
	t.Parallel()

	type jsonResponse struct {
		Columns   []map[string]string          `json:"columns"`
		Rows      [][]interface{}              `json:"rows"`
		ConnName  string                       `json:"connectionName"`
		Query     string                       `json:"query"`
		Execution *query.QueryExecutionSummary `json:"execution,omitempty"`
	}

	rowsAffected := int64(7)
	tests := []struct {
		name   string
		result *query.QueryResult
		rows   [][]interface{}
	}{
		{
			name:   "rows",
			result: &query.QueryResult{Columns: []string{"id", "name", "amount"}, ColumnTypes: []string{"INT64", "STRING", "NUMERIC"}},
			rows:   streamTestRows(),
		},
		{
			name:   "no rows",
			result: &query.QueryResult{Columns: []string{"id"}, ColumnTypes: []string{"INT64"}},
		},
		{
			name: "execution summary",
			result: &query.QueryResult{
				Columns:     []string{},
				ColumnTypes: []string{},
				Execution:   query.NewExecutionSummaryFromStatement("bigquery", "INSERT", &rowsAffected),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got bytes.Buffer
			streamTo(t, &jsonSink{w: bufio.NewWriter(&got), connName: "conn", queryStr: `SELECT "<x>"`}, tt.result, tt.rows)

			buffered := &query.QueryResult{Columns: tt.result.Columns, ColumnTypes: tt.result.ColumnTypes, Rows: tt.rows, Execution: tt.result.Execution}
			if buffered.Rows == nil {
				buffered.Rows = [][]interface{}{}
			}
			want := jsonResponse{
				Columns:  queryJSONColumns(buffered),
				Rows:     formatQueryRowsForJSON(buffered.Rows),
				ConnName: "conn",
				Query:    `SELECT "<x>"`,
			}
			if shouldOutputExecutionSummary(buffered) {
				want.Execution = buffered.Execution
			}
			wantJSON, err := json.Marshal(want)
			require.NoError(t, err)

			assert.Equal(t, string(wantJSON)+"\n", got.String())
		})
	}
}

func TestJSONSinkAbortKeepsDocumentValid(t *testing.T) {
	t.Parallel()

	var got bytes.Buffer
	sink := &jsonSink{w: bufio.NewWriter(&got), connName: "conn", queryStr: "SELECT 1"}
	h := &streamHandler{sink: sink, log: openStreamTestQueryLog(t)}
	require.NoError(t, h.Schema([]string{"id"}, []string{"INT64"}))
	require.NoError(t, h.Row([]interface{}{int64(1)}))
	require.Error(t, sink.abort(assert.AnError))

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(got.Bytes(), &doc))
	assert.Equal(t, assert.AnError.Error(), doc["error"])
	assert.Equal(t, []interface{}{[]interface{}{float64(1)}}, doc["rows"])
}

func TestCSVSinkMatchesBufferedOutput(t *testing.T) {
	t.Parallel()

	rowsAffected := int64(3)
	tests := []struct {
		name   string
		result *query.QueryResult
		rows   [][]interface{}
		want   func(w *csv.Writer)
	}{
		{
			name:   "rows",
			result: &query.QueryResult{Columns: []string{"id", "name", "amount"}},
			rows:   streamTestRows(),
			want: func(w *csv.Writer) {
				_ = w.Write([]string{"id", "name", "amount"})
				for _, row := range streamTestRows() {
					_ = w.Write(formatQueryRowForCSV(row))
				}
			},
		},
		{
			name:   "no rows",
			result: &query.QueryResult{Columns: []string{"id"}},
			want:   func(w *csv.Writer) { _ = w.Write([]string{"id"}) },
		},
		{
			name:   "execution summary",
			result: &query.QueryResult{Execution: query.NewExecutionSummaryFromStatement("bigquery", "DELETE", &rowsAffected)},
			want: func(w *csv.Writer) {
				_ = writeExecutionSummaryCSV(w, query.NewExecutionSummaryFromStatement("bigquery", "DELETE", &rowsAffected))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got bytes.Buffer
			streamTo(t, newCSVSink(bufio.NewWriter(&got)), tt.result, tt.rows)

			var want bytes.Buffer
			writer := csv.NewWriter(&want)
			tt.want(writer)
			writer.Flush()

			assert.Equal(t, want.String(), got.String())
		})
	}
}

func TestExportSinkSplitsFiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		numRows     int
		splitRows   int
		rowsPerFile []int
	}{
		{name: "single file", numRows: 5, splitRows: 0, rowsPerFile: []int{5}},
		{name: "even split", numRows: 10, splitRows: 5, rowsPerFile: []int{5, 5}},
		{name: "with remainder", numRows: 10, splitRows: 4, rowsPerFile: []int{4, 4, 2}},
		{name: "empty result with split", numRows: 0, splitRows: 3, rowsPerFile: []int{0}},
		{name: "empty result without split", numRows: 0, splitRows: 0, rowsPerFile: []int{0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			rows := make([][]interface{}, tt.numRows)
			for i := range tt.numRows {
				rows[i] = []interface{}{i, fmt.Sprintf("name_%d", i)}
			}
			columns := []string{"id", "name"}
			sink := newExportSink("plain", dir, tt.splitRows, 1700000000000)
			streamTo(t, sink, &query.QueryResult{Columns: columns}, rows)

			require.Len(t, sink.paths, len(tt.rowsPerFile))
			if tt.splitRows > 0 {
				assert.Equal(t, filepath.Join(dir, "query_result_1700000000000_part1.csv"), sink.paths[0])
			} else {
				assert.Equal(t, filepath.Join(dir, "query_result_1700000000000.csv"), sink.paths[0])
			}

			next := 0
			for i, path := range sink.paths {
				file, err := os.Open(path)
				require.NoError(t, err)
				records, err := csv.NewReader(file).ReadAll()
				require.NoError(t, file.Close())
				require.NoError(t, err)

				require.Len(t, records, tt.rowsPerFile[i]+1)
				assert.Equal(t, columns, records[0])
				for _, record := range records[1:] {
					assert.Equal(t, fmt.Sprintf("name_%d", next), record[1])
					next++
				}
			}
			assert.Equal(t, tt.numRows, next)
		})
	}
}

func TestExportSinkAbortRemovesFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sink := newExportSink("plain", dir, 2, 1700000000000)
	h := &streamHandler{sink: sink, log: openStreamTestQueryLog(t)}
	require.NoError(t, h.Schema([]string{"id"}, nil))
	for i := range 5 {
		require.NoError(t, h.Row([]interface{}{i}))
	}
	require.Len(t, sink.paths, 3)
	require.Error(t, sink.abort(assert.AnError))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestStreamHandlerWritesEveryRowToQueryLog(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	rows := make([][]interface{}, aggregatedQueryLogMaxRows+5)
	for i := range rows {
		rows[i] = []interface{}{i}
	}
	h := streamTo(t, newCSVSink(bufio.NewWriter(&out)), &query.QueryResult{Columns: []string{"n"}}, rows)
	require.NoError(t, h.log.finish(nil))

	data, err := os.ReadFile(h.log.path)
	require.NoError(t, err)
	var log QueryLog
	require.NoError(t, json.Unmarshal(data, &log))
	assert.True(t, log.Success)
	assert.Equal(t, []string{"n"}, log.Columns)
	require.Len(t, log.Rows, len(rows))
	assert.Equal(t, []interface{}{float64(len(rows) - 1)}, log.Rows[len(rows)-1])
}

func TestStreamHandlerIgnoresQueryLogFailure(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	sink := newCSVSink(bufio.NewWriter(&out))
	h := &streamHandler{sink: sink, log: openStreamTestQueryLog(t)}
	// Closing the file underneath makes every later log write fail.
	require.NoError(t, h.log.file.Close())

	require.NoError(t, h.Schema([]string{"n"}, []string{"INT"}))
	for i := range 5000 {
		require.NoError(t, h.Row([]interface{}{i}), "a log failure must not stop the query")
	}
	require.NoError(t, sink.finish(&query.QueryResult{Columns: []string{"n"}}))

	records, err := csv.NewReader(&out).ReadAll()
	require.NoError(t, err)
	assert.Len(t, records, 5001, "every row should reach the output")

	require.Error(t, h.log.finish(nil))
	entries, err := os.ReadDir(h.log.logDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "a failed log should leave no file behind")
}

func TestPlainSinkSwitchesToCSVAfterMaxRows(t *testing.T) {
	t.Parallel()

	for _, n := range []int{plainTableMaxRows, plainTableMaxRows + 1} {
		var out bytes.Buffer
		rows := make([][]interface{}, n)
		for i := range rows {
			rows[i] = []interface{}{int64(i), fmt.Sprintf("v%d", i)}
		}
		streamTo(t, &plainSink{w: bufio.NewWriter(&out)}, &query.QueryResult{Columns: []string{"n", "s"}}, rows)

		if n <= plainTableMaxRows {
			assert.Contains(t, out.String(), "│ 99 │ v99 │", "up to the limit the result should be a table")
			assert.NotContains(t, out.String(), "n,s")
			continue
		}
		records, err := csv.NewReader(&out).ReadAll()
		require.NoError(t, err, "above the limit the output should be CSV")
		require.Len(t, records, n+1)
		assert.Equal(t, []string{"n", "s"}, records[0])
		assert.Equal(t, []string{"0", "v0"}, records[1], "buffered rows come first, in order")
		assert.Equal(t, []string{"100", "v100"}, records[n])
	}
}

func TestPlainSinkShowsEmptyResultAsTable(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	sink := &plainSink{w: bufio.NewWriter(&out)}
	require.NoError(t, sink.finish(&query.QueryResult{Columns: []string{"n"}}))
	assert.Equal(t, "No data available\n", out.String())
}
