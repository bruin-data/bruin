package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newQueryLogTestRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, ".git"), 0o755))
	return dir
}

func openTestQueryLog(t *testing.T, dir string) *queryLogWriter {
	t.Helper()
	log := openQueryLog("SELECT id, name", "warehouse", QueryLogOptions{Asset: dir})
	require.NoError(t, log.err)
	t.Cleanup(func() { _ = log.file.Close() })
	return log
}

func decodeTestQueryLog(t *testing.T, path string) QueryLog {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	dec := json.NewDecoder(file)
	dec.UseNumber()
	var log QueryLog
	require.NoError(t, dec.Decode(&log))
	var extra any
	require.ErrorIs(t, dec.Decode(&extra), io.EOF)
	return log
}

func writeQueryLogTestFixture(t *testing.T, payload string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "query.json")
	require.NoError(t, os.WriteFile(path, []byte(payload), 0o600))
	return path
}

func TestQueryLogWriterRoundTripsComplexCells(t *testing.T) {
	t.Parallel()

	rat := big.NewRat(128388988, 2000000)
	var nilRat *big.Rat
	tests := []struct {
		name string
		row  []any
		want string
	}{
		{
			name: "escaped and unicode text",
			row:  []any{"İstanbul 🦆", "\"\\\n\t\r\x00</script>\u2028\u2029"},
			want: `["İstanbul 🦆","\"\\\n\t\r\u0000</script>\u2028\u2029"]`,
		},
		{
			name: "integer precision",
			row:  []any{int64(9007199254740993), uint64(math.MaxUint64), int64(math.MinInt64)},
			want: `[9007199254740993,18446744073709551615,-9223372036854775808]`,
		},
		{
			name: "decimal precision",
			row:  []any{rat, *big.NewRat(-1, 8), json.Number("12345678901234567890.123456789012345678")},
			want: `[64.194494,-0.125,12345678901234567890.123456789012345678]`,
		},
		{
			name: "null bool and empty values",
			row:  []any{nil, nilRat, true, false, "", []any{}, map[string]any{}},
			want: `[null,null,true,false,"",[],{}]`,
		},
		{
			name: "nested arrays and objects",
			row:  []any{map[string]any{"items": []any{nil, map[string]any{"id": json.Number("9007199254740993")}}}, []any{[]any{1, "two"}}},
			want: `[{"items":[null,{"id":9007199254740993}]},[[1,"two"]]]`,
		},
		{
			name: "binary timestamp and raw json",
			row:  []any{[]byte{0, 1, 255}, time.Date(2026, 11, 3, 10, 20, 30, 123456789, time.UTC), json.RawMessage(`{"nested":[1,true,null]}`)},
			want: `["AAH/","2026-11-03T10:20:30.123456789Z",{"nested":[1,true,null]}]`,
		},
		{name: "nil row", row: nil, want: `[]`},
		{name: "empty row", row: []any{}, want: `[]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			log := openTestQueryLog(t, newQueryLogTestRepo(t))
			log.writeRow(tt.row)
			require.NoError(t, log.finish(nil))
			got := decodeTestQueryLog(t, log.path)
			var want []any
			dec := json.NewDecoder(strings.NewReader(tt.want))
			dec.UseNumber()
			require.NoError(t, dec.Decode(&want))
			require.Equal(t, [][]any{want}, got.Rows)
			assert.True(t, got.Success)
		})
	}
}

func TestQueryLogWriterStreamsBeforeFinish(t *testing.T) {
	t.Parallel()

	dir := newQueryLogTestRepo(t)
	log := openTestQueryLog(t, dir)
	log.writeColumns([]string{"id", "name"})
	row := []any{1, strings.Repeat("İstanbul\n", 2048)}
	log.writeRow(row)
	row[0], row[1] = 2, "Ece"
	log.writeRow(row)
	require.NoError(t, log.err)
	info, err := os.Stat(log.path + ".tmp")
	require.NoError(t, err)
	assert.Positive(t, info.Size())
	assert.NoFileExists(t, log.path)

	target := filepath.Join(dir, "aggregate.json")
	require.NoError(t, writeAggregatedQueryLog(log.logDir, target))
	payload, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.JSONEq(t, `[]`, string(payload))

	require.NoError(t, log.finish(nil))
	assert.NoFileExists(t, log.path+".tmp")
	got := decodeTestQueryLog(t, log.path)
	assert.Equal(t, []string{"id", "name"}, got.Columns)
	require.Len(t, got.Rows, 2)
	assert.Equal(t, []any{json.Number("1"), strings.Repeat("İstanbul\n", 2048)}, got.Rows[0])
	assert.Equal(t, []any{json.Number("2"), "Ece"}, got.Rows[1])
	info, err = os.Stat(log.path)
	require.NoError(t, err)
	if runtime.GOOS != osWindows {
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
}

func TestQueryLogWriterPreservesPartialRowsOnQueryError(t *testing.T) {
	t.Parallel()

	log := openTestQueryLog(t, newQueryLogTestRepo(t))
	log.writeColumns([]string{"id", "name"})
	log.writeRow([]any{1, "Ali"})
	log.writeRow([]any{2, "Ece"})
	require.NoError(t, log.finish(context.Canceled))
	got := decodeTestQueryLog(t, log.path)
	assert.False(t, got.Success)
	assert.Equal(t, context.Canceled.Error(), got.Error)
	assert.Equal(t, [][]any{{json.Number("1"), "Ali"}, {json.Number("2"), "Ece"}}, got.Rows)
	assert.False(t, got.Timestamp.IsZero())
}

func TestSaveQueryLogEmptyResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		result   *query.QueryResult
		queryErr error
		columns  []string
	}{
		{name: "nil result"},
		{name: "empty result", result: &query.QueryResult{}},
		{name: "schema without rows", result: &query.QueryResult{Columns: []string{"id", "name"}}, columns: []string{"id", "name"}},
		{name: "error without result", queryErr: context.DeadlineExceeded},
		{name: "error discards buffered result", result: &query.QueryResult{Columns: []string{"id"}, Rows: [][]any{{1}}}, queryErr: context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := newQueryLogTestRepo(t)
			require.NoError(t, saveQueryLog("SELECT empty", "conn", tt.result, tt.queryErr, QueryLogOptions{Asset: dir}))
			paths, err := filepath.Glob(filepath.Join(dir, "logs", "queries", "*.json"))
			require.NoError(t, err)
			require.Len(t, paths, 1)
			got := decodeTestQueryLog(t, paths[0])
			assert.Equal(t, tt.columns, got.Columns)
			assert.Empty(t, got.Rows)
			assert.Equal(t, tt.queryErr == nil, got.Success)
			if tt.queryErr != nil {
				assert.Equal(t, tt.queryErr.Error(), got.Error)
			}
		})
	}
}

func TestQueryLogWriterPreservesMetadata(t *testing.T) {
	t.Parallel()

	dir := newQueryLogTestRepo(t)
	asset := filepath.Join(dir, "assets", "query.sql")
	require.NoError(t, os.MkdirAll(filepath.Dir(asset), 0o755))
	require.NoError(t, os.WriteFile(asset, []byte("SELECT 1"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("existing-pattern\n"), 0o644))
	started := time.Date(2026, 11, 3, 9, 8, 7, 123456789, time.FixedZone("offset", 3*60*60))
	opts := QueryLogOptions{QueryStartTimestamp: started, Asset: asset, Environment: "dev\"\\\n🦆", Limit: 123456789, Timeout: 86400, Description: "line one\nline two\t\"description\""}
	queryStr, conn := "SELECT '\"\\\nİstanbul'", "connection\"\\\n"
	log := openQueryLog(queryStr, conn, opts)
	require.NoError(t, log.err)
	t.Cleanup(func() { _ = log.file.Close() })
	log.writeColumns([]string{"id\"", "name\n🦆"})
	log.writeRow([]any{1, "Ali"})
	require.NoError(t, log.finish(nil))
	got := decodeTestQueryLog(t, log.path)
	assert.Equal(t, queryStr, got.Query)
	assert.Equal(t, conn, got.Connection)
	assert.True(t, started.Equal(got.QueryStartTimestamp))
	assert.Equal(t, opts.Asset, got.Asset)
	assert.Equal(t, opts.Environment, got.Environment)
	assert.Equal(t, opts.Limit, got.Limit)
	assert.Equal(t, opts.Timeout, got.Timeout)
	assert.Equal(t, opts.Description, got.Description)
	assert.Equal(t, []string{"id\"", "name\n🦆"}, got.Columns)
	payload, err := os.ReadFile(filepath.Join(dir, ".gitignore"))
	require.NoError(t, err)
	assert.Contains(t, string(payload), "existing-pattern\n")
	assert.Equal(t, 1, strings.Count(string(payload), "logs/queries"))
}

func TestQueryLogWriterCleansUpInvalidRows(t *testing.T) {
	t.Parallel()

	cycle := map[string]any{}
	cycle["self"] = cycle
	tests := []struct {
		name string
		cell any
	}{
		{name: "channel", cell: make(chan int)},
		{name: "function", cell: func() {}},
		{name: "nan", cell: math.NaN()},
		{name: "positive infinity", cell: math.Inf(1)},
		{name: "negative infinity", cell: math.Inf(-1)},
		{name: "invalid raw json", cell: json.RawMessage(`{"broken":`)},
		{name: "cyclic object", cell: cycle},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			log := openTestQueryLog(t, newQueryLogTestRepo(t))
			log.writeRow([]any{1, "already written"})
			log.writeRow([]any{2, tt.cell})
			firstErr := log.err
			require.ErrorContains(t, firstErr, "failed to marshal query log row")
			log.writeRow([]any{3, "ignored"})
			log.writeColumns([]string{"ignored"})
			require.Same(t, firstErr, log.finish(nil))
			assert.NoFileExists(t, log.path)
			assert.NoFileExists(t, log.path+".tmp")
		})
	}
}

func TestQueryLogWriterCleansUpIOFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(*testing.T, *queryLogWriter)
		want  string
	}{
		{
			name: "write during streaming",
			setup: func(t *testing.T, log *queryLogWriter) {
				require.NoError(t, log.file.Close())
				log.writeRow([]any{strings.Repeat("x", 16384)})
				require.Error(t, log.err)
			},
			want: "failed to write query log file",
		},
		{
			name: "flush at finish",
			setup: func(t *testing.T, log *queryLogWriter) {
				log.writeRow([]any{1, "buffered"})
				require.NoError(t, log.file.Close())
				require.NoError(t, log.err)
			},
			want: "failed to write query log file",
		},
		{
			name: "close at finish",
			setup: func(t *testing.T, log *queryLogWriter) {
				log.w = bufio.NewWriter(io.Discard)
				require.NoError(t, log.file.Close())
			},
			want: "failed to close query log file",
		},
		{
			name: "rename at finish",
			setup: func(t *testing.T, log *queryLogWriter) {
				require.NoError(t, os.Mkdir(log.path, 0o755))
			},
			want: "failed to finalize query log file",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			log := openTestQueryLog(t, newQueryLogTestRepo(t))
			tt.setup(t, log)
			require.ErrorContains(t, log.finish(nil), tt.want)
			assert.NoFileExists(t, log.path)
			assert.NoFileExists(t, log.path+".tmp")
		})
	}
}

func TestOpenQueryLogFailuresRemainSafe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(*testing.T) string
		want  string
	}{
		{name: "outside repository", setup: func(t *testing.T) string { return t.TempDir() }, want: "failed to find repo root"},
		{
			name: "gitignore is a directory",
			setup: func(t *testing.T) string {
				dir := newQueryLogTestRepo(t)
				require.NoError(t, os.Mkdir(filepath.Join(dir, ".gitignore"), 0o755))
				return dir
			},
			want: "failed to add logs/queries to .gitignore",
		},
		{
			name: "logs path is a file",
			setup: func(t *testing.T) string {
				dir := newQueryLogTestRepo(t)
				require.NoError(t, os.WriteFile(filepath.Join(dir, "logs"), []byte("blocked"), 0o600))
				return dir
			},
			want: "failed to create logs/queries directory",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			log := openQueryLog("SELECT 1", "conn", QueryLogOptions{Asset: tt.setup(t)})
			log.writeColumns([]string{"id"})
			log.writeRow([]any{1})
			require.ErrorContains(t, log.finish(nil), tt.want)
		})
	}
}

func TestQueryLogWriterUniqueFilesAtSameTimestamp(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		dir := newQueryLogTestRepo(t)
		first := openTestQueryLog(t, dir)
		second := openTestQueryLog(t, dir)
		require.NotEqual(t, first.path, second.path)
		first.writeRow([]any{1, "Ali"})
		second.writeRow([]any{2, "Ece"})
		require.NoError(t, second.finish(nil))
		require.NoError(t, first.finish(nil))
		assert.Equal(t, [][]any{{json.Number("1"), "Ali"}}, decodeTestQueryLog(t, first.path).Rows)
		assert.Equal(t, [][]any{{json.Number("2"), "Ece"}}, decodeTestQueryLog(t, second.path).Rows)
	})
}

func TestQueryLogWriterLargeStreamWithReusedRow(t *testing.T) {
	t.Parallel()

	log := openTestQueryLog(t, newQueryLogTestRepo(t))
	log.writeColumns([]string{"id", "name"})
	row := make([]any, 2)
	const count = 10000
	for i := range count {
		row[0], row[1] = i, strings.Repeat("escaped\"\\\n🦆", 32)
		log.writeRow(row)
	}
	require.NoError(t, log.finish(nil))
	got := decodeTestQueryLog(t, log.path)
	require.Len(t, got.Rows, count)
	for i, gotRow := range got.Rows {
		require.Equal(t, []any{json.Number(strconv.Itoa(i)), strings.Repeat("escaped\"\\\n🦆", 32)}, gotRow)
	}
	preview, err := readQueryLogPreview(log.path)
	require.NoError(t, err)
	require.Equal(t, got.Rows[:aggregatedQueryLogMaxRows], preview.Rows)
}

func TestReadQueryLogPreviewFieldOrderAndCompatibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
		rows    int
	}{
		{name: "rows first", payload: `{"rows":[[1,"Ali"]],"query":"SELECT 1","success":true}`, rows: 1},
		{name: "rows last", payload: `{"query":"SELECT 1","success":true,"rows":[[1,"Ali"]]}`, rows: 1},
		{name: "unknown nested fields", payload: `{"unknown":{"rows":[{"rows":[]}]},"rows":[[1,"Ali"]],"query":"SELECT 1","success":true}`, rows: 1},
		{name: "empty rows", payload: `{"rows":[],"query":"SELECT 1","success":true}`},
		{name: "omitted rows", payload: `{"query":"SELECT 1","success":true}`},
		{name: "null rows", payload: `{"rows":null,"query":"SELECT 1","success":true}`},
		{name: "whitespace", payload: " \n { \"query\" : \"SELECT 1\", \"rows\" : [[1,\"Ali\"]], \"success\":true } \t\n", rows: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := readQueryLogPreview(writeQueryLogTestFixture(t, tt.payload))
			require.NoError(t, err)
			assert.Equal(t, "SELECT 1", got.Query)
			assert.True(t, got.Success)
			require.Len(t, got.Rows, tt.rows)
			if tt.rows > 0 {
				assert.Equal(t, []any{json.Number("1"), "Ali"}, got.Rows[0])
			}
		})
	}
}

func TestReadQueryLogPreviewRejectsMalformedDocuments(t *testing.T) {
	t.Parallel()

	skippedPrefix := `{"rows":[` + strings.Repeat(`[1],`, aggregatedQueryLogMaxRows)
	tests := []struct {
		name    string
		payload string
	}{
		{name: "empty", payload: ""},
		{name: "array document", payload: `[]`},
		{name: "null document", payload: `null`},
		{name: "truncated metadata", payload: `{"query":"unfinished`},
		{name: "truncated row", payload: `{"rows":[[1,"unfinished`},
		{name: "missing rows closing bracket", payload: `{"rows":[[1]}`},
		{name: "missing object closing brace", payload: `{"query":"SELECT 1","rows":[[1]]`},
		{name: "wrong object closing delimiter", payload: `{"query":"SELECT 1"]`},
		{name: "trailing comma", payload: `{"rows":[[1],]}`},
		{name: "rows is object", payload: `{"rows":{}}`},
		{name: "row is scalar", payload: `{"rows":[42]}`},
		{name: "invalid skipped row", payload: skippedPrefix + `{"broken":]}`},
		{name: "truncated skipped row", payload: skippedPrefix + `["unfinished`},
		{name: "truncated metadata after skipped rows", payload: skippedPrefix + `[21]],"success":`},
		{name: "second json document", payload: `{"rows":[]} {"success":true}`},
		{name: "trailing garbage", payload: `{"rows":[]} garbage`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := readQueryLogPreview(writeQueryLogTestFixture(t, tt.payload))
			require.Error(t, err)
		})
	}
}

func TestReadQueryLogPreviewCapsRowsAndPreservesPrecision(t *testing.T) {
	t.Parallel()

	for _, count := range []int{0, 1, 19, 20, 21, 2000} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			rows := make([][]any, count)
			for i := range rows {
				rows[i] = []any{json.Number("9007199254740993"), map[string]any{"n": json.Number("18446744073709551615")}, i}
			}
			payload, err := json.Marshal(rows)
			require.NoError(t, err)
			path := writeQueryLogTestFixture(t, `{"rows":`+string(payload)+`,"query":"SELECT n","limit":9007199254740993,"success":false,"error":"query failed","description":"after rows"}`)
			got, err := readQueryLogPreview(path)
			require.NoError(t, err)
			require.Len(t, got.Rows, min(count, aggregatedQueryLogMaxRows))
			assert.Equal(t, int64(9007199254740993), got.Limit)
			assert.Equal(t, "after rows", got.Description)
			assert.False(t, got.Success)
			assert.Equal(t, "query failed", got.Error)
			for i, row := range got.Rows {
				assert.Equal(t, []any{json.Number("9007199254740993"), map[string]any{"n": json.Number("18446744073709551615")}, json.Number(strconv.Itoa(i))}, row)
			}
		})
	}
}

func TestWriteAggregatedQueryLogSkipsIncompleteAndInvalidFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	files := map[string]string{
		"query_1.json":     `{"query":"first","timestamp":"2026-11-03T10:00:00Z","rows":[[9007199254740993,"Ali"]],"success":true}`,
		"query_2.json":     `{"query":"second","timestamp":"2026-11-03T11:00:00Z","success":false,"error":"failed"}`,
		"partial.json":     `{"query":"partial","rows":[[1]]`,
		"broken.json":      `{"rows":[[1],]}`,
		"stream.json.tmp":  `{"query":"in progress","rows":[[1]]}`,
		"ignored.txt":      `{"query":"ignored"}`,
		"another.json.tmp": `{"query":`,
	}
	for name, payload := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(payload), 0o600))
	}
	require.NoError(t, os.Mkdir(filepath.Join(dir, "directory.json"), 0o755))
	target := filepath.Join(dir, "aggregate.json")
	for range 2 {
		require.NoError(t, writeAggregatedQueryLog(dir, target))
		file, err := os.Open(target)
		require.NoError(t, err)
		dec := json.NewDecoder(file)
		dec.UseNumber()
		var got []QueryLog
		err = dec.Decode(&got)
		require.NoError(t, file.Close())
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, "first", got[0].Query)
		assert.Equal(t, "second", got[1].Query)
		assert.Equal(t, [][]any{{json.Number("9007199254740993"), "Ali"}}, got[0].Rows)
		assert.Equal(t, "failed", got[1].Error)
	}
}

func TestWriteAggregatedQueryLogConcurrentRefreshes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "query.json"), []byte(`{"query":"SELECT 1","rows":[[1]],"success":true}`), 0o600))
	target := filepath.Join(t.TempDir(), "aggregate.json")
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for range cap(errors) {
		wg.Go(func() { errors <- writeAggregatedQueryLog(dir, target) })
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	payload, err := os.ReadFile(target)
	require.NoError(t, err)
	var logs []QueryLog
	require.NoError(t, json.Unmarshal(payload, &logs))
	require.Len(t, logs, 1)
	assert.Equal(t, "SELECT 1", logs[0].Query)
	leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(target), "*.tmp-*"))
	require.NoError(t, err)
	assert.Empty(t, leftovers)
}

func TestQueryLogWriterRefreshesConfiguredAggregate(t *testing.T) {
	dir := newQueryLogTestRepo(t)
	target := filepath.Join(dir, "aggregate.json")
	t.Setenv(aggregatedQueryLogPathEnv, target)
	log := openTestQueryLog(t, dir)
	for i := range 30 {
		log.writeRow([]any{i})
	}
	require.NoError(t, log.finish(nil))
	payload, err := os.ReadFile(target)
	require.NoError(t, err)
	var got []QueryLog
	require.NoError(t, json.Unmarshal(payload, &got))
	require.Len(t, got, 1)
	assert.Len(t, got[0].Rows, aggregatedQueryLogMaxRows)
	assert.Len(t, decodeTestQueryLog(t, log.path).Rows, 30)
}

func TestQueryLogWriterAggregateFailureDoesNotFailQueryLog(t *testing.T) {
	dir := newQueryLogTestRepo(t)
	blocked := filepath.Join(dir, "blocked")
	require.NoError(t, os.WriteFile(blocked, []byte("file"), 0o600))
	t.Setenv(aggregatedQueryLogPathEnv, filepath.Join(blocked, "aggregate.json"))
	log := openTestQueryLog(t, dir)
	log.writeRow([]any{1, "Ali"})
	require.NoError(t, log.finish(nil))
	assert.True(t, decodeTestQueryLog(t, log.path).Success)
}

func TestQueryLogWriterCapsRowsFromEnv(t *testing.T) {
	t.Setenv(queryLogMaxRowsEnv, "2")

	dir := newQueryLogTestRepo(t)
	log := openTestQueryLog(t, dir)
	for i := range 5 {
		log.writeRow([]any{i})
	}
	require.NoError(t, log.finish(nil))

	got := decodeTestQueryLog(t, log.path)
	assert.Equal(t, [][]any{{json.Number("0")}, {json.Number("1")}}, got.Rows)
	assert.Equal(t, 5, got.RowCount)
	assert.True(t, got.RowsTruncated)
	assert.True(t, got.Success)

	preview, err := readQueryLogPreview(log.path)
	require.NoError(t, err)
	assert.Equal(t, 5, preview.RowCount)
	assert.True(t, preview.RowsTruncated)
}

func TestQueryLogWriterKeepsRowsUnderEnvCap(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "abc", "5"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(queryLogMaxRowsEnv, value)

			log := openTestQueryLog(t, newQueryLogTestRepo(t))
			log.writeRow([]any{1})
			log.writeRow([]any{2})
			require.NoError(t, log.finish(nil))

			raw, err := os.ReadFile(log.path)
			require.NoError(t, err)
			assert.NotContains(t, string(raw), "row_count")
			assert.NotContains(t, string(raw), "rows_truncated")
			assert.Len(t, decodeTestQueryLog(t, log.path).Rows, 2)
		})
	}
}
