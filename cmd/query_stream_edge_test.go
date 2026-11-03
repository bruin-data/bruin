package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

type queryStreamTestFunc func(context.Context, *query.Query, query.RowHandler) (*query.QueryResult, error)

func (f queryStreamTestFunc) StreamWithSchema(ctx context.Context, q *query.Query, h query.RowHandler) (*query.QueryResult, error) {
	return f(ctx, q, h)
}

func queryStreamTestRows(count int, terminalErr error) queryStreamTestFunc {
	return func(_ context.Context, _ *query.Query, h query.RowHandler) (*query.QueryResult, error) {
		result := &query.QueryResult{Columns: []string{"id", "value"}, ColumnTypes: []string{"BIGINT", "VARCHAR"}}
		if err := h.Schema(result.Columns, result.ColumnTypes); err != nil {
			return nil, err
		}
		for i := range count {
			if err := h.Row([]any{i, fmt.Sprintf("value_%d", i)}); err != nil {
				return nil, err
			}
		}
		return result, terminalErr
	}
}

type queryStreamTestOutput struct {
	stdout string
	stderr string
	err    error
}

func captureQueryStreamOutput(t *testing.T, closedStdout bool, run func() error) queryStreamTestOutput {
	t.Helper()
	dir := t.TempDir()
	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = stdout.Close() })
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = stderr.Close() })
	oldOut, oldErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = stdout, stderr
	defer func() { os.Stdout, os.Stderr = oldOut, oldErr }()
	if closedStdout {
		require.NoError(t, stdout.Close())
	}
	runErr := run()
	out, err := os.ReadFile(stdout.Name())
	require.NoError(t, err)
	diagnostics, err := os.ReadFile(stderr.Name())
	require.NoError(t, err)
	return queryStreamTestOutput{stdout: string(out), stderr: string(diagnostics), err: runErr}
}

func runQueryStreamTest(t *testing.T, mode, dir string, closedStdout bool, streamer query.RowStreamer) queryStreamTestOutput {
	t.Helper()
	return captureQueryStreamOutput(t, closedStdout, func() error {
		command := &cli.Command{
			Name: "stream-test",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "output", Value: mode},
			},
			ExitErrHandler: func(context.Context, *cli.Command, error) {},
			Action: func(ctx context.Context, c *cli.Command) error {
				return runStreamedQuery(ctx, c, streamer, &query.Query{Query: "SELECT edge"}, "interface-test", "SELECT edge", QueryLogOptions{Asset: dir})
			},
		}
		return command.Run(t.Context(), []string{"stream-test"})
	})
}

func requireStreamTestRows(t *testing.T, output, mode string, count int) {
	t.Helper()
	want := make([][]string, count)
	for i := range count {
		want[i] = []string{strconv.Itoa(i), fmt.Sprintf("value_%d", i)}
	}
	var got [][]string
	switch {
	case mode == "json":
		var doc struct {
			Rows [][]json.RawMessage `json:"rows"`
		}
		require.NoError(t, json.Unmarshal([]byte(output), &doc))
		got = make([][]string, len(doc.Rows))
		for i, row := range doc.Rows {
			require.Len(t, row, 2)
			var value string
			require.NoError(t, json.Unmarshal(row[1], &value))
			got[i] = []string{string(row[0]), value}
		}
	case mode == outputFormatPlain && count <= plainTableMaxRows:
		got = regexp.MustCompile(`│\s*(\d+)\s*│\s*(value_\d+)\s*│`).FindAllStringSubmatch(output, -1)
		for i, row := range got {
			got[i] = row[1:]
		}
		if count == 0 {
			assert.Equal(t, "No data available\n", output)
			got = [][]string{}
		}
	default:
		records, err := csv.NewReader(strings.NewReader(output)).ReadAll()
		require.NoError(t, err)
		require.NotEmpty(t, records)
		require.Equal(t, []string{"id", "value"}, records[0])
		got = records[1:]
	}
	require.Equal(t, want, got)
}

func TestPlainSinkPreservesReusedRows(t *testing.T) {
	t.Parallel()
	for _, count := range []int{1, 99, 100, 101, 105} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer
			sink := &plainSink{w: bufio.NewWriter(&out)}
			require.NoError(t, sink.writeHeader([]string{"id", "value"}, nil))
			row := make([]any, 2)
			for i := range count {
				row[0], row[1] = i, fmt.Sprintf("value_%d", i)
				require.NoError(t, sink.writeRow(row))
			}
			row[0], row[1] = -1, "overwritten"
			require.NoError(t, sink.finish(&query.QueryResult{}))
			requireStreamTestRows(t, out.String(), outputFormatPlain, count)
		})
	}
}

func TestRunStreamedQueryReportsOutputFailures(t *testing.T) {
	t.Setenv(aggregatedQueryLogPathEnv, "")
	for _, mode := range []string{outputFormatPlain, "csv", "json"} {
		for _, count := range []int{0, 1, 101, 4097} {
			t.Run(fmt.Sprintf("%s/%d", mode, count), func(t *testing.T) {
				t.Setenv(aggregatedQueryLogPathEnv, "")
				got := runQueryStreamTest(t, mode, newQueryLogTestRepo(t), true, queryStreamTestRows(count, nil))
				var exit cli.ExitCoder
				require.ErrorAs(t, got.err, &exit)
				assert.Equal(t, 1, exit.ExitCode())
				assert.Contains(t, got.stderr, "Error:")
				assert.Empty(t, got.stdout)
			})
		}
	}
}

func TestRunStreamedQueryInterfaceBoundaries(t *testing.T) {
	t.Setenv(aggregatedQueryLogPathEnv, "")
	for _, mode := range []string{outputFormatPlain, "csv", "json"} {
		for _, count := range []int{0, 1, 19, 20, 21, 99, 100, 101, 105, 4097} {
			t.Run(fmt.Sprintf("%s/%d", mode, count), func(t *testing.T) {
				dir := newQueryLogTestRepo(t)
				aggregatePath := filepath.Join(dir, "aggregate.json")
				t.Setenv(aggregatedQueryLogPathEnv, aggregatePath)
				got := runQueryStreamTest(t, mode, dir, false, queryStreamTestRows(count, nil))
				require.NoError(t, got.err)
				assert.Empty(t, got.stderr)
				requireStreamTestRows(t, got.stdout, mode, count)
				paths, err := filepath.Glob(filepath.Join(dir, "logs/queries/*.json"))
				require.NoError(t, err)
				require.Len(t, paths, 1)
				log := decodeTestQueryLog(t, paths[0])
				assert.True(t, log.Success)
				require.Len(t, log.Rows, count)
				for i, row := range log.Rows {
					assert.Equal(t, []any{json.Number(strconv.Itoa(i)), fmt.Sprintf("value_%d", i)}, row)
				}
				payload, err := os.ReadFile(aggregatePath)
				require.NoError(t, err)
				var aggregate []QueryLog
				require.NoError(t, json.Unmarshal(payload, &aggregate))
				require.Len(t, aggregate, 1)
				assert.Len(t, aggregate[0].Rows, min(count, aggregatedQueryLogMaxRows))
				pending, err := filepath.Glob(filepath.Join(dir, "logs/queries/*.tmp"))
				require.NoError(t, err)
				assert.Empty(t, pending)
			})
		}
	}
}

func TestRunStreamedQueryPreservesPartialResultsOnError(t *testing.T) {
	t.Setenv(aggregatedQueryLogPathEnv, "")
	cases := []struct {
		name  string
		count int
		err   error
	}{
		{name: "before schema", count: -1, err: assert.AnError},
		{name: "after schema", count: 0, err: context.Canceled},
		{name: "first row", count: 1, err: context.DeadlineExceeded},
		{name: "preview boundary", count: 20, err: assert.AnError},
		{name: "table boundary", count: 100, err: context.Canceled},
		{name: "CSV transition", count: 101, err: context.DeadlineExceeded},
		{name: "after transition", count: 105, err: assert.AnError},
	}
	for _, mode := range []string{outputFormatPlain, "csv", "json"} {
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				t.Setenv(aggregatedQueryLogPathEnv, "")
				dir := newQueryLogTestRepo(t)
				streamer := queryStreamTestRows(tc.count, tc.err)
				if tc.count < 0 {
					streamer = func(context.Context, *query.Query, query.RowHandler) (*query.QueryResult, error) {
						return nil, tc.err
					}
				}
				got := runQueryStreamTest(t, mode, dir, false, streamer)
				var exit cli.ExitCoder
				require.ErrorAs(t, got.err, &exit)
				assert.Equal(t, 1, exit.ExitCode())
				switch {
				case mode == "json":
					var doc struct {
						Error string `json:"error"`
					}
					require.NoError(t, json.Unmarshal([]byte(got.stdout), &doc))
					assert.Contains(t, doc.Error, "query execution failed: "+tc.err.Error())
					requireStreamTestRows(t, got.stdout, mode, max(tc.count, 0))
					assert.Empty(t, got.stderr)
				case (mode == "csv" && tc.count >= 0) || tc.count > plainTableMaxRows:
					requireStreamTestRows(t, got.stdout, "csv", tc.count)
					assert.Contains(t, got.stderr, tc.err.Error())
					assert.NotContains(t, got.stdout, "Error:")
				default:
					assert.Contains(t, got.stdout, tc.err.Error())
					assert.NotContains(t, got.stdout, "value_")
					assert.Empty(t, got.stderr)
				}
				paths, err := filepath.Glob(filepath.Join(dir, "logs/queries/*.json"))
				require.NoError(t, err)
				require.Len(t, paths, 1)
				log := decodeTestQueryLog(t, paths[0])
				assert.False(t, log.Success)
				assert.Equal(t, tc.err.Error(), log.Error)
				assert.Len(t, log.Rows, max(tc.count, 0))
			})
		}
	}
}

func TestJSONSinkStreamsBeforeFinish(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	sink := &jsonSink{w: bufio.NewWriterSize(&out, 128), connName: "conn\"\n🦆", queryStr: "SELECT '<&>\\\u96ea'"}
	require.NoError(t, sink.writeHeader([]string{"id", "value"}, []string{"BIGINT", "VARCHAR"}))
	value := strings.Repeat("\u96ea\"\\\n\r\t\x00</script>", 1024)
	row := []any{int64(9007199254740993), value}
	require.NoError(t, sink.writeRow(row))
	assert.Positive(t, out.Len())
	assert.False(t, json.Valid(out.Bytes()))
	row[0], row[1] = 2, nil
	require.NoError(t, sink.writeRow(row))
	require.NoError(t, sink.finish(&query.QueryResult{}))
	decoder := json.NewDecoder(&out)
	decoder.UseNumber()
	var doc struct {
		Rows       [][]any `json:"rows"`
		Connection string  `json:"connectionName"`
		Query      string  `json:"query"`
	}
	require.NoError(t, decoder.Decode(&doc))
	assert.Equal(t, [][]any{{json.Number("9007199254740993"), value}, {json.Number("2"), nil}}, doc.Rows)
	assert.Equal(t, sink.connName, doc.Connection)
	assert.Equal(t, sink.queryStr, doc.Query)
}

func TestJSONSinkRejectsInvalidRowsWithoutCorruptingOutput(t *testing.T) {
	t.Parallel()
	cycle := map[string]any{}
	cycle["self"] = cycle
	cases := []struct {
		name string
		cell any
	}{
		{name: "NaN", cell: math.NaN()},
		{name: "infinity", cell: math.Inf(1)},
		{name: "invalid JSON", cell: json.RawMessage(`{"broken":`)},
		{name: "cyclic value", cell: cycle},
	}
	for _, tc := range cases {
		for _, count := range []int{0, 21, 101} {
			t.Run(fmt.Sprintf("%s/%d", tc.name, count), func(t *testing.T) {
				t.Parallel()
				var out bytes.Buffer
				sink := &jsonSink{w: bufio.NewWriter(&out)}
				log := openStreamTestQueryLog(t)
				handler := &streamHandler{sink: sink, log: log}
				require.NoError(t, handler.Schema([]string{"id", "value"}, []string{"BIGINT", "VARCHAR"}))
				for i := range count {
					require.NoError(t, handler.Row([]any{i, fmt.Sprintf("value_%d", i)}))
				}
				err := handler.Row([]any{count, tc.cell})
				require.ErrorContains(t, err, "failed to marshal result to JSON")
				assert.Same(t, err, handler.sinkErr)
				require.Error(t, sink.abort(err))
				requireStreamTestRows(t, out.String(), "json", count)
				var doc map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(out.Bytes(), &doc))
				assert.Contains(t, string(doc["error"]), "failed to marshal result to JSON")
				assert.Equal(t, count, sink.rows)
				require.Error(t, log.finish(err))
				assert.NoFileExists(t, log.path)
				assert.NoFileExists(t, log.path+".tmp")
			})
		}
	}
}

func TestRunStreamedQueryLogFailureDoesNotInterruptOutput(t *testing.T) {
	t.Setenv(aggregatedQueryLogPathEnv, "")
	for _, mode := range []string{outputFormatPlain, "csv", "json"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(aggregatedQueryLogPathEnv, "")
			dir := newQueryLogTestRepo(t)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "logs"), []byte("blocked"), 0o600))
			got := runQueryStreamTest(t, mode, dir, false, queryStreamTestRows(105, nil))
			require.NoError(t, got.err)
			assert.Contains(t, got.stderr, "Warning: failed to save query log")
			requireStreamTestRows(t, got.stdout, mode, 105)
		})
	}
}

func TestExportSinkCleansUpIOFailures(t *testing.T) {
	t.Setenv(aggregatedQueryLogPathEnv, "")
	for _, stage := range []string{"row write", "final flush", "rotation flush", "next file"} {
		t.Run(stage, func(t *testing.T) {
			t.Setenv(aggregatedQueryLogPathEnv, "")
			dir := t.TempDir()
			keep := filepath.Join(dir, "unrelated.txt")
			require.NoError(t, os.WriteFile(keep, []byte("preserve"), 0o600))
			sink := newExportSink("json", dir, 1, 123)
			require.NoError(t, sink.writeHeader([]string{"value"}, nil))
			require.NoError(t, sink.writeRow([]any{"first"}))
			var writeErr error
			switch stage {
			case "row write":
				sink.splitRows = 0
				require.NoError(t, sink.file.Close())
				writeErr = sink.writeRow([]any{strings.Repeat("x", 16384)})
			case "final flush":
				require.NoError(t, sink.file.Close())
				writeErr = sink.finish(&query.QueryResult{})
			case "rotation flush":
				require.NoError(t, sink.file.Close())
				writeErr = sink.writeRow([]any{"second"})
			case "next file":
				require.NoError(t, os.Mkdir(filepath.Join(dir, "query_result_123_part2.csv"), 0o755))
				writeErr = sink.writeRow([]any{"second"})
			}
			require.Error(t, writeErr)
			got := captureQueryStreamOutput(t, false, func() error { return sink.abort(writeErr) })
			require.Error(t, got.err)
			assert.True(t, json.Valid([]byte(got.stdout)))
			for _, path := range sink.paths {
				assert.NoFileExists(t, path)
			}
			content, err := os.ReadFile(keep)
			require.NoError(t, err)
			assert.Equal(t, "preserve", string(content))
			if stage == "next file" {
				assert.DirExists(t, filepath.Join(dir, "query_result_123_part2.csv"))
			}
		})
	}
}

func TestRunStreamedQueryExecutionSummary(t *testing.T) {
	t.Setenv(aggregatedQueryLogPathEnv, "")
	for _, mode := range []string{outputFormatPlain, "csv", "json"} {
		for _, closed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/closed=%t", mode, closed), func(t *testing.T) {
				t.Setenv(aggregatedQueryLogPathEnv, "")
				streamer := queryStreamTestFunc(func(context.Context, *query.Query, query.RowHandler) (*query.QueryResult, error) {
					affected := int64(42)
					return &query.QueryResult{Execution: &query.QueryExecutionSummary{StatementType: "INSERT", DMLAffectedRows: &affected}}, nil
				})
				got := runQueryStreamTest(t, mode, newQueryLogTestRepo(t), closed, streamer)
				if closed {
					var exit cli.ExitCoder
					require.ErrorAs(t, got.err, &exit)
					assert.Equal(t, 1, exit.ExitCode())
					assert.Contains(t, got.stderr, "Error:")
					return
				}
				require.NoError(t, got.err)
				assert.Empty(t, got.stderr)
				switch mode {
				case "json":
					var doc struct {
						Rows      [][]any                     `json:"rows"`
						Execution query.QueryExecutionSummary `json:"execution"`
					}
					require.NoError(t, json.Unmarshal([]byte(got.stdout), &doc))
					assert.Empty(t, doc.Rows)
					assert.Equal(t, "INSERT", doc.Execution.StatementType)
					require.NotNil(t, doc.Execution.DMLAffectedRows)
					assert.Equal(t, int64(42), *doc.Execution.DMLAffectedRows)
				case "csv":
					records, err := csv.NewReader(strings.NewReader(got.stdout)).ReadAll()
					require.NoError(t, err)
					assert.Equal(t, [][]string{{"metric", "value"}, {"Statement type", "INSERT"}, {"Rows affected", "42"}}, records)
				default:
					assert.Equal(t, "Statement executed successfully\nStatement type: INSERT\nRows affected: 42\n", got.stdout)
				}
			})
		}
	}
}
