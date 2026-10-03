package inference

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

type inputConnection struct{ result *query.QueryResult }

func TestPromptCacheUsageIsSeparateFromSavedResults(t *testing.T) {
	t.Parallel()
	op, ti, _, _ := fixture(t)
	ti.Asset.Parameters["extract_parallelism"] = 16
	op.structured = func(ctx context.Context, c *Client, state, instructions string, columns []outputColumn) (map[string]any, error) {
		c.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			body := responseText(`{"category":"billing"}`)
			body = strings.TrimSuffix(body, "}") + `,"usage":{"input_tokens_details":{"cached_tokens":1200,"cache_write_tokens":300}}}`
			return response(http.StatusOK, body), nil
		})}
		return c.CompleteStructured(ctx, state, instructions, columns)
	}
	var out bytes.Buffer
	ctx := context.WithValue(t.Context(), executor.KeyPrinter, &out)
	require.NoError(t, op.Run(ctx, ti))
	require.Contains(t, out.String(), "2400 tokens read, 600 tokens written (usage reported by 2/2 model calls)")
	out.Reset()
	require.NoError(t, op.Run(ctx, ti))
	require.Contains(t, out.String(), "0 model calls, 2 cached results")
	require.NotContains(t, out.String(), "Provider prompt cache")
	out.Reset()
	ti.Asset.Parameters["cache"] = false
	require.NoError(t, op.Run(ctx, ti))
	require.Contains(t, out.String(), "2400 tokens read, 600 tokens written (usage reported by 2/2 model calls)")
	require.Contains(t, out.String(), "2 model calls, 0 cached results")
}

func (c *inputConnection) GetConnection(string) any       { return c }
func (c *inputConnection) GetIngestrURI() (string, error) { return "duckdb:///test.db", nil }
func (c *inputConnection) SelectWithSchema(context.Context, *query.Query) (*query.QueryResult, error) {
	return c.result, nil
}

type captureRunner struct {
	rows  []map[string]any
	args  []string
	err   error
	calls int
}

func (r *captureRunner) RunIngestr(_ context.Context, args, _ []string, _ *git.Repo) error {
	r.calls++
	r.args = args
	r.rows = nil
	for i, arg := range args {
		if arg != "--source-uri" {
			continue
		}
		f, err := os.Open(strings.TrimPrefix(args[i+1], "mmap://"))
		if err != nil {
			return err
		}
		defer f.Close()
		reader, err := ipc.NewFileReader(f)
		if err != nil {
			return err
		}
		defer reader.Close()
		for batch := range reader.NumRecords() {
			record, err := reader.RecordBatchAt(batch)
			if err != nil {
				return err
			}
			for j := range int(record.NumRows()) {
				row := make(map[string]any)
				for k, field := range record.Schema().Fields() {
					row[field.Name] = record.Column(k).GetOneForMarshal(j)
				}
				r.rows = append(r.rows, row)
			}
			record.Release()
		}
	}
	return r.err
}

func testAsset() *pipeline.Asset {
	asset := &pipeline.Asset{
		Name: "analytics.classified", Type: pipeline.AssetTypeInference, Connection: "warehouse",
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyMerge},
		Columns:         []pipeline.Column{{Name: "id", PrimaryKey: true}, {Name: "category", Type: "string"}},
		Parameters: pipeline.ParameterMap{
			"provider": "opencode", "model": "test-model", "input_query": "select id, body from tickets",
			"context": "{{ row.body }}",
		},
	}
	asset.Columns[1].Inference = &pipeline.ColumnInference{Prompt: "Classify the ticket", Choices: map[string]string{"billing": "Billing issue", "technical": "Technical issue"}}
	return asset
}

func fixture(t *testing.T) (*Operator, *scheduler.AssetInstance, *inputConnection, *captureRunner) {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, ".git"), 0o700))
	asset := testAsset()
	// Cache/retry fixtures exercise deterministic row order; concurrency tests override this.
	asset.Parameters["extract_parallelism"] = 1
	asset.DefinitionFile.Path = filepath.Join(root, "tickets.asset.yml")
	conn := &inputConnection{result: &query.QueryResult{
		Columns: []string{"id", "body"}, ColumnTypes: []string{"bigint", "varchar"}, Rows: [][]any{{1, "charged twice"}, {7, "server is down"}},
	}}
	runner := &captureRunner{}
	op := NewOperator(testProviderConnections(t, conn, false))
	op.cacheDir = t.TempDir()
	op.runner = runner
	return op, &scheduler.AssetInstance{Asset: asset, Pipeline: &pipeline.Pipeline{Name: "test"}}, conn, runner
}

func TestOperatorCacheAndMaterialization(t *testing.T) {
	t.Parallel()
	op, ti, conn, runner := fixture(t)
	calls := 0
	op.structured = func(_ context.Context, _ *Client, state, _ string, _ []outputColumn) (map[string]any, error) {
		calls++
		if strings.Contains(state, "charged twice") {
			return map[string]any{"category": "billing"}, nil
		}
		return map[string]any{"category": "technical"}, nil
	}
	require.NoError(t, op.Run(t.Context(), ti))
	require.Equal(t, 2, calls)
	require.Equal(t, []map[string]any{
		{"id": int64(1), "body": "charged twice", "category": "billing"},
		{"id": int64(7), "body": "server is down", "category": "technical"},
	}, runner.rows)
	require.Contains(t, strings.Join(runner.args, " "), "--incremental-strategy merge")
	require.Contains(t, strings.Join(runner.args, " "), "--primary-key id")
	require.NotContains(t, ti.Asset.Parameters, "incremental_strategy")
	require.NoError(t, op.Run(t.Context(), ti))
	require.Equal(t, 2, calls, "same requests must reuse saved results")
	conn.result.Rows[0][1] = "cannot log in"
	require.NoError(t, op.Run(t.Context(), ti))
	require.Equal(t, 3, calls, "only the changed row should infer again")
	require.Equal(t, "technical", runner.rows[0]["category"])
	ti.Asset.Parameters["model"] = "another-model"
	require.NoError(t, op.Run(t.Context(), ti))
	require.Equal(t, 5, calls, "model changes invalidate both rows")
	ti.Asset.Columns[1].Inference.Prompt = "Updated classification prompt"
	require.NoError(t, op.Run(t.Context(), ti))
	require.Equal(t, 7, calls)
	ti.Asset.Materialization.Strategy = pipeline.MaterializationStrategyCreateReplace
	require.NoError(t, op.Run(t.Context(), ti))
	require.Equal(t, 7, calls, "table rebuilds reuse inference")
	require.Len(t, runner.rows, 2, "replace must publish cached rows as well")
	require.Contains(t, strings.Join(runner.args, " "), "--incremental-strategy replace")
	ti.Asset.Parameters["cache"] = false
	require.NoError(t, op.Run(t.Context(), ti))
	require.Equal(t, 9, calls)
}

func TestOperatorResumesFailureWithoutPublishingPartialResults(t *testing.T) {
	t.Parallel()
	op, ti, _, runner := fixture(t)
	calls := 0
	op.structured = func(context.Context, *Client, string, string, []outputColumn) (map[string]any, error) {
		calls++
		if calls == 2 {
			return nil, errors.New("unavailable")
		}
		return map[string]any{"category": "billing"}, nil
	}
	require.ErrorContains(t, op.Run(t.Context(), ti), "input row 2")
	require.Zero(t, runner.calls)
	// A new operator simulates a process restart; only its disk cache survives.
	restarted := NewOperator(op.conn)
	restarted.cacheDir, restarted.runner, restarted.structured = op.cacheDir, runner, op.structured
	runner.err = errors.New("destination unavailable")
	require.ErrorContains(t, restarted.Run(t.Context(), ti), "destination unavailable")
	require.Equal(t, 3, calls)
	runner.err = nil
	require.NoError(t, restarted.Run(t.Context(), ti))
	require.Equal(t, 3, calls, "failed destination writes must not repeat model calls")
	require.Len(t, runner.rows, 2)
}

func TestOperatorRejectsInvalidInputBeforeCallingModel(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"duplicate", "null", "missing", "too many", "collision", "case collision", "generated case collision"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			op, ti, conn, runner := fixture(t)
			switch name {
			case "duplicate":
				conn.result.Rows[1][0] = 1
			case "null":
				conn.result.Rows[1][0] = nil
			case "missing":
				conn.result.Columns[0] = "wrong_key"
			case "too many":
				ti.Asset.Parameters["max_rows"] = 1
			case "collision":
				conn.result.Columns[1] = "category"
			case "case collision":
				conn.result.Columns[1] = "Category"
			case "generated case collision":
				column := ti.Asset.Columns[1]
				column.Name = "Category"
				ti.Asset.Columns = append(ti.Asset.Columns, column)
			}
			op.structured = func(context.Context, *Client, string, string, []outputColumn) (map[string]any, error) {
				t.Fatal("unexpected call")
				return nil, nil
			}
			require.Error(t, op.Run(t.Context(), ti))
			require.Zero(t, runner.calls)
		})
	}
}

func TestOperatorRejectsInvalidOutputAndEmptyReplace(t *testing.T) {
	t.Parallel()
	op, ti, conn, runner := fixture(t)
	calls := 0
	op.structured = func(ctx context.Context, c *Client, state, instructions string, columns []outputColumn) (map[string]any, error) {
		c.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return response(http.StatusOK, responseText(`{"category":"unrecognized"}`)), nil
		})}
		return c.CompleteStructured(ctx, state, instructions, columns)
	}
	require.Error(t, op.Run(t.Context(), ti))
	require.Error(t, op.Run(t.Context(), ti))
	require.Equal(t, 2, calls, "invalid responses must not be cached")
	require.Zero(t, runner.calls)
	conn.result.Rows = nil
	require.NoError(t, op.Run(t.Context(), ti))
	ti.Asset.Materialization.Strategy = pipeline.MaterializationStrategyCreateReplace
	require.NoError(t, op.Run(t.Context(), ti))
	require.Equal(t, 1, runner.calls)
	require.Empty(t, runner.rows)
}

func TestValidateAsset(t *testing.T) {
	t.Parallel()
	require.NoError(t, ValidateAsset(testAsset()))
	for _, name := range []string{"provider", "context", "max_rows", "inference", "key", "view", "append", "connection", "output", "unknown"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			a := testAsset()
			switch name {
			case "provider":
				a.Parameters["provider"] = "unknown"
			case "context":
				delete(a.Parameters, "context")
			case "max_rows":
				a.Parameters["max_rows"] = 0
			case "inference":
				a.Columns[1].Inference = nil
			case "key":
				a.Columns[0].PrimaryKey = false
			case "view":
				a.Materialization.Type = pipeline.MaterializationTypeView
			case "append":
				a.Materialization.Strategy = pipeline.MaterializationStrategyAppend
			case "connection":
				a.Connection = ""
			case "output":
				a.Columns[1].Type = "integer"
			case "unknown":
				a.Parameters["max_row"] = 10
			}
			require.Error(t, ValidateAsset(a))
		})
	}
}

func TestOperatorParallelFailureCancelsRequests(t *testing.T) {
	t.Parallel()
	op, ti, conn, runner := fixture(t)
	ti.Asset.Parameters["extract_parallelism"] = 2
	conn.result.Rows = append(conn.result.Rows, []any{9, "must not start"})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	var calls atomic.Int64
	op.structured = func(ctx context.Context, _ *Client, state, _ string, _ []outputColumn) (map[string]any, error) {
		calls.Add(1)
		if strings.Contains(state, "charged twice") {
			select {
			case <-started:
				return nil, errors.New("provider failed")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	require.ErrorContains(t, op.Run(ctx, ti), "provider failed")
	require.NoError(t, ctx.Err(), "sibling should cancel before the parent deadline")
	require.Equal(t, int64(2), calls.Load())
	require.Zero(t, runner.calls)
}

func TestExtractParallelismConfig(t *testing.T) {
	t.Parallel()
	cfg, err := readConfig(testAsset())
	require.NoError(t, err)
	require.Equal(t, 16, cfg.parallelism)
	for _, value := range []any{0, -1, "no", 1.5, true} {
		asset := testAsset()
		asset.Parameters["extract_parallelism"] = value
		require.ErrorContains(t, ValidateAsset(asset), "extract_parallelism must be a positive integer")
	}
	asset := testAsset()
	asset.Parameters["extract_parallelism"] = "10000"
	cfg, err = readConfig(asset)
	require.NoError(t, err)
	require.Equal(t, 10000, cfg.parallelism, "configured concurrency must not be clamped")
}
