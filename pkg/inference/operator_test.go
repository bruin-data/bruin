package inference

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

type inputConnection struct{ result *query.QueryResult }

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
	return &pipeline.Asset{
		Name: "analytics.classified", Type: pipeline.AssetTypeInference, Connection: "warehouse",
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyMerge},
		Columns:         []pipeline.Column{{Name: "id", PrimaryKey: true}, {Name: "category", Type: "string"}},
		Parameters: pipeline.ParameterMap{
			"provider": "opencode", "model": "test-model", "input_query": "select id, body from tickets",
			"prompt": "Classify: {{ row.body }}", "output_column": "category", "allowed_values": []string{"billing", "technical"},
		},
	}
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
	op := NewOperator(conn)
	op.cacheDir = t.TempDir()
	op.runner = runner
	return op, &scheduler.AssetInstance{Asset: asset, Pipeline: &pipeline.Pipeline{Name: "test"}}, conn, runner
}

func TestOperatorCacheAndMaterialization(t *testing.T) {
	t.Parallel()
	op, ti, conn, runner := fixture(t)
	calls := 0
	op.complete = func(_ context.Context, _ *Client, prompt string) (string, error) {
		calls++
		if strings.Contains(prompt, "charged twice") {
			return "billing", nil
		}
		return "technical", nil
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
	ti.Asset.Parameters["prompt"] = "Updated prompt: {{ row.body }}"
	require.NoError(t, op.Run(t.Context(), ti))
	require.Equal(t, 7, calls)
	ti.Asset.Materialization.Strategy = pipeline.MaterializationStrategyCreateReplace
	require.NoError(t, op.Run(t.Context(), ti))
	require.Equal(t, 7, calls, "table rebuilds reuse inference")
	require.Len(t, runner.rows, 2, "replace must publish cached rows as well")
	require.Contains(t, strings.Join(runner.args, " "), "--incremental-strategy replace")
	ti.Asset.Parameters["force"] = true
	require.NoError(t, op.Run(t.Context(), ti))
	require.Equal(t, 9, calls)
}

func TestOperatorResumesFailureWithoutPublishingPartialResults(t *testing.T) {
	t.Parallel()
	op, ti, _, runner := fixture(t)
	calls := 0
	op.complete = func(context.Context, *Client, string) (string, error) {
		calls++
		if calls == 2 {
			return "", errors.New("unavailable")
		}
		return "billing", nil
	}
	require.ErrorContains(t, op.Run(t.Context(), ti), "input row 2")
	require.Zero(t, runner.calls)
	// A new operator simulates a process restart; only its disk cache survives.
	restarted := NewOperator(op.conn)
	restarted.cacheDir, restarted.runner, restarted.complete = op.cacheDir, runner, op.complete
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
	for _, name := range []string{"duplicate", "null", "missing", "too many", "collision"} {
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
			}
			op.complete = func(context.Context, *Client, string) (string, error) { t.Fatal("unexpected call"); return "", nil }
			require.Error(t, op.Run(t.Context(), ti))
			require.Zero(t, runner.calls)
		})
	}
}

func TestOperatorRejectsInvalidOutputAndEmptyReplace(t *testing.T) {
	t.Parallel()
	op, ti, conn, runner := fixture(t)
	calls := 0
	op.complete = func(context.Context, *Client, string) (string, error) { calls++; return "unrecognized", nil }
	require.ErrorContains(t, op.Run(t.Context(), ti), "allowed_values")
	require.ErrorContains(t, op.Run(t.Context(), ti), "allowed_values")
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
	for _, name := range []string{"provider", "prompt", "max_rows", "allowed_values", "key", "view", "append", "connection", "output", "unknown"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			a := testAsset()
			switch name {
			case "provider":
				a.Parameters["provider"] = "unknown"
			case "prompt":
				delete(a.Parameters, "prompt")
			case "max_rows":
				a.Parameters["max_rows"] = 0
			case "allowed_values":
				a.Parameters["allowed_values"] = []int{1}
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

func TestOperatorParallelRequestsPreserveRows(t *testing.T) {
	t.Parallel()
	op, ti, conn, runner := fixture(t)
	ti.Asset.Parameters["extract_parallelism"] = 2
	ti.Asset.Parameters["prompt"] = "{{ row.body }}"
	delete(ti.Asset.Parameters, "allowed_values")
	conn.result.Rows = [][]any{{1, "first"}, {7, "second"}, {9, "third"}, {12, "fourth"}}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := make(chan string, 4)
	release := map[string]chan struct{}{}
	for _, name := range []string{"first", "second", "third", "fourth"} {
		release[name] = make(chan struct{})
	}
	var active, peak atomic.Int64
	op.complete = func(ctx context.Context, _ *Client, prompt string) (string, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		started <- prompt
		select {
		case <-release[prompt]:
			return "result-" + prompt, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() { done <- op.Run(ctx, ti) }()
	next := func() string {
		t.Helper()
		select {
		case name := <-started:
			return name
		case <-ctx.Done():
			t.Fatal("requests did not run concurrently")
			return ""
		}
	}
	require.ElementsMatch(t, []string{"first", "second"}, []string{next(), next()})
	// Keep the first row blocked while every later row finishes.
	close(release["second"])
	require.Equal(t, "third", next())
	close(release["third"])
	require.Equal(t, "fourth", next())
	close(release["fourth"])
	close(release["first"])
	require.NoError(t, <-done)
	require.Equal(t, int64(2), peak.Load())
	require.Len(t, runner.rows, 4)
	for i, name := range []string{"first", "second", "third", "fourth"} {
		require.Equal(t, name, runner.rows[i]["body"])
		require.Equal(t, "result-"+name, runner.rows[i]["category"])
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
	op.complete = func(ctx context.Context, _ *Client, prompt string) (string, error) {
		calls.Add(1)
		if strings.Contains(prompt, "charged twice") {
			select {
			case <-started:
				return "", errors.New("provider failed")
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
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
	require.Equal(t, 4, cfg.parallelism)
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

func TestZenLive(t *testing.T) {
	t.Parallel()
	if os.Getenv("BRUIN_INFERENCE_LIVE_TEST") != "1" {
		t.Skip("set BRUIN_INFERENCE_LIVE_TEST=1 to call the paid Zen Muse Spark model with synthetic data")
	}
	c := &Client{Provider: "opencode", Model: "muse-spark-1.3", APIKey: os.Getenv("OPENCODE_API_KEY")}
	text, err := c.Complete(t.Context(), "Classify this synthetic ticket. Reply with exactly one word: billing or technical. Ticket: I was charged twice for my subscription.")
	require.NoError(t, err)
	require.Equal(t, "billing", text)
}
