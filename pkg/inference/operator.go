package inference

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/ingestruri"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/gofrs/flock"
	"golang.org/x/sync/errgroup"
)

type ingestrRunner interface {
	RunIngestr(ctx context.Context, args, extraPackages []string, repo *git.Repo) error
}

// Operator enriches query results and publishes them through Bruin's ingestr writer.
type Operator struct {
	conn     config.ConnectionGetter
	runner   ingestrRunner
	complete func(context.Context, *Client, string) (string, error)
	cacheDir string
}

func NewOperator(conn config.ConnectionGetter) *Operator {
	return &Operator{
		conn: conn,
		runner: &python.UvPythonRunner{
			UvInstaller: &python.UvChecker{}, IngestrInstaller: &python.IngestrChecker{}, Cmd: &python.CommandRunner{},
		},
		complete: func(ctx context.Context, c *Client, prompt string) (string, error) { return c.Complete(ctx, prompt) },
	}
}

func (o *Operator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	asset := ti.GetAsset()
	cfg, err := readConfig(asset)
	if err != nil {
		return err
	}
	client := &Client{Provider: cfg.provider, Model: cfg.model, APIKey: os.Getenv(cfg.keyEnv), MaxOutputTokens: cfg.maxTokens}
	if client.APIKey == "" && cfg.provider != "opencode" {
		return fmt.Errorf("inference requires environment variable %s", cfg.keyEnv)
	}
	conn := o.conn.GetConnection(asset.Connection)
	inputQuery, err := resolveInputQuery(ti.GetPipeline(), asset, o.conn)
	if err != nil {
		return err
	}
	// Resolve the destination before making any billable requests.
	destURI, err := ingestruri.ForConnection(ctx, o.conn, "destination", asset.Connection)
	if err != nil {
		return err
	}
	repo, err := (&git.RepoFinder{}).Repo(asset.DefinitionFile.Path)
	if err != nil {
		return err
	}
	cacheRoot := o.cacheDir
	if cacheRoot == "" {
		cacheRoot, err = os.UserCacheDir()
		if err != nil {
			return err
		}
		cacheRoot = filepath.Join(cacheRoot, "bruin", "inference")
	}
	namespace, err := fingerprint([]string{repo.Path, ti.GetPipeline().Name, asset.Name, asset.Connection, destURI})
	if err != nil {
		return err
	}
	cachePath := filepath.Join(cacheRoot, namespace)
	if err := os.MkdirAll(cachePath, 0o700); err != nil {
		return err
	}
	lock := flock.New(filepath.Join(cachePath, ".lock"))
	locked, err := lock.TryLock()
	if err != nil {
		return err
	}
	if !locked {
		return errors.New("inference asset is already running against this local cache")
	}
	defer lock.Unlock() //nolint:errcheck

	// input_query is rendered by Bruin's parameter mutator. Never render it twice.
	record, err := readRecord(ctx, conn, inputQuery, cfg.maxRows, asset.Columns)
	if err != nil {
		return fmt.Errorf("inference input query failed: %w", err)
	}
	defer record.Release()
	input := &query.QueryResult{Columns: make([]string, record.NumCols()), Rows: make([][]any, record.NumRows())}
	for i, field := range record.Schema().Fields() {
		input.Columns[i] = field.Name
	}
	for i := range input.Rows {
		input.Rows[i] = make([]any, record.NumCols())
		for j, column := range record.Columns() {
			input.Rows[i][j] = column.GetOneForMarshal(i)
		}
	}
	rows, keys, err := inputRows(input, asset.ColumnNamesWithPrimaryKey(), cfg)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fullRefresh, _ := ctx.Value(pipeline.RunConfigFullRefresh).(bool)
		if asset.Materialization.Strategy != pipeline.MaterializationStrategyCreateReplace && !asset.FullRefreshEnabled(fullRefresh) {
			return nil
		}
	}

	results := make([]string, len(rows))
	var called, reused atomic.Int64
	group, requestCtx := errgroup.WithContext(ctx)
	group.SetLimit(cfg.parallelism)
	for i, row := range rows {
		group.Go(func() error {
			if err := requestCtx.Err(); err != nil {
				return err
			}
			// A fresh renderer keeps row values isolated between concurrent assets.
			renderer := jinja.NewRenderer(jinja.Context{"row": row})
			prompt, err := renderer.Render(cfg.prompt)
			if err != nil {
				return fmt.Errorf("could not render inference prompt for input row %d", i+1)
			}
			key, err := fingerprint([]any{keys[i], cfg.provider, cfg.model, cfg.maxTokens, cfg.outputColumn, cfg.allowedValues, prompt})
			if err != nil {
				return err
			}
			path := filepath.Join(cachePath, key+".json")
			var result string
			cached, readErr := os.ReadFile(path)
			if readErr != nil && !os.IsNotExist(readErr) {
				return readErr
			}
			if !cfg.force && readErr == nil {
				if json.Unmarshal(cached, &result) != nil || result == "" || (len(cfg.allowedValues) != 0 && !slices.Contains(cfg.allowedValues, result)) {
					return fmt.Errorf("invalid inference cache entry; remove %s and retry", path)
				}
				reused.Add(1)
			} else {
				result, err = o.complete(requestCtx, client, prompt)
				if err != nil {
					return fmt.Errorf("inference failed for input row %d: %w", i+1, err)
				}
				called.Add(1)
				if result == "" || (len(cfg.allowedValues) != 0 && !slices.Contains(cfg.allowedValues, result)) {
					return fmt.Errorf("inference output for input row %d is empty or not in allowed_values", i+1)
				}
				if err := saveResult(path, result); err != nil {
					return err
				}
			}
			results[i] = result
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if out, ok := ctx.Value(executor.KeyPrinter).(io.Writer); ok {
		_, _ = fmt.Fprintf(out, "Inference: %d rows, %d model calls, %d cached results\n", len(rows), called.Load(), reused.Load())
	}
	return o.publish(ctx, asset, repo, destURI, record, cfg.outputColumn, results)
}

func inputRows(input *query.QueryResult, primaryKeys []string, cfg *assetConfig) ([]map[string]any, [][]any, error) {
	if input == nil || len(input.Rows) > cfg.maxRows {
		return nil, nil, fmt.Errorf("inference input is missing or exceeds max_rows (%d); no model calls were made", cfg.maxRows)
	}
	columns := make(map[string]bool, len(input.Columns))
	for _, name := range input.Columns {
		if columns[name] || name == cfg.outputColumn {
			return nil, nil, errors.New("inference input has duplicate columns or already contains output_column")
		}
		columns[name] = true
	}
	for _, key := range primaryKeys {
		if !columns[key] {
			return nil, nil, fmt.Errorf("inference primary key %s is missing from input", key)
		}
	}
	rows := make([]map[string]any, len(input.Rows))
	keys := make([][]any, len(input.Rows))
	seen := make(map[string]bool, len(rows))
	for i, values := range input.Rows {
		if len(values) != len(input.Columns) {
			return nil, nil, fmt.Errorf("inference input row %d has an invalid column count", i+1)
		}
		row := make(map[string]any, len(values)+1)
		for j, value := range values {
			row[input.Columns[j]] = value
		}
		for _, name := range primaryKeys {
			if row[name] == nil {
				return nil, nil, fmt.Errorf("inference primary keys cannot be null (row %d)", i+1)
			}
			keys[i] = append(keys[i], row[name])
		}
		key, err := fingerprint(keys[i])
		if err != nil {
			return nil, nil, err
		}
		if seen[key] {
			return nil, nil, fmt.Errorf("inference input contains duplicate primary keys (row %d)", i+1)
		}
		seen[key] = true
		rows[i] = row
	}
	return rows, keys, nil
}

func fingerprint(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", errors.New("could not encode inference input identity")
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

func saveResult(path, result string) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".result-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	err = json.NewEncoder(file).Encode(result)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(file.Name(), path)
}

func (o *Operator) publish(ctx context.Context, asset *pipeline.Asset, repo *git.Repo, destURI string, record arrow.RecordBatch, outputColumn string, results []string) error {
	file, err := os.CreateTemp("", "bruin-inference-*.arrow")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := writeArrow(file, record, outputColumn, results); err != nil {
		_ = file.Close()
		return fmt.Errorf("could not write inference Arrow output: %w", err)
	}
	if err := file.Close(); err != nil {
		return err
	}
	strategy, _ := python.TranslateBruinStrategyToIngestr(asset.Materialization.Strategy)
	// Do not pass inference parameters to the loader or mutate the parsed asset.
	writeAsset := *asset
	writeAsset.Parameters = pipeline.ParameterMap{"incremental_strategy": strategy}
	args, err := python.ConsolidatedParameters(ctx, &writeAsset, []string{
		"ingest", "--source-uri", "mmap://" + file.Name(), "--source-table", "inference",
		"--dest-uri", destURI, "--dest-table", asset.Name, "--yes", "--progress", "log",
	}, nil)
	if err != nil {
		return err
	}
	return o.runner.RunIngestr(ctx, args, python.AddExtraPackages(destURI, "mmap://"+file.Name(), nil), repo)
}
