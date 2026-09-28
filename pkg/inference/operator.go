package inference

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/ingestruri"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/user"
	"github.com/gofrs/flock"
	"github.com/spf13/afero"
)

type ingestrRunner interface {
	RunIngestr(context.Context, []string, []string, *git.Repo) error
}

// Operator enriches query results and publishes them through Bruin's ingestr writer.
type Operator struct {
	conn       config.ConnectionAndDetailsGetter
	runner     ingestrRunner
	structured func(context.Context, *Client, string, string, []outputColumn) (map[string]any, error)
	cacheDir   string
}

func NewOperator(conn config.ConnectionAndDetailsGetter) *Operator {
	return &Operator{
		conn: conn,
		runner: &python.UvPythonRunner{
			UvInstaller: &python.UvChecker{}, IngestrInstaller: &python.IngestrChecker{}, Cmd: &python.CommandRunner{},
		},
		structured: func(ctx context.Context, c *Client, state, instructions string, columns []outputColumn) (map[string]any, error) {
			return c.CompleteStructured(ctx, state, instructions, columns)
		},
	}
}

func (o *Operator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	asset := ti.GetAsset()
	cfg, err := readConfig(asset)
	if err != nil {
		return err
	}
	groups, err := o.resolveGroups(ti.GetPipeline(), cfg)
	if err != nil {
		return err
	}
	conn := o.conn.GetConnection(asset.Connection)
	inputQuery, err := resolveInputQuery(ti.GetPipeline(), asset, cfg, o.conn)
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
	var cachePath string
	if cfg.cache {
		cacheRoot := o.cacheDir
		if cacheRoot == "" {
			cacheRoot, err = user.NewConfigManager(afero.NewOsFs()).EnsureAndGetBruinHomeDir()
			if err != nil {
				return err
			}
			cacheRoot = filepath.Join(cacheRoot, "inference")
		}
		namespace, err := fingerprint([]string{repo.Path, ti.GetPipeline().Name, asset.Name, asset.Connection, destURI})
		if err != nil {
			return err
		}
		cachePath = filepath.Join(cacheRoot, namespace)
		if err := os.MkdirAll(cachePath, 0o700); err != nil {
			return err
		}
		lock := flock.New(filepath.Join(cachePath, ".lock"))
		locked, err := lock.TryLock()
		if err != nil {
			return err
		}
		if !locked {
			return fmt.Errorf("inference asset is already running against this local cache")
		}
		defer lock.Unlock() //nolint:errcheck
	}

	// input_query is rendered by Bruin's parameter mutator. Never render it twice.
	record, err := readRecord(ctx, conn, inputQuery, cfg.maxRows, asset.Columns)
	if err != nil {
		return fmt.Errorf("inference input query failed: %w", err)
	}
	defer record.Release()
	rows, err := inputRows(record, asset.ColumnNamesWithPrimaryKey(), cfg.outputs)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fullRefresh, _ := ctx.Value(pipeline.RunConfigFullRefresh).(bool)
		if asset.Materialization.Strategy != pipeline.MaterializationStrategyCreateReplace && !asset.FullRefreshEnabled(fullRefresh) {
			return nil
		}
	}

	results, called, reused, err := o.inferRows(ctx, cfg, groups, rows, cachePath)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if out, ok := ctx.Value(executor.KeyPrinter).(io.Writer); ok {
		_, _ = fmt.Fprintf(out, "Inference: %d rows, %d model calls, %d cached results\n", len(rows), called, reused)
	}
	return o.publish(ctx, asset, repo, destURI, record, cfg.outputs, results)
}

func inputRows(input arrow.RecordBatch, primaryKeys []string, outputs []outputColumn) ([]map[string]any, error) {
	fields := input.Schema().Fields()
	columns := make(map[string]bool, len(fields))
	generated := make(map[string]bool, len(outputs))
	for _, column := range outputs {
		generated[column.Name] = true
	}
	for _, field := range fields {
		if columns[field.Name] || generated[field.Name] {
			return nil, fmt.Errorf("inference input has duplicate columns or already contains a generated column")
		}
		columns[field.Name] = true
	}
	for _, key := range primaryKeys {
		if !columns[key] {
			return nil, fmt.Errorf("inference primary key %s is missing from input", key)
		}
	}
	rows := make([]map[string]any, input.NumRows())
	seen := make(map[string]bool, len(rows))
	for i := range rows {
		row := make(map[string]any, len(fields))
		for j, column := range input.Columns() {
			row[fields[j].Name] = column.GetOneForMarshal(i)
		}
		keys := make([]any, 0, len(primaryKeys))
		for _, name := range primaryKeys {
			if row[name] == nil {
				return nil, fmt.Errorf("inference primary keys cannot be null (row %d)", i+1)
			}
			keys = append(keys, row[name])
		}
		key, err := fingerprint(keys)
		if err != nil {
			return nil, err
		}
		if seen[key] {
			return nil, fmt.Errorf("inference input contains duplicate primary keys (row %d)", i+1)
		}
		seen[key] = true
		rows[i] = row
	}
	return rows, nil
}

func fingerprint(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("could not encode inference input identity")
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

func (o *Operator) publish(ctx context.Context, asset *pipeline.Asset, repo *git.Repo, destURI string, record arrow.RecordBatch, outputs []outputColumn, results [][]any) error {
	file, err := os.CreateTemp("", "bruin-inference-*.arrow")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := writeInferenceArrow(file, record, outputs, results); err != nil {
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
