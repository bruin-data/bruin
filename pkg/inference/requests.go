package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/bruin-data/bruin/pkg/jinja"
	"golang.org/x/sync/errgroup"
)

// inferRows schedules requests, not rows: all provider groups share one limit.
// Each group writes distinct column/row cells, so results need no shared maps.
func (o *Operator) inferRows(ctx context.Context, cfg *assetConfig, groups []requestGroup, rows []map[string]any, cachePath string) ([][]any, int64, int64, error) {
	cache := newResultCache(cachePath, 1024)
	results := make([][]any, len(cfg.outputs))
	indices := make(map[string]int, len(cfg.outputs))
	for i, column := range cfg.outputs {
		results[i] = make([]any, len(rows))
		indices[column.Name] = i
	}
	var called, reused atomic.Int64
	workers, requestCtx := errgroup.WithContext(ctx)
	workers.SetLimit(cfg.parallelism)
	for i, row := range rows {
		for _, group := range groups {
			workers.Go(func() error {
				if err := requestCtx.Err(); err != nil {
					return err
				}
				renderer := jinja.NewRenderer(jinja.Context{"row": row})
				render := func(template string) (string, error) {
					value, err := renderer.Render(template)
					if err != nil {
						return "", fmt.Errorf("could not render inference prompt for input row %d", i+1)
					}
					return value, nil
				}
				state, err := render(cfg.context)
				if err != nil {
					return err
				}
				instructions, err := render(cfg.instructions)
				if err != nil {
					return err
				}
				columns := slices.Clone(group.columns)
				for j := range columns {
					columns[j].Prompt, err = render(columns[j].Prompt)
					if err != nil {
						return err
					}
				}
				identity := []any{"structured-v3", group.provider, group.model, group.connection, cfg.maxTokens, state, instructions, columns}
				key, err := fingerprint(identity)
				if err != nil {
					return err
				}
				var values map[string]any
				result, err := cache.get(key, cfg.cache, func() (string, error) {
					client := &Client{Provider: group.provider, Model: group.model, APIKey: group.apiKey, MaxOutputTokens: cfg.maxTokens}
					var err error
					values, err = o.structured(requestCtx, client, state, instructions, columns)
					if err != nil {
						return "", fmt.Errorf("inference failed for input row %d (%s/%s): %w", i+1, group.provider, group.model, err)
					}
					called.Add(1)
					data, err := json.Marshal(values)
					return string(data), err
				})
				if err != nil {
					return err
				}
				if values == nil {
					reused.Add(1)
					values, err = validateStructuredResult(result, columns)
					if err != nil {
						return fmt.Errorf("inference output for input row %d: %w", i+1, err)
					}
				}
				for name, value := range values {
					results[indices[name]][i] = value
				}
				return nil
			})
		}
	}
	err := workers.Wait()
	return results, called.Load(), reused.Load(), err
}
