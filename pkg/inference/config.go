package inference

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

type assetConfig struct {
	provider, model, inputQuery, inputAsset string
	maxRows, maxTokens, parallelism         int
	cache                                   bool
	context, instructions                   string
	groups                                  []requestGroup
	outputs                                 []outputColumn
}

// ValidateAsset validates the first-version inference contract without making requests.
func ValidateAsset(asset *pipeline.Asset) error {
	_, err := readConfig(asset)
	return err
}

func supportedProvider(provider string) bool {
	switch provider {
	case "opencode", "openrouter", "openai", "anthropic", "google", "typesafe":
		return true
	default:
		return false
	}
}

func readConfig(asset *pipeline.Asset) (*assetConfig, error) {
	c := &assetConfig{maxRows: 1000, maxTokens: defaultMaxOutputTokens, parallelism: 4, cache: true}
	if _, exists := asset.Parameters["api_key_env"]; exists {
		return nil, fmt.Errorf("api_key_env is not supported; use a Bruin provider connection")
	}
	if raw, exists := asset.Parameters["inference_connection"]; exists {
		connection, ok := raw.(string)
		if !ok || strings.TrimSpace(connection) == "" {
			return nil, fmt.Errorf("inference_connection must be a nonempty Bruin connection name")
		}
	}
	fields := map[string]*string{
		"provider": &c.provider, "model": &c.model, "context": &c.context,
	}
	if raw, exists := asset.Parameters["instructions"]; exists {
		var ok bool
		c.instructions, ok = raw.(string)
		if !ok {
			return nil, fmt.Errorf("inference instructions must be a string")
		}
	}
	for name, dest := range fields {
		value, ok := asset.Parameters.GetString(name)
		if !ok || strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("inference requires parameters.%s", name)
		}
		*dest = value
	}
	inputQuery, queryExists := asset.Parameters["input_query"]
	inputAsset, assetExists := asset.Parameters["input_asset"]
	if queryExists == assetExists {
		return nil, errors.New("inference requires exactly one of parameters.input_query or parameters.input_asset")
	}
	if queryExists {
		var ok bool
		c.inputQuery, ok = inputQuery.(string)
		if !ok || strings.TrimSpace(c.inputQuery) == "" {
			return nil, errors.New("inference parameters.input_query must be a nonempty string")
		}
	} else {
		var ok bool
		c.inputAsset, ok = inputAsset.(string)
		if !ok || strings.TrimSpace(c.inputAsset) == "" {
			return nil, errors.New("inference parameters.input_asset must be a nonempty string")
		}
	}
	if !supportedProvider(c.provider) {
		return nil, fmt.Errorf("unsupported inference provider %q: use opencode, openrouter, openai, anthropic, google or typesafe", c.provider)
	}
	if err := c.readColumns(asset); err != nil {
		return nil, err
	}
	if len(c.outputs) == 0 {
		return nil, fmt.Errorf("inference requires at least one column with an inference definition")
	}
	for name := range asset.Parameters {
		switch name {
		case "provider", "model", "context", "instructions", "input_query", "input_asset",
			"inference_connection", "max_rows", "max_output_tokens", "extract_parallelism", "cache", "force":
		default:
			return nil, fmt.Errorf("unsupported inference parameter %q", name)
		}
	}
	for name, dest := range map[string]*int{"max_rows": &c.maxRows, "max_output_tokens": &c.maxTokens, "extract_parallelism": &c.parallelism} {
		if value, exists := asset.Parameters[name]; exists {
			n, err := strconv.Atoi(fmt.Sprint(value))
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("inference %s must be a positive integer", name)
			}
			*dest = n
		}
	}
	if _, exists := asset.Parameters["force"]; exists {
		return nil, fmt.Errorf("inference force is not supported; use cache: false to disable caching")
	}
	if _, exists := asset.Parameters["cache"]; exists {
		value, ok := asset.Parameters.GetString("cache")
		if !ok || (value != "true" && value != "false") {
			return nil, fmt.Errorf("inference cache must be true or false")
		}
		c.cache = value == "true"
	}
	if asset.Connection == "" {
		return nil, errors.New("inference requires an explicit warehouse connection")
	}
	if asset.Materialization.Type != pipeline.MaterializationTypeTable ||
		(asset.Materialization.Strategy != pipeline.MaterializationStrategyMerge && asset.Materialization.Strategy != pipeline.MaterializationStrategyCreateReplace) {
		return nil, errors.New("inference requires table materialization with merge or create+replace strategy")
	}
	if asset.Materialization.IncrementalKey != "" || asset.Materialization.IncrementalPredicate != "" || asset.Materialization.PartitionBy != "" || len(asset.Materialization.ClusterBy) != 0 {
		return nil, errors.New("inference does not yet support incremental keys, predicates, partitioning or clustering; filter input_query explicitly")
	}
	if len(asset.ColumnNamesWithPrimaryKey()) == 0 {
		return nil, errors.New("inference requires primary key columns for result identity")
	}
	for _, column := range asset.Columns {
		if len(column.Checks) != 0 {
			return nil, errors.New("inference checks are not yet supported; define checks on a downstream SQL asset")
		}
	}
	if len(asset.CustomChecks) != 0 || len(asset.Hooks.Pre) != 0 || len(asset.Hooks.Post) != 0 {
		return nil, errors.New("inference custom checks and hooks are not yet supported")
	}
	return c, nil
}
