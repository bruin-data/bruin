package inference

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

type assetConfig struct {
	provider, model, prompt, inputQuery, inputAsset, outputColumn, keyEnv string
	maxRows, maxTokens, parallelism                                       int
	allowedValues                                                         []string
	force                                                                 bool
}

// ValidateAsset validates the first-version inference contract without making requests.
func ValidateAsset(asset *pipeline.Asset) error {
	_, err := readConfig(asset)
	return err
}

// ProviderDefaultAPIKeyEnv returns the conventional API key environment variable for provider.
func ProviderDefaultAPIKeyEnv(provider string) (string, bool) {
	value, ok := map[string]string{
		"opencode":   "OPENCODE_API_KEY",
		"openrouter": "OPENROUTER_API_KEY",
		"openai":     "OPENAI_API_KEY",
		"anthropic":  "ANTHROPIC_API_KEY",
		"google":     "GOOGLE_API_KEY",
	}[provider]
	return value, ok
}

func readConfig(asset *pipeline.Asset) (*assetConfig, error) {
	c := &assetConfig{maxRows: 1000, maxTokens: defaultMaxOutputTokens, parallelism: 4}
	fields := map[string]*string{
		"provider": &c.provider, "model": &c.model, "prompt": &c.prompt,
		"output_column": &c.outputColumn,
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
		return nil, fmt.Errorf("inference requires exactly one of parameters.input_query or parameters.input_asset")
	}
	if queryExists {
		var ok bool
		c.inputQuery, ok = inputQuery.(string)
		if !ok || strings.TrimSpace(c.inputQuery) == "" {
			return nil, fmt.Errorf("inference parameters.input_query must be a nonempty string")
		}
	} else {
		var ok bool
		c.inputAsset, ok = inputAsset.(string)
		if !ok || strings.TrimSpace(c.inputAsset) == "" {
			return nil, fmt.Errorf("inference parameters.input_asset must be a nonempty string")
		}
	}
	var ok bool
	c.keyEnv, ok = ProviderDefaultAPIKeyEnv(c.provider)
	if !ok {
		return nil, fmt.Errorf("unsupported inference provider %q: use opencode, openrouter, openai, anthropic or google", c.provider)
	}
	for name := range asset.Parameters {
		switch name {
		case "provider", "model", "prompt", "input_query", "input_asset", "output_column",
			"api_key_env", "max_rows", "max_output_tokens", "extract_parallelism", "force", "allowed_values":
		default:
			return nil, fmt.Errorf("unsupported inference parameter %q", name)
		}
	}
	if value, exists := asset.Parameters.GetString("api_key_env"); exists {
		if value == "" {
			return nil, fmt.Errorf("inference api_key_env cannot be empty")
		}
		c.keyEnv = value
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
	if value, exists := asset.Parameters.GetString("force"); exists {
		if value != "true" && value != "false" {
			return nil, fmt.Errorf("inference force must be true or false")
		}
		c.force = value == "true"
	}
	if values, exists := asset.Parameters["allowed_values"]; exists {
		encoded, err := json.Marshal(values)
		if err != nil || json.Unmarshal(encoded, &c.allowedValues) != nil || len(c.allowedValues) == 0 {
			return nil, fmt.Errorf("inference allowed_values must be a nonempty list of strings")
		}
	}
	if asset.Connection == "" {
		return nil, fmt.Errorf("inference requires an explicit warehouse connection")
	}
	if asset.Materialization.Type != pipeline.MaterializationTypeTable ||
		(asset.Materialization.Strategy != pipeline.MaterializationStrategyMerge && asset.Materialization.Strategy != pipeline.MaterializationStrategyCreateReplace) {
		return nil, fmt.Errorf("inference requires table materialization with merge or create+replace strategy")
	}
	if asset.Materialization.IncrementalKey != "" || asset.Materialization.IncrementalPredicate != "" || asset.Materialization.PartitionBy != "" || len(asset.Materialization.ClusterBy) != 0 {
		return nil, fmt.Errorf("inference does not yet support incremental keys, predicates, partitioning or clustering; filter input_query explicitly")
	}
	if len(asset.ColumnNamesWithPrimaryKey()) == 0 {
		return nil, fmt.Errorf("inference requires primary key columns for result identity")
	}
	found := false
	for _, column := range asset.Columns {
		if len(column.Checks) != 0 {
			return nil, fmt.Errorf("inference checks are not yet supported; define checks on a downstream SQL asset")
		}
		if column.Name == c.outputColumn {
			found = true
			if column.PrimaryKey || column.Type != "string" {
				return nil, fmt.Errorf("inference output_column must be a declared non-primary-key string column")
			}
		}
	}
	if !found {
		return nil, fmt.Errorf("inference output_column must be declared in columns with type string")
	}
	if len(asset.CustomChecks) != 0 || len(asset.Hooks.Pre) != 0 || len(asset.Hooks.Post) != 0 {
		return nil, fmt.Errorf("inference custom checks and hooks are not yet supported")
	}
	return c, nil
}
