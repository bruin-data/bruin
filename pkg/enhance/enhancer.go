package enhance

import (
	"context"

	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

const (
	defaultModel = "claude-sonnet-4-20250514"
)

// EnhancerInterface defines the interface for asset enhancement.
type EnhancerInterface interface {
	SetAPIKey(apiKey string)
	SetDebug(debug bool)
	EnsureCLI() error
	EnhanceAsset(ctx context.Context, asset *pipeline.Asset, pipelineName, tableSummaryJSON string) error
}

// Enhancer coordinates the AI enhancement process for assets.
type Enhancer struct {
	provider        Provider
	pipelineBuilder *pipeline.Builder
	fs              afero.Fs
}

// NewEnhancer creates a new Enhancer instance.
func NewEnhancer(providerType ProviderType, model string) *Enhancer {
	fs := afero.NewOsFs()
	var provider Provider
	switch providerType {
	case ProviderCodex:
		provider = NewCodexProvider(model, fs)
	case ProviderOpenCode:
		provider = NewOpenCodeProvider(model, fs)
	case ProviderClaude:
		provider = NewClaudeProvider(model, fs)
	default:
		// Default to Claude
		provider = NewClaudeProvider(model, fs)
	}
	return &Enhancer{
		provider:        provider,
		pipelineBuilder: pipeline.NewBuilder(pipeline.BuilderConfig{}, pipeline.CreateTaskFromYamlDefinition(fs), pipeline.CreateTaskFromFileComments(fs), fs, nil),
		fs:              fs,
	}
}

// SetAPIKey sets the API key for the provider.
func (e *Enhancer) SetAPIKey(apiKey string) {
	e.provider.SetAPIKey(apiKey)
}

// SetDebug enables or disables debug output.
func (e *Enhancer) SetDebug(debug bool) {
	e.provider.SetDebug(debug)
}

// EnsureCLI checks if the provider's CLI is available.
func (e *Enhancer) EnsureCLI() error {
	return e.provider.EnsureCLI()
}

// EnhanceAsset runs AI enhancement on a single asset.
func (e *Enhancer) EnhanceAsset(ctx context.Context, asset *pipeline.Asset, pipelineName, tableSummaryJSON string) error {
	if err := e.EnsureCLI(); err != nil {
		return errors.Wrapf(err, "%s CLI not available", e.provider.Name())
	}

	if asset.DefinitionFile.Path == "" {
		return errors.New("asset definition file path is required")
	}

	// Build prompt with file path and optional pre-fetched stats
	prompt := BuildEnhancePrompt(asset.DefinitionFile.Path, asset.Name, pipelineName, tableSummaryJSON)
	systemPrompt := GetSystemPrompt(tableSummaryJSON != "")

	// Call the provider CLI to enhance the asset
	if err := e.provider.Enhance(ctx, prompt, systemPrompt); err != nil {
		return errors.Wrap(err, "failed to enhance asset")
	}

	// Reload the asset from file after it was edited
	updatedAsset, err := e.pipelineBuilder.CreateAssetFromFile(asset.DefinitionFile.Path, nil)
	if err != nil {
		return errors.Wrap(err, "failed to reload asset after enhancement")
	}

	if updatedAsset == nil {
		return errors.New("no valid asset found after enhancement")
	}

	// Format the asset by persisting it (this formats and writes it back)
	if err := updatedAsset.Persist(e.fs); err != nil {
		return errors.Wrap(err, "failed to format asset")
	}

	// Validate the asset using lint rules
	if err := e.validateAsset(ctx, updatedAsset); err != nil {
		return errors.Wrap(err, "asset validation failed")
	}

	return nil
}

// validateAsset runs basic validation rules on the asset.
func (e *Enhancer) validateAsset(ctx context.Context, asset *pipeline.Asset) error {
	// Create a minimal pipeline containing just this asset for validation
	p := &pipeline.Pipeline{
		Name:   "validation",
		Assets: []*pipeline.Asset{asset},
	}

	// Run basic fast lint rules that don't require external dependencies
	rules := []lint.Rule{
		&lint.SimpleRule{
			Identifier:       "task-name-valid",
			Fast:             true,
			Severity:         lint.ValidatorSeverityCritical,
			AssetValidator:   lint.EnsureTaskNameIsValidForASingleAsset,
			ApplicableLevels: []lint.Level{lint.LevelAsset},
		},
		&lint.SimpleRule{
			Identifier:       "task-type-correct",
			Fast:             true,
			Severity:         lint.ValidatorSeverityCritical,
			AssetValidator:   lint.EnsureTypeIsCorrectForASingleAsset,
			ApplicableLevels: []lint.Level{lint.LevelAsset},
		},
	}

	for _, rule := range rules {
		issues, err := rule.ValidateAsset(ctx, p, asset)
		if err != nil {
			return errors.Wrapf(err, "validation rule '%s' failed", rule.Name())
		}

		if len(issues) > 0 {
			// Return the first issue as an error
			return errors.Errorf("validation failed: %s", issues[0].Description)
		}
	}

	return nil
}

