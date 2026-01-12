package enhance

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

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
	EnsureClaudeCLI() error
	EnhanceAsset(ctx context.Context, asset *pipeline.Asset, pipelineName, tableSummaryJSON string) error
}

// Enhancer coordinates the AI enhancement process for assets.
type Enhancer struct {
	model           string
	claudePath      string
	apiKey          string
	debug           bool
	pipelineBuilder *pipeline.Builder
	fs              afero.Fs
}

// NewEnhancer creates a new Enhancer instance.
func NewEnhancer(model string) *Enhancer {
	if model == "" {
		model = defaultModel
	}
	claudePath, _ := exec.LookPath("claude")
	fs := afero.NewOsFs()
	return &Enhancer{
		model:           model,
		claudePath:      claudePath,
		pipelineBuilder: pipeline.NewBuilder(pipeline.BuilderConfig{}, pipeline.CreateTaskFromYamlDefinition(fs), pipeline.CreateTaskFromFileComments(fs), fs, nil),
		fs:              fs,
	}
}

// SetAPIKey sets the Anthropic API key to use for Claude CLI.
func (e *Enhancer) SetAPIKey(apiKey string) {
	e.apiKey = apiKey
}

// SetDebug enables or disables debug output.
func (e *Enhancer) SetDebug(debug bool) {
	e.debug = debug
}

// EnsureClaudeCLI checks if Claude CLI is installed and installs it if not.
func (e *Enhancer) EnsureClaudeCLI() error {
	if e.claudePath != "" {
		return nil
	}

	claudePath, err := e.installClaudeCLI()
	if err != nil {
		return err
	}
	e.claudePath = claudePath
	return nil
}

// EnhanceAsset runs AI enhancement on a single asset.
func (e *Enhancer) EnhanceAsset(ctx context.Context, asset *pipeline.Asset, pipelineName, tableSummaryJSON string) error {
	if err := e.EnsureClaudeCLI(); err != nil {
		return errors.Wrap(err, "claude CLI not available")
	}

	if asset.DefinitionFile.Path == "" {
		return errors.New("asset definition file path is required")
	}

	// Build prompt with file path and optional pre-fetched stats
	prompt := BuildEnhancePrompt(asset.DefinitionFile.Path, asset.Name, pipelineName, tableSummaryJSON)
	systemPrompt := GetSystemPrompt(tableSummaryJSON != "")

	// Call Claude CLI - Claude will edit the file directly
	if err := e.callClaude(ctx, prompt, systemPrompt); err != nil {
		return errors.Wrap(err, "failed to enhance asset")
	}

	// Reload the asset from file after Claude edited it
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

// callClaude executes the Claude CLI with the given prompt.
func (e *Enhancer) callClaude(ctx context.Context, prompt, systemPrompt string) error {
	args := []string{
		"-p",
		"--output-format", "text",
		"--model", e.model,
		"--dangerously-skip-permissions",
	}

	if systemPrompt != "" {
		args = append(args, "--append-system-prompt", systemPrompt)
	}

	args = append(args, prompt)

	cmd := exec.CommandContext(ctx, e.claudePath, args...) //nolint:gosec

	if e.apiKey != "" {
		cmd.Env = append(os.Environ(), "ANTHROPIC_API_KEY="+e.apiKey)
	}

	// In debug mode, stream output to stdout/stderr for visibility
	if e.debug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "claude CLI failed")
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

// installClaudeCLI installs the Claude CLI using the official installation script.
func (e *Enhancer) installClaudeCLI() (string, error) {
	if runtime.GOOS == "windows" {
		return "", errors.New("automatic Claude CLI installation is not supported on Windows; please install manually or use WSL")
	}

	installCmd := exec.Command("bash", "-c", "curl -fsSL https://claude.ai/install.sh | bash")

	var stdout, stderr bytes.Buffer
	installCmd.Stdout = &stdout
	installCmd.Stderr = &stderr

	if err := installCmd.Run(); err != nil {
		return "", errors.Wrapf(err, "failed to install Claude CLI: %s", stderr.String())
	}

	claudePath, err := exec.LookPath("claude")
	if err != nil {
		commonPaths := []string{
			filepath.Join(os.Getenv("HOME"), ".local", "bin", "claude"),
			"/usr/local/bin/claude",
			"/usr/bin/claude",
		}
		for _, p := range commonPaths {
			if _, statErr := os.Stat(p); statErr == nil {
				return p, nil
			}
		}
		return "", errors.New("Claude CLI installation appeared to succeed but 'claude' not found in PATH")
	}

	return claudePath, nil
}
