package enhance

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// CLIProviderConfig holds configuration for a generic CLI provider.
type CLIProviderConfig struct {
	Name              string
	BinaryName        string
	DefaultModel      string
	UseAPIKeyEnv      bool   // Whether to inject API key via environment variable.
	APIKeyEnvVar      string // Environment variable name for API key (e.g., "ANTHROPIC_API_KEY").
	BuildArgs         func(model, prompt, systemPrompt string) []string
	CommonSearchPaths []string // Additional search paths beyond standard ones.
}

// CLIProvider implements the Provider interface for any CLI-based AI tool.
type CLIProvider struct {
	config  CLIProviderConfig
	cliPath string
	model   string
	apiKey  string
	debug   bool
	fs      afero.Fs
}

// NewCLIProvider creates a new generic CLI provider.
func NewCLIProvider(config CLIProviderConfig, model string, fs afero.Fs) *CLIProvider {
	// Use default model if none specified
	if model == "" {
		model = config.DefaultModel
	}

	cliPath, _ := exec.LookPath(config.BinaryName)
	return &CLIProvider{
		config:  config,
		cliPath: cliPath,
		model:   model,
		fs:      fs,
	}
}

// Name returns the provider name.
func (p *CLIProvider) Name() string {
	return p.config.Name
}

// SetDebug enables or disables debug output.
func (p *CLIProvider) SetDebug(debug bool) {
	p.debug = debug
}

// SetAPIKey sets the API key for the provider.
func (p *CLIProvider) SetAPIKey(apiKey string) {
	p.apiKey = apiKey
}

// EnsureCLI checks if the CLI is available.
func (p *CLIProvider) EnsureCLI() error {
	if p.cliPath != "" {
		return nil
	}

	// Try to find CLI in PATH
	cliPath, err := exec.LookPath(p.config.BinaryName)
	if err == nil {
		p.cliPath = cliPath
		return nil
	}

	// Search common installation locations
	commonPaths := []string{
		filepath.Join(os.Getenv("HOME"), ".local", "bin", p.config.BinaryName),
		filepath.Join(os.Getenv("HOME"), ".npm-global", "bin", p.config.BinaryName),
		"/usr/local/bin/" + p.config.BinaryName,
		"/usr/bin/" + p.config.BinaryName,
	}

	// Add provider-specific paths
	commonPaths = append(commonPaths, p.config.CommonSearchPaths...)

	for _, path := range commonPaths {
		if _, statErr := os.Stat(path); statErr == nil {
			p.cliPath = path
			return nil
		}
	}

	return errors.Errorf("%s CLI not found. Please install it before using the enhance command", p.config.Name)
}

// Enhance executes the CLI with the given prompt.
func (p *CLIProvider) Enhance(ctx context.Context, prompt, systemPrompt string) error {
	args := p.config.BuildArgs(p.model, prompt, systemPrompt)

	cmd := exec.CommandContext(ctx, p.cliPath, args...) //nolint:gosec

	// Set working directory to current directory
	if wd, err := os.Getwd(); err == nil {
		cmd.Dir = wd
	}

	// Inject API key via environment variable if configured
	if p.config.UseAPIKeyEnv && p.apiKey != "" {
		cmd.Env = append(os.Environ(), p.config.APIKeyEnvVar+"="+p.apiKey)
	}

	// Always capture stderr to provide better error messages
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	// In debug mode, also stream to stdout
	if p.debug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)
	}

	if err := cmd.Run(); err != nil {
		errMsg := stderr.String()
		if errMsg != "" {
			return errors.Wrapf(err, "%s CLI failed: %s", p.config.Name, errMsg)
		}
		return errors.Wrapf(err, "%s CLI failed", p.config.Name)
	}

	return nil
}
