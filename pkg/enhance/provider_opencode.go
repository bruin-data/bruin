package enhance

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// OpenCodeProvider implements the Provider interface for OpenCode CLI.
type OpenCodeProvider struct {
	opencodePath string
	model        string
	debug        bool
	fs           afero.Fs
}

// NewOpenCodeProvider creates a new OpenCode CLI provider.
func NewOpenCodeProvider(model string, fs afero.Fs) *OpenCodeProvider {
	if model == "" {
		model = defaultModel
	}
	opencodePath, _ := exec.LookPath("opencode")
	return &OpenCodeProvider{
		opencodePath: opencodePath,
		model:        model,
		fs:           fs,
	}
}

// Name returns the provider name.
func (p *OpenCodeProvider) Name() string {
	return "opencode"
}

// SetDebug enables or disables debug output.
func (p *OpenCodeProvider) SetDebug(debug bool) {
	p.debug = debug
}

// SetAPIKey is a no-op for OpenCode (uses native auth system).
func (p *OpenCodeProvider) SetAPIKey(apiKey string) {
	// OpenCode uses its native auth system (~/.local/share/opencode/auth.json)
	// No need to inject API key via environment variable
}

// EnsureCLI checks if OpenCode CLI is available.
func (p *OpenCodeProvider) EnsureCLI() error {
	if p.opencodePath != "" {
		return nil
	}

	// Try to find opencode in PATH
	opencodePath, err := exec.LookPath("opencode")
	if err == nil {
		p.opencodePath = opencodePath
		return nil
	}

	// Search common installation locations
	commonPaths := []string{
		filepath.Join(os.Getenv("HOME"), ".local", "bin", "opencode"),
		"/usr/local/bin/opencode",
		"/usr/bin/opencode",
	}
	for _, path := range commonPaths {
		if _, statErr := os.Stat(path); statErr == nil {
			p.opencodePath = path
			return nil
		}
	}

	return errors.New("OpenCode CLI not found. Install it from: https://opencode.ai/docs/cli/")
}

// Enhance executes the OpenCode CLI with the given prompt.
func (p *OpenCodeProvider) Enhance(ctx context.Context, prompt, systemPrompt string) error {
	// OpenCode CLI uses the "run" command for non-interactive execution
	args := []string{
		"run",
	}

	// Add model flag if specified
	if p.model != "" {
		args = append(args, "--model", p.model)
	}

	// OpenCode may not support system prompts in the same way as Claude
	// We'll combine them into the main prompt if system prompt exists
	fullPrompt := prompt
	if systemPrompt != "" {
		fullPrompt = systemPrompt + "\n\n" + prompt
	}

	args = append(args, fullPrompt)

	cmd := exec.CommandContext(ctx, p.opencodePath, args...) //nolint:gosec

	// In debug mode, stream output to stdout/stderr for visibility
	if p.debug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "opencode CLI failed")
	}

	return nil
}
