package enhance

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// ClaudeProvider implements the Provider interface for Claude CLI.
type ClaudeProvider struct {
	claudePath string
	model      string
	apiKey     string
	debug      bool
	fs         afero.Fs
}

// NewClaudeProvider creates a new Claude CLI provider.
func NewClaudeProvider(model string, fs afero.Fs) *ClaudeProvider {
	if model == "" {
		model = defaultModel
	}
	claudePath, _ := exec.LookPath("claude")
	return &ClaudeProvider{
		claudePath: claudePath,
		model:      model,
		fs:         fs,
	}
}

// Name returns the provider name.
func (p *ClaudeProvider) Name() string {
	return "claude"
}

// SetDebug enables or disables debug output.
func (p *ClaudeProvider) SetDebug(debug bool) {
	p.debug = debug
}

// SetAPIKey sets the Anthropic API key to use for Claude CLI.
func (p *ClaudeProvider) SetAPIKey(apiKey string) {
	p.apiKey = apiKey
}

// EnsureCLI checks if Claude CLI is available.
func (p *ClaudeProvider) EnsureCLI() error {
	if p.claudePath != "" {
		return nil
	}

	// Try to find claude in PATH
	claudePath, err := exec.LookPath("claude")
	if err == nil {
		p.claudePath = claudePath
		return nil
	}

	// Search common installation locations
	commonPaths := []string{
		filepath.Join(os.Getenv("HOME"), ".local", "bin", "claude"),
		"/usr/local/bin/claude",
		"/usr/bin/claude",
	}
	for _, path := range commonPaths {
		if _, statErr := os.Stat(path); statErr == nil {
			p.claudePath = path
			return nil
		}
	}

	return errors.New("Claude CLI not found. Install it from: https://claude.ai/download")
}

// Enhance executes the Claude CLI with the given prompt.
func (p *ClaudeProvider) Enhance(ctx context.Context, prompt, systemPrompt string) error {
	args := []string{
		"-p",
		"--output-format", "text",
		"--model", p.model,
		"--dangerously-skip-permissions",
	}

	if systemPrompt != "" {
		args = append(args, "--append-system-prompt", systemPrompt)
	}

	args = append(args, prompt)

	cmd := exec.CommandContext(ctx, p.claudePath, args...) //nolint:gosec

	if p.apiKey != "" {
		cmd.Env = append(os.Environ(), "ANTHROPIC_API_KEY="+p.apiKey)
	}

	// In debug mode, stream output to stdout/stderr for visibility
	if p.debug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "claude CLI failed")
	}

	return nil
}
