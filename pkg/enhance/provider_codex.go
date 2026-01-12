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

// CodexProvider implements the Provider interface for Codex CLI.
type CodexProvider struct {
	codexPath string
	model     string
	apiKey    string
	debug     bool
	fs        afero.Fs
}

// NewCodexProvider creates a new Codex CLI provider.
func NewCodexProvider(model string, fs afero.Fs) *CodexProvider {
	// Codex defaults to gpt-5-codex if no model specified
	if model == "" || model == defaultModel {
		model = "gpt-5-codex"
	}
	codexPath, _ := exec.LookPath("codex")
	return &CodexProvider{
		codexPath: codexPath,
		model:     model,
		fs:        fs,
	}
}

// Name returns the provider name.
func (p *CodexProvider) Name() string {
	return "codex"
}

// SetDebug enables or disables debug output.
func (p *CodexProvider) SetDebug(debug bool) {
	p.debug = debug
}

// SetAPIKey is a no-op for Codex (uses native auth system).
func (p *CodexProvider) SetAPIKey(apiKey string) {
	// Codex uses its native auth system (requires `codex` login first)
	// Authentication is stored locally and managed by Codex CLI
	// Setting the apiKey anyway in case it's needed in the future
	p.apiKey = apiKey
}

// EnsureCLI checks if Codex CLI is available.
func (p *CodexProvider) EnsureCLI() error {
	if p.codexPath != "" {
		return nil
	}

	// Try to find codex in PATH
	codexPath, err := exec.LookPath("codex")
	if err == nil {
		p.codexPath = codexPath
		return nil
	}

	// Search common installation locations (including npm global)
	commonPaths := []string{
		filepath.Join(os.Getenv("HOME"), ".local", "bin", "codex"),
		filepath.Join(os.Getenv("HOME"), ".npm-global", "bin", "codex"),
		"/usr/local/bin/codex",
		"/usr/bin/codex",
	}
	for _, path := range commonPaths {
		if _, statErr := os.Stat(path); statErr == nil {
			p.codexPath = path
			return nil
		}
	}

	return errors.New("Codex CLI not found. Please install it before using the enhance command")
}

// Enhance executes the Codex CLI with the given prompt.
func (p *CodexProvider) Enhance(ctx context.Context, prompt, systemPrompt string) error {
	// Codex CLI uses the "exec" command for non-interactive execution
	// Based on documentation: "automate repeatable workflows by scripting Codex"
	args := []string{
		"exec",
	}

	// Add model specification if available
	// Note: Exact syntax may need adjustment based on actual CLI behavior
	if p.model != "" {
		args = append(args, "--model", p.model)
	}

	// Combine system prompt and user prompt if system prompt exists
	fullPrompt := prompt
	if systemPrompt != "" {
		fullPrompt = systemPrompt + "\n\n" + prompt
	}

	args = append(args, fullPrompt)

	cmd := exec.CommandContext(ctx, p.codexPath, args...) //nolint:gosec

	// Set working directory to current directory
	cmd.Dir, _ = os.Getwd()

	// Note: Codex uses its own authentication system
	// Users must run `codex` login first to authenticate
	// The OPENAI_API_KEY environment variable is not used by Codex CLI

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
			return errors.Wrapf(err, "codex CLI failed: %s", errMsg)
		}
		return errors.Wrap(err, "codex CLI failed")
	}

	return nil
}
