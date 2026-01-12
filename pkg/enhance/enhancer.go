package enhance

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
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
	model      string
	claudePath string
	apiKey     string
	bruinPath  string
	debug      bool
}

// NewEnhancer creates a new Enhancer instance.
func NewEnhancer(model string) *Enhancer {
	if model == "" {
		model = defaultModel
	}
	claudePath, _ := exec.LookPath("claude")
	bruinPath, _ := exec.LookPath("bruin")
	return &Enhancer{
		model:      model,
		claudePath: claudePath,
		bruinPath:  bruinPath,
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

	// Run bruin format on the file
	if err := e.runBruinFormat(ctx, asset.DefinitionFile.Path); err != nil {
		return errors.Wrap(err, "failed to format asset")
	}

	// Run bruin validate on the file
	if err := e.runBruinValidate(ctx, asset.DefinitionFile.Path); err != nil {
		return errors.Wrap(err, "failed to validate asset")
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

	if e.debug {
		return e.runClaudeWithStreaming(cmd)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := stderr.String()
		if errMsg == "" {
			errMsg = stdout.String()
		}
		return errors.Wrapf(err, "claude CLI failed: %s", errMsg)
	}

	return nil
}

// runClaudeWithStreaming runs the Claude CLI and streams output in real-time.
func (e *Enhancer) runClaudeWithStreaming(cmd *exec.Cmd) error {
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "failed to create stdout pipe")
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return errors.Wrap(err, "failed to create stderr pipe")
	}

	if err := cmd.Start(); err != nil {
		return errors.Wrap(err, "failed to start claude CLI")
	}

	done := make(chan error, 2)
	go func() {
		_, copyErr := io.Copy(os.Stdout, stdoutPipe)
		done <- copyErr
	}()
	go func() {
		_, copyErr := io.Copy(os.Stderr, stderrPipe)
		done <- copyErr
	}()

	<-done
	<-done

	if err := cmd.Wait(); err != nil {
		return errors.Wrap(err, "claude CLI failed")
	}

	return nil
}

// runBruinFormat runs bruin format on the asset file.
func (e *Enhancer) runBruinFormat(ctx context.Context, filePath string) error {
	if e.bruinPath == "" {
		return errors.New("bruin CLI not found")
	}

	cmd := exec.CommandContext(ctx, e.bruinPath, "format", filePath) //nolint:gosec
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "bruin format failed: %s", string(output))
	}
	return nil
}

// runBruinValidate runs bruin validate on the asset file.
func (e *Enhancer) runBruinValidate(ctx context.Context, filePath string) error {
	if e.bruinPath == "" {
		return errors.New("bruin CLI not found")
	}

	cmd := exec.CommandContext(ctx, e.bruinPath, "validate", filePath) //nolint:gosec
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "bruin validate failed: %s", string(output))
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
