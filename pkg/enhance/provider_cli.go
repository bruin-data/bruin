package enhance

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

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
	BuildStreamArgs   func(model, prompt, systemPrompt string) []string // Args for streaming mode (optional).
	ParseStream       func(r io.Reader, w io.Writer)                    // Parse streaming output into human-readable lines (optional).
	CommonSearchPaths []string                                          // Additional search paths beyond standard ones.
}

// CLIProvider implements the Provider interface for any CLI-based AI tool.
type CLIProvider struct {
	config  CLIProviderConfig
	cliPath string
	model   string
	apiKey  string
	debug   bool
	output  io.Writer
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

// SetOutput sets a writer for streaming CLI output.
func (p *CLIProvider) SetOutput(w io.Writer) {
	p.output = w
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
	// Use streaming args if output writer is set and streaming is supported
	useStreaming := p.output != nil && !p.debug && p.config.BuildStreamArgs != nil
	var args []string
	if useStreaming {
		args = p.config.BuildStreamArgs(p.model, prompt, systemPrompt)
	} else {
		args = p.config.BuildArgs(p.model, prompt, systemPrompt)
	}

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

	switch {
	case p.debug:
		cmd.Stdout = os.Stdout
		cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)
	case useStreaming && p.config.ParseStream != nil:
		// Pipe stdout through the stream parser which writes human-readable lines to the output writer
		stdoutPipe, pipeErr := cmd.StdoutPipe()
		if pipeErr != nil {
			return errors.Wrap(pipeErr, "failed to create stdout pipe")
		}
		cmd.Stderr = io.MultiWriter(p.output, &stderr)

		if startErr := cmd.Start(); startErr != nil {
			return errors.Wrapf(startErr, "%s CLI failed to start", p.config.Name)
		}

		p.config.ParseStream(stdoutPipe, p.output)

		if err := cmd.Wait(); err != nil {
			errMsg := stderr.String()
			if errMsg != "" {
				return errors.Wrapf(err, "%s CLI failed: %s", p.config.Name, errMsg)
			}
			return errors.Wrapf(err, "%s CLI failed", p.config.Name)
		}

		// Some CLIs exit 0 but print errors to stderr (e.g. invalid model).
		// Surface stderr as an error if it looks like a failure.
		if errMsg := stderr.String(); looksLikeCLIError(errMsg) {
			return errors.Errorf("%s CLI reported an error: %s", p.config.Name, errMsg)
		}
		return nil
	case p.output != nil:
		cmd.Stdout = p.output
		cmd.Stderr = io.MultiWriter(p.output, &stderr)
	}

	if err := cmd.Run(); err != nil {
		errMsg := stderr.String()
		if errMsg != "" {
			return errors.Wrapf(err, "%s CLI failed: %s", p.config.Name, errMsg)
		}
		return errors.Wrapf(err, "%s CLI failed", p.config.Name)
	}

	if errMsg := stderr.String(); looksLikeCLIError(errMsg) {
		return errors.Errorf("%s CLI reported an error: %s", p.config.Name, errMsg)
	}

	return nil
}

// looksLikeCLIError checks if stderr output indicates a CLI error despite exit code 0.
func looksLikeCLIError(stderr string) bool {
	if len(stderr) == 0 {
		return false
	}
	lower := strings.ToLower(stderr)
	errorIndicators := []string{
		"error:",
		"fatal:",
		"panic:",
		"unhandled",
		"exception",
		"stack trace",
		"traceback",
	}
	for _, indicator := range errorIndicators {
		if strings.Contains(lower, indicator) {
			return true
		}
	}
	return false
}

// parseClaudeStreamJSON reads Claude CLI stream-json output and writes human-readable
// activity lines (tool use, results) to the writer.
func parseClaudeStreamJSON(r io.Reader, w io.Writer) {
	scanner := bufio.NewScanner(r)
	// Claude stream-json can have large messages
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var event map[string]interface{}
		if err := json.Unmarshal(line, &event); err != nil {
			continue
		}

		// Extract meaningful activity from stream events
		msgType, _ := event["type"].(string)
		switch msgType {
		case "assistant":
			// Tool use events
			if msg, ok := event["message"].(map[string]interface{}); ok {
				if content, ok := msg["content"].([]interface{}); ok {
					for _, block := range content {
						if b, ok := block.(map[string]interface{}); ok {
							if b["type"] == "tool_use" {
								toolName, _ := b["name"].(string)
								if toolName != "" {
									detail := claudeToolDetail(toolName, b["input"])
									if detail != "" {
										fmt.Fprintf(w, "%s: %s\n", toolName, detail)
									} else {
										fmt.Fprintf(w, "%s\n", toolName)
									}
								}
							}
						}
					}
				}
			}
		case "result":
			// Final result
			if sub, _ := event["subtype"].(string); sub == "success" {
				fmt.Fprintln(w, "enhancement complete")
			}
		}
	}
}

// claudeToolDetail extracts a human-readable summary from a Claude tool_use input.
func claudeToolDetail(toolName string, rawInput interface{}) string {
	input, ok := rawInput.(map[string]interface{})
	if !ok || input == nil {
		return ""
	}

	switch toolName {
	case "Read":
		return firstString(input, "file_path", "path")
	case "Edit":
		if fp := firstString(input, "file_path", "path"); fp != "" {
			return fp
		}
	case "Write":
		return firstString(input, "file_path", "path")
	case "Glob":
		return firstString(input, "pattern", "glob")
	case "Grep":
		if pattern := firstString(input, "pattern", "regex"); pattern != "" {
			if fp := firstString(input, "path", "include"); fp != "" {
				return fmt.Sprintf(`"%s" in %s`, pattern, fp)
			}
			return fmt.Sprintf(`"%s"`, pattern)
		}
	case "Bash":
		if cmd := firstString(input, "command", "cmd"); cmd != "" {
			if len(cmd) > 80 {
				cmd = cmd[:77] + "..."
			}
			return cmd
		}
	case "WebFetch":
		return firstString(input, "url")
	case "WebSearch":
		return firstString(input, "query")
	}

	return ""
}

// firstString returns the first non-empty string value from the map for the given keys.
func firstString(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// parseCodexStreamJSON reads Codex CLI --json JSONL output and writes human-readable
// activity lines to the writer.
func parseCodexStreamJSON(r io.Reader, w io.Writer) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var event map[string]interface{}
		if err := json.Unmarshal(line, &event); err != nil {
			continue
		}

		msgType, _ := event["type"].(string)
		switch msgType {
		case "item.started":
			if item, ok := event["item"].(map[string]interface{}); ok {
				if itemType, _ := item["type"].(string); itemType == "command_execution" {
					if cmd, _ := item["command"].(string); cmd != "" {
						fmt.Fprintf(w, "exec: %s\n", cmd)
					}
				}
			}
		case "item.completed":
			if item, ok := event["item"].(map[string]interface{}); ok {
				itemType, _ := item["type"].(string)
				switch itemType {
				case "command_execution":
					if status, _ := item["status"].(string); status == "completed" {
						exitCode, _ := item["exit_code"].(float64)
						fmt.Fprintf(w, "command finished (exit %d)\n", int(exitCode))
					}
				case "agent_message":
					// Agent produced a text message — show a truncated preview
					if text, _ := item["text"].(string); text != "" {
						preview := text
						if len(preview) > 80 {
							preview = preview[:77] + "..."
						}
						// Only show if it's short (not a full file listing dump)
						if len(text) < 200 {
							fmt.Fprintf(w, "agent: %s\n", preview)
						}
					}
				}
			}
		case "turn.completed":
			fmt.Fprintln(w, "enhancement complete")
		}
	}
}

// parseOpenCodeStreamJSON reads OpenCode CLI --format json JSONL output and writes
// human-readable activity lines to the writer.
func parseOpenCodeStreamJSON(r io.Reader, w io.Writer) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var event map[string]interface{}
		if err := json.Unmarshal(line, &event); err != nil {
			continue
		}

		msgType, _ := event["type"].(string)
		switch msgType {
		case "tool_use":
			if part, ok := event["part"].(map[string]interface{}); ok {
				toolName, _ := part["tool"].(string)
				if toolName != "" {
					// Try to get the description/title from state.input
					desc := ""
					if state, ok := part["state"].(map[string]interface{}); ok {
						if input, ok := state["input"].(map[string]interface{}); ok {
							desc, _ = input["description"].(string)
						}
					}
					if desc != "" {
						fmt.Fprintf(w, "tool: %s — %s\n", toolName, desc)
					} else {
						fmt.Fprintf(w, "tool: %s\n", toolName)
					}
				}
			}
		case "step_finish":
			if part, ok := event["part"].(map[string]interface{}); ok {
				if reason, _ := part["reason"].(string); reason == "stop" {
					fmt.Fprintln(w, "enhancement complete")
				}
			}
		}
	}
}
