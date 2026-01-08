package enhance

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

const (
	defaultModel = "claude-sonnet-4-20250514"
)

// Enhancer coordinates the AI enhancement process for assets.
type Enhancer struct {
	fs          afero.Fs
	model       string
	claudePath  string
	apiKey      string
	bruinPath   string // path to bruin binary for MCP server
	useMCP      bool   // whether to use bruin MCP server
	repoRoot    string // path to the Bruin repository root
	environment string // environment name for database connections
	debug       bool   // whether to print debug information
}

// NewEnhancer creates a new Enhancer instance.
func NewEnhancer(fs afero.Fs, model string) *Enhancer {
	if model == "" {
		model = defaultModel
	}
	claudePath, _ := exec.LookPath("claude")
	bruinPath, _ := exec.LookPath("bruin")
	return &Enhancer{
		fs:         fs,
		model:      model,
		claudePath: claudePath,
		bruinPath:  bruinPath,
		useMCP:     bruinPath != "", // Enable MCP if bruin is available
	}
}

// NewEnhancerWithAPIKey creates a new Enhancer instance with an API key.
func NewEnhancerWithAPIKey(fs afero.Fs, model, apiKey string) *Enhancer {
	e := NewEnhancer(fs, model)
	e.apiKey = apiKey
	return e
}

// SetAPIKey sets the Anthropic API key to use for Claude CLI.
func (e *Enhancer) SetAPIKey(apiKey string) {
	e.apiKey = apiKey
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
// When MCP is enabled, Claude directly edits the file and returns nil suggestions.
// When MCP is disabled, Claude returns suggestions that need to be applied manually.
func (e *Enhancer) EnhanceAsset(ctx context.Context, asset *pipeline.Asset, pipelineName string) (*EnhancementSuggestions, error) {
	if err := e.EnsureClaudeCLI(); err != nil {
		return nil, errors.Wrap(err, "claude CLI not available")
	}

	// If MCP is enabled, use the agentic file-editing approach
	if e.useMCP && asset.DefinitionFile.Path != "" {
		return e.enhanceAssetWithMCP(ctx, asset, pipelineName)
	}

	// Fallback to JSON response mode
	return e.enhanceAssetWithJSON(ctx, asset, pipelineName)
}

// enhanceAssetWithMCP uses Claude to directly edit the asset file.
func (e *Enhancer) enhanceAssetWithMCP(ctx context.Context, asset *pipeline.Asset, pipelineName string) (*EnhancementSuggestions, error) {
	// Build prompt with file path
	prompt := BuildEnhancePromptWithFilePath(asset.DefinitionFile.Path, asset.Name, pipelineName)
	systemPrompt := GetSystemPrompt(true)

	// Call Claude CLI - Claude will use MCP tools to edit the file directly
	_, err := e.callClaude(ctx, prompt, systemPrompt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to enhance asset")
	}

	// Return nil suggestions since Claude edited the file directly
	// The caller should reload the asset to see the changes
	return nil, nil
}

// enhanceAssetWithJSON uses Claude to return JSON suggestions (fallback mode).
func (e *Enhancer) enhanceAssetWithJSON(ctx context.Context, asset *pipeline.Asset, pipelineName string) (*EnhancementSuggestions, error) {
	// Build the prompt
	prompt := BuildEnhancePrompt(asset, pipelineName)
	systemPrompt := GetSystemPrompt(false)

	// Call Claude CLI
	response, err := e.callClaude(ctx, prompt, systemPrompt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get AI suggestions")
	}

	// Parse response
	suggestions, err := ParseClaudeResponse(response)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse AI suggestions")
	}

	// Filter out suggestions for columns/checks that already exist
	suggestions = e.filterExistingSuggestions(suggestions, asset)

	return suggestions, nil
}

// callClaude executes the Claude CLI with the given prompt.
func (e *Enhancer) callClaude(ctx context.Context, prompt, systemPrompt string) (string, error) {
	args := []string{
		"-p", // print mode (non-interactive)
		"--output-format", "text",
		"--model", e.model,
		"--dangerously-skip-permissions",
	}

	// Add MCP server configuration if bruin is available
	if e.useMCP && e.bruinPath != "" {
		mcpConfig := e.buildMCPConfig()
		args = append(args, "--mcp-config", mcpConfig)
	}

	if systemPrompt != "" {
		args = append(args, "--append-system-prompt", systemPrompt)
	}

	// Add the prompt as the last argument
	args = append(args, prompt)

	cmd := exec.CommandContext(ctx, e.claudePath, args...)

	// Set API key as environment variable if provided
	if e.apiKey != "" {
		cmd.Env = append(os.Environ(), "ANTHROPIC_API_KEY="+e.apiKey)
	}

	// In debug mode, stream Claude CLI output in real-time using pipes
	if e.debug {
		return e.runClaudeWithStreaming(cmd)
	}

	// Non-debug mode: capture output
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := stderr.String()
		if errMsg == "" {
			errMsg = stdout.String()
		}
		return "", errors.Wrapf(err, "claude CLI failed: %s", errMsg)
	}

	return stdout.String(), nil
}

// runClaudeWithStreaming runs the Claude CLI and streams output in real-time.
func (e *Enhancer) runClaudeWithStreaming(cmd *exec.Cmd) (string, error) {
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return "", errors.Wrap(err, "failed to create stdout pipe")
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return "", errors.Wrap(err, "failed to create stderr pipe")
	}

	if err := cmd.Start(); err != nil {
		return "", errors.Wrap(err, "failed to start claude CLI")
	}

	// Stream stdout and stderr concurrently
	done := make(chan error, 2)
	go func() {
		_, copyErr := io.Copy(os.Stdout, stdoutPipe)
		done <- copyErr
	}()
	go func() {
		_, copyErr := io.Copy(os.Stderr, stderrPipe)
		done <- copyErr
	}()

	// Wait for both streams to complete
	<-done
	<-done

	if err := cmd.Wait(); err != nil {
		return "", errors.Wrap(err, "claude CLI failed")
	}

	return "", nil
}

// buildMCPConfig creates the MCP server configuration JSON for bruin.
func (e *Enhancer) buildMCPConfig() string {
	bruinConfig := map[string]interface{}{
		"command": e.bruinPath,
		"args":    []string{"mcp"},
	}

	// Add environment variables for database connectivity
	env := make(map[string]string)
	if e.repoRoot != "" {
		env["BRUIN_REPO_ROOT"] = e.repoRoot
	}
	if e.environment != "" {
		env["BRUIN_ENVIRONMENT"] = e.environment
	}
	if len(env) > 0 {
		bruinConfig["env"] = env
	}

	config := map[string]interface{}{
		"mcpServers": map[string]interface{}{
			"bruin": bruinConfig,
		},
	}
	jsonBytes, _ := json.Marshal(config)
	return string(jsonBytes)
}

// SetRepoRoot sets the Bruin repository root path for MCP database tools.
func (e *Enhancer) SetRepoRoot(repoRoot string) {
	e.repoRoot = repoRoot
}

// SetEnvironment sets the environment name for MCP database tools.
func (e *Enhancer) SetEnvironment(environment string) {
	e.environment = environment
}

// SetDebug enables or disables debug output.
// When debug is true, Claude CLI output is streamed directly to stdout/stderr.
func (e *Enhancer) SetDebug(debug bool) {
	e.debug = debug
}

// ClaudeResponse represents the JSON response when using json output format.
type ClaudeResponse struct {
	Content string `json:"content"`
	Error   string `json:"error,omitempty"`
}

// callClaudeJSON executes Claude CLI and expects JSON response.
func (e *Enhancer) callClaudeJSON(ctx context.Context, prompt, systemPrompt string) (string, error) {
	args := []string{
		"-p", // print mode (non-interactive)
		"--output-format", "json",
		"--model", e.model,
		"--dangerously-skip-permissions",
	}

	if systemPrompt != "" {
		args = append(args, "--append-system-prompt", systemPrompt)
	}

	args = append(args, prompt)

	cmd := exec.CommandContext(ctx, e.claudePath, args...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", errors.Wrapf(err, "claude CLI failed: %s", string(output))
	}

	var response ClaudeResponse
	if err := json.Unmarshal(output, &response); err != nil {
		// If JSON parsing fails, return raw output
		return string(output), nil
	}

	if response.Error != "" {
		return "", errors.New("Claude returned error: " + response.Error)
	}

	return response.Content, nil
}

// filterExistingSuggestions removes suggestions for things that already exist.
func (e *Enhancer) filterExistingSuggestions(suggestions *EnhancementSuggestions, asset *pipeline.Asset) *EnhancementSuggestions {
	if suggestions == nil {
		return nil
	}

	filtered := &EnhancementSuggestions{
		ColumnDescriptions: make(map[string]string),
		ColumnChecks:       make(map[string][]CheckSuggestion),
	}

	// Only include description if asset doesn't have one
	if asset.Description == "" && suggestions.AssetDescription != "" {
		filtered.AssetDescription = suggestions.AssetDescription
	}

	// Filter column descriptions - only for columns without descriptions
	for colName, desc := range suggestions.ColumnDescriptions {
		col := getColumnByName(asset, colName)
		if col != nil && col.Description == "" {
			filtered.ColumnDescriptions[colName] = desc
		}
	}

	// Filter column checks - only for checks that don't exist
	for colName, checks := range suggestions.ColumnChecks {
		col := getColumnByName(asset, colName)
		if col == nil {
			continue
		}

		var validChecks []CheckSuggestion
		for _, check := range checks {
			if !hasCheck(col.Checks, check.Name) {
				validChecks = append(validChecks, check)
			}
		}
		if len(validChecks) > 0 {
			filtered.ColumnChecks[colName] = validChecks
		}
	}

	// Filter tags - only tags that don't exist
	for _, tag := range suggestions.SuggestedTags {
		if !containsString(asset.Tags, tag) {
			filtered.SuggestedTags = append(filtered.SuggestedTags, tag)
		}
	}

	// Filter domains
	for _, domain := range suggestions.SuggestedDomains {
		if !containsString(asset.Domains, domain) {
			filtered.SuggestedDomains = append(filtered.SuggestedDomains, domain)
		}
	}

	// Only include owner if not set
	if asset.Owner == "" && suggestions.SuggestedOwner != "" {
		filtered.SuggestedOwner = suggestions.SuggestedOwner
	}

	// Filter custom checks
	for _, customCheck := range suggestions.CustomChecks {
		if !hasCustomCheck(asset.CustomChecks, customCheck.Name) {
			filtered.CustomChecks = append(filtered.CustomChecks, customCheck)
		}
	}

	return filtered
}

func getColumnByName(asset *pipeline.Asset, name string) *pipeline.Column {
	for i := range asset.Columns {
		if strings.EqualFold(asset.Columns[i].Name, name) {
			return &asset.Columns[i]
		}
	}
	return nil
}

// installClaudeCLI installs the Claude CLI using the official installation script.
func (e *Enhancer) installClaudeCLI() (string, error) {
	if runtime.GOOS == "windows" {
		return "", errors.New("automatic Claude CLI installation is not supported on Windows; please install manually or use WSL")
	}

	// Run the official installation script
	installCmd := exec.Command("bash", "-c", "curl -fsSL https://claude.ai/install.sh | bash")

	var stdout, stderr bytes.Buffer
	installCmd.Stdout = &stdout
	installCmd.Stderr = &stderr

	if err := installCmd.Run(); err != nil {
		return "", errors.Wrapf(err, "failed to install Claude CLI: %s", stderr.String())
	}

	// After installation, try to find claude in common locations
	claudePath, err := exec.LookPath("claude")
	if err != nil {
		// Check common installation paths
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

// IsClaudeCLIInstalled checks if Claude CLI is available.
func (e *Enhancer) IsClaudeCLIInstalled() bool {
	return e.claudePath != ""
}

// GetModel returns the model being used.
func (e *Enhancer) GetModel() string {
	return e.model
}
