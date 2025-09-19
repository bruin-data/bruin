package claudecode

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Parameter constants
const (
	ParamPrompt          = "prompt"
	ParamModel           = "model"
	ParamFallbackModel   = "fallback_model"
	ParamOutputFormat    = "output_format"
	ParamSystemPrompt    = "system_prompt"
	ParamAllowedDirs     = "allowed_directories"
	ParamAllowedTools    = "allowed_tools"
	ParamDisallowedTools = "disallowed_tools"
	ParamSkipPermissions = "skip_permissions"
	ParamPermissionMode  = "permission_mode"
	ParamSessionID       = "session_id"
	ParamContinueSession = "continue_session"
	ParamDebug           = "debug"
	ParamVerbose         = "verbose"
)

// Valid parameter values
var (
	ValidModels = map[string]bool{
		"opus":   true,
		"sonnet": true,
		"haiku":  true,
		// Also allow full model names
		"claude-3-opus-20240229":         true,
		"claude-3-5-sonnet-20241022":     true,
		"claude-3-5-haiku-20241022":      true,
		"claude-3-sonnet-20240229":       true,
		"claude-3-haiku-20240307":        true,
	}
	
	ValidOutputFormats = map[string]bool{
		"text":        true,
		"json":        true,
		"stream-json": true,
	}
	
	ValidPermissionModes = map[string]bool{
		"default":           true,
		"plan":              true,
		"acceptEdits":       true,
		"bypassPermissions": true,
	}
)

// ClaudeParameters holds all configuration for Claude execution
type ClaudeParameters struct {
	Prompt          string
	Model           string
	FallbackModel   string
	OutputFormat    string
	SystemPrompt    string
	AllowedDirs     []string
	AllowedTools    string
	DisallowedTools string
	SkipPermissions bool
	PermissionMode  string
	SessionID       string
	ContinueSession bool
	Debug           bool
	Verbose         bool
}

// ClaudeResponse represents the JSON response when using json output format
type ClaudeResponse struct {
	Content string                 `json:"content"`
	Model   string                 `json:"model,omitempty"`
	Error   string                 `json:"error,omitempty"`
	Meta    map[string]interface{} `json:"meta,omitempty"`
}

type ClaudeCodeOperator struct {
	renderer *jinja.Renderer
}

func NewClaudeCodeOperator(renderer *jinja.Renderer) *ClaudeCodeOperator {
	return &ClaudeCodeOperator{
		renderer: renderer,
	}
}

// log is a helper function to write messages to the context writer
func log(ctx context.Context, message string) {
	if ctx.Value(executor.KeyPrinter) == nil {
		return
	}

	if !strings.HasSuffix(message, "\n") {
		message += "\n"
	}

	writer := ctx.Value(executor.KeyPrinter).(io.Writer)
	_, _ = writer.Write([]byte(message))
}

func (o *ClaudeCodeOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	assetInstance, ok := ti.(*scheduler.AssetInstance)
	if !ok {
		return errors.New("claude_code assets can only be run as a main asset")
	}

	var ctxWithLogger context.Context
	if ctx.Value(executor.ContextLogger) == nil {
		logger := zap.NewNop().Sugar()
		ctxWithLogger = context.WithValue(ctx, executor.ContextLogger, logger)
	} else {
		ctxWithLogger = ctx
	}

	logger := ctxWithLogger.Value(executor.ContextLogger).(logger.Logger)
	asset := assetInstance.GetAsset()

	logger.Debugf("Running Claude Code asset: %s", asset.Name)

	// Check if claude CLI is installed
	claudePath, err := exec.LookPath("claude")
	if err != nil {
		logger.Debug("Claude CLI not found, attempting to install...")
		log(ctx, "Claude CLI not found, installing...")
		claudePath, err = o.installClaudeCLI(logger)
		if err != nil {
			return errors.Wrap(err, "failed to install Claude CLI")
		}
		logger.Debug("Claude CLI installed successfully")
		log(ctx, "Claude CLI installed successfully")
	}

	// Extract all parameters
	params, err := o.extractParameters(asset)
	if err != nil {
		return errors.Wrap(err, "failed to extract parameters")
	}

	// Validate parameters
	if err := o.validateParameters(params); err != nil {
		return errors.Wrap(err, "parameter validation failed")
	}

	// Render Jinja templates in prompt and system prompt
	renderedPrompt, err := o.renderer.Render(params.Prompt)
	if err != nil {
		return errors.Wrap(err, "failed to render prompt template")
	}
	params.Prompt = renderedPrompt

	if params.SystemPrompt != "" {
		renderedSystemPrompt, err := o.renderer.Render(params.SystemPrompt)
		if err != nil {
			return errors.Wrap(err, "failed to render system prompt template")
		}
		params.SystemPrompt = renderedSystemPrompt
	}

	logger.Debugf("Parameters: Model=%s, OutputFormat=%s, PermissionMode=%s", 
		params.Model, params.OutputFormat, params.PermissionMode)

	// Build command
	cmdArgs := o.buildCommand(params)
	cmd := exec.CommandContext(ctx, claudePath, cmdArgs...)

	// Log the command for debugging
	logger.Debugf("Executing command: claude %s", strings.Join(cmdArgs, " "))

	// For JSON output format, we need to capture the output for parsing
	if params.OutputFormat == "json" {
		output, err := cmd.CombinedOutput()
		if err != nil {
			logger.Debugf("Claude execution failed: %s", string(output))
			return errors.Wrapf(err, "failed to execute Claude: %s", string(output))
		}
		
		var response ClaudeResponse
		if err := json.Unmarshal(output, &response); err != nil {
			logger.Debugf("Failed to parse JSON response: %s", string(output))
			// Still write the raw output so user can see what went wrong
			log(ctx, string(output))
			return errors.Wrap(err, "failed to parse Claude JSON response")
		}
		
		if response.Error != "" {
			return errors.New(fmt.Sprintf("Claude returned error: %s", response.Error))
		}
		
		// Write the Claude response content to the output
		log(ctx, response.Content)
		
		logger.Debugf("Claude response (JSON): %+v", response)
		// Store the structured response in context for downstream tasks
		// This could be extended to save to a file or database
	} else {
		// For text and stream-json formats, stream output in real-time
		var output io.Writer = os.Stdout
		if ctx.Value(executor.KeyPrinter) != nil {
			output = ctx.Value(executor.KeyPrinter).(io.Writer)
		}
		
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return errors.Wrap(err, "failed to get stdout pipe")
		}
		
		stderr, err := cmd.StderrPipe()
		if err != nil {
			return errors.Wrap(err, "failed to get stderr pipe")
		}
		
		wg := new(errgroup.Group)
		wg.Go(func() error { return o.consumePipe(stdout, output) })
		wg.Go(func() error { return o.consumePipe(stderr, output) })
		
		err = cmd.Start()
		if err != nil {
			return errors.Wrap(err, "failed to start Claude command")
		}
		
		res := cmd.Wait()
		if res != nil {
			return res
		}
		
		err = wg.Wait()
		if err != nil {
			return errors.Wrap(err, "failed to consume pipe")
		}
	}

	return nil
}

func (o *ClaudeCodeOperator) extractParameters(asset *pipeline.Asset) (*ClaudeParameters, error) {
	if asset.Parameters == nil {
		return nil, errors.New("no parameters defined for asset")
	}

	params := &ClaudeParameters{
		OutputFormat:   "text", // default
		PermissionMode: "default", // default
	}

	// Required: prompt
	prompt, exists := asset.Parameters[ParamPrompt]
	if !exists || strings.TrimSpace(prompt) == "" {
		return nil, errors.New("'prompt' parameter is required and cannot be empty")
	}
	params.Prompt = prompt

	// Optional: model
	if model, exists := asset.Parameters[ParamModel]; exists && model != "" {
		params.Model = model
	}

	// Optional: fallback_model
	if fallbackModel, exists := asset.Parameters[ParamFallbackModel]; exists && fallbackModel != "" {
		params.FallbackModel = fallbackModel
	}

	// Optional: output_format
	if format, exists := asset.Parameters[ParamOutputFormat]; exists && format != "" {
		params.OutputFormat = format
	}

	// Optional: system_prompt
	if systemPrompt, exists := asset.Parameters[ParamSystemPrompt]; exists && systemPrompt != "" {
		params.SystemPrompt = systemPrompt
	}

	// Optional: allowed_directories (comma-separated)
	if dirs, exists := asset.Parameters[ParamAllowedDirs]; exists && dirs != "" {
		params.AllowedDirs = strings.Split(dirs, ",")
		for i := range params.AllowedDirs {
			params.AllowedDirs[i] = strings.TrimSpace(params.AllowedDirs[i])
		}
	}

	// Optional: allowed_tools
	if tools, exists := asset.Parameters[ParamAllowedTools]; exists && tools != "" {
		params.AllowedTools = tools
	}

	// Optional: disallowed_tools
	if tools, exists := asset.Parameters[ParamDisallowedTools]; exists && tools != "" {
		params.DisallowedTools = tools
	}

	// Optional: skip_permissions
	if skip, exists := asset.Parameters[ParamSkipPermissions]; exists {
		params.SkipPermissions = skip == "true" || skip == "yes" || skip == "1"
	}

	// Optional: permission_mode
	if mode, exists := asset.Parameters[ParamPermissionMode]; exists && mode != "" {
		params.PermissionMode = mode
	}

	// Optional: session_id
	if sessionID, exists := asset.Parameters[ParamSessionID]; exists && sessionID != "" {
		params.SessionID = sessionID
	}

	// Optional: continue_session
	if continueSession, exists := asset.Parameters[ParamContinueSession]; exists {
		params.ContinueSession = continueSession == "true" || continueSession == "yes" || continueSession == "1"
	}

	// Optional: debug
	if debug, exists := asset.Parameters[ParamDebug]; exists {
		params.Debug = debug == "true" || debug == "yes" || debug == "1"
	}

	// Optional: verbose
	if verbose, exists := asset.Parameters[ParamVerbose]; exists {
		params.Verbose = verbose == "true" || verbose == "yes" || verbose == "1"
	}

	return params, nil
}

func (o *ClaudeCodeOperator) validateParameters(params *ClaudeParameters) error {
	// Validate model if specified
	if params.Model != "" && !ValidModels[params.Model] {
		// Check if it looks like a model name (contains "claude")
		if !strings.Contains(params.Model, "claude") {
			return fmt.Errorf("invalid model: %s. Valid options are: opus, sonnet, haiku, or full model names like claude-3-5-sonnet-20241022", params.Model)
		}
	}

	// Validate fallback model if specified
	if params.FallbackModel != "" && !ValidModels[params.FallbackModel] {
		if !strings.Contains(params.FallbackModel, "claude") {
			return fmt.Errorf("invalid fallback_model: %s", params.FallbackModel)
		}
	}

	// Validate output format
	if !ValidOutputFormats[params.OutputFormat] {
		return fmt.Errorf("invalid output_format: %s. Valid options are: text, json, stream-json", params.OutputFormat)
	}

	// Validate permission mode
	if !ValidPermissionModes[params.PermissionMode] {
		return fmt.Errorf("invalid permission_mode: %s. Valid options are: default, plan, acceptEdits, bypassPermissions", params.PermissionMode)
	}

	// Validate session ID if specified (should be UUID format)
	if params.SessionID != "" {
		uuidRegex := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
		if !uuidRegex.MatchString(params.SessionID) {
			return fmt.Errorf("session_id must be a valid UUID")
		}
	}

	// Validate directories exist if specified
	for _, dir := range params.AllowedDirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			return fmt.Errorf("allowed directory does not exist: %s", dir)
		}
	}

	return nil
}

func (o *ClaudeCodeOperator) buildCommand(params *ClaudeParameters) []string {
	var args []string

	// Always use print mode for non-interactive execution
	args = append(args, "-p")

	// Add output format if not default
	if params.OutputFormat != "text" && params.OutputFormat != "" {
		args = append(args, "--output-format", params.OutputFormat)
	}

	// Add model if specified
	if params.Model != "" {
		args = append(args, "--model", params.Model)
	}

	// Add fallback model if specified
	if params.FallbackModel != "" {
		args = append(args, "--fallback-model", params.FallbackModel)
	}

	// Add system prompt if specified
	if params.SystemPrompt != "" {
		args = append(args, "--append-system-prompt", params.SystemPrompt)
	}

	// Add allowed directories
	for _, dir := range params.AllowedDirs {
		args = append(args, "--add-dir", dir)
	}

	// Add allowed tools
	if params.AllowedTools != "" {
		args = append(args, "--allowed-tools", params.AllowedTools)
	}

	// Add disallowed tools
	if params.DisallowedTools != "" {
		args = append(args, "--disallowed-tools", params.DisallowedTools)
	}

	// Add skip permissions flag if enabled
	if params.SkipPermissions {
		args = append(args, "--dangerously-skip-permissions")
	}

	// Add permission mode if not default
	if params.PermissionMode != "" && params.PermissionMode != "default" {
		args = append(args, "--permission-mode", params.PermissionMode)
	}

	// Add session ID if specified
	if params.SessionID != "" {
		args = append(args, "--session-id", params.SessionID)
	}

	// Add continue session flag if enabled
	if params.ContinueSession {
		args = append(args, "--continue")
	}

	// Add debug flag if enabled
	if params.Debug {
		args = append(args, "--debug")
	}

	// Add verbose flag if enabled
	if params.Verbose {
		args = append(args, "--verbose")
	}

	// Finally, add the prompt (must be last)
	args = append(args, params.Prompt)

	return args
}

func (o *ClaudeCodeOperator) consumePipe(pipe io.Reader, output io.Writer) error {
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		// the size of the slice here is important, the added 4 at the end includes the 3 bytes for the prefix and the 1 byte for the newline
		msg := make([]byte, len(scanner.Bytes())+4)
		copy(msg, ">> ")
		copy(msg[3:], scanner.Bytes())
		msg[len(msg)-1] = '\n'

		_, err := output.Write(msg)
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *ClaudeCodeOperator) installClaudeCLI(logger logger.Logger) (string, error) {
	logger.Debug("Installing Claude CLI using official installation script...")
	
	// Run the official installation script
	// Note: This requires bash and curl to be available, which may not be the case on Windows
	// Windows users should install Claude CLI manually or use WSL
	installCmd := exec.Command("bash", "-c", "curl -fsSL https://claude.ai/install.sh | bash")
	
	var stdout, stderr bytes.Buffer
	installCmd.Stdout = &stdout
	installCmd.Stderr = &stderr
	
	err := installCmd.Run()
	if err != nil {
		logger.Debugf("Installation stderr: %s", stderr.String())
		return "", errors.Wrapf(err, "failed to install Claude CLI: %s", stderr.String())
	}
	
	logger.Debugf("Installation output: %s", stdout.String())
	
	// After installation, try to find claude again
	claudePath, err := exec.LookPath("claude")
	if err != nil {
		// If still not found in PATH, check common installation locations
		possiblePaths := []string{
			"/usr/local/bin/claude",
			os.ExpandEnv("$HOME/.local/bin/claude"),
			os.ExpandEnv("$HOME/bin/claude"),
		}
		
		for _, path := range possiblePaths {
			if _, err := os.Stat(path); err == nil {
				logger.Debugf("Found Claude CLI at: %s", path)
				return path, nil
			}
		}
		
		return "", errors.Wrap(err, "Claude CLI installation succeeded but binary not found in PATH")
	}
	
	logger.Debugf("Claude CLI found at: %s", claudePath)
	return claudePath, nil
}