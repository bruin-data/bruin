package enhance

import (
	"github.com/spf13/afero"
)

// NewClaudeProvider creates a new Claude CLI provider.
func NewClaudeProvider(model string, fs afero.Fs) Provider {
	buildClaudeArgs := func(modelName, prompt, systemPrompt, outputFormat string, verbose bool) []string {
		args := []string{
			"-p",
			"--output-format", outputFormat,
			"--model", modelName,
			"--dangerously-skip-permissions",
		}
		if verbose {
			args = append(args, "--verbose")
		}
		if systemPrompt != "" {
			args = append(args, "--append-system-prompt", systemPrompt)
		}
		args = append(args, prompt)
		return args
	}

	config := CLIProviderConfig{
		Name:         "claude",
		BinaryName:   "claude",
		DefaultModel: "claude-sonnet-4-20250514",
		UseAPIKeyEnv: true,
		APIKeyEnvVar: "ANTHROPIC_API_KEY",
		BuildArgs: func(modelName, prompt, systemPrompt string) []string {
			return buildClaudeArgs(modelName, prompt, systemPrompt, "text", false)
		},
		BuildStreamArgs: func(modelName, prompt, systemPrompt string) []string {
			return buildClaudeArgs(modelName, prompt, systemPrompt, "stream-json", true)
		},
		ParseStream: parseClaudeStreamJSON,
	}
	return NewCLIProvider(config, model, fs)
}

// NewOpenCodeProvider creates a new OpenCode CLI provider.
func NewOpenCodeProvider(model string, fs afero.Fs) Provider {
	buildOpenCodeArgs := func(modelName, prompt, systemPrompt, format string) []string {
		args := []string{"run"}
		if modelName != "" {
			args = append(args, "--model", modelName)
		}
		if format != "" {
			args = append(args, "--format", format)
		}

		// Combine system prompt and user prompt
		fullPrompt := prompt
		if systemPrompt != "" {
			fullPrompt = systemPrompt + "\n\n" + prompt
		}
		args = append(args, fullPrompt)
		return args
	}

	config := CLIProviderConfig{
		Name:         "opencode",
		BinaryName:   "opencode",
		DefaultModel: "anthropic/claude-sonnet-4-20250514",
		UseAPIKeyEnv: false, // Uses native auth system
		BuildArgs: func(modelName, prompt, systemPrompt string) []string {
			return buildOpenCodeArgs(modelName, prompt, systemPrompt, "")
		},
		BuildStreamArgs: func(modelName, prompt, systemPrompt string) []string {
			return buildOpenCodeArgs(modelName, prompt, systemPrompt, "json")
		},
		ParseStream: parseOpenCodeStreamJSON,
	}
	return NewCLIProvider(config, model, fs)
}

// NewCodexProvider creates a new Codex CLI provider.
func NewCodexProvider(model string, fs afero.Fs) Provider {
	buildCodexArgs := func(modelName, prompt, systemPrompt string, json bool) []string {
		args := []string{"exec", "--full-auto"}
		if json {
			args = append(args, "--json")
		}
		if modelName != "" {
			args = append(args, "--model", modelName)
		}

		// Combine system prompt and user prompt
		fullPrompt := prompt
		if systemPrompt != "" {
			fullPrompt = systemPrompt + "\n\n" + prompt
		}
		args = append(args, fullPrompt)
		return args
	}

	config := CLIProviderConfig{
		Name:         "codex",
		BinaryName:   "codex",
		DefaultModel: "",
		UseAPIKeyEnv: false, // Uses native auth system (requires `codex` login first)
		BuildArgs: func(modelName, prompt, systemPrompt string) []string {
			return buildCodexArgs(modelName, prompt, systemPrompt, false)
		},
		BuildStreamArgs: func(modelName, prompt, systemPrompt string) []string {
			return buildCodexArgs(modelName, prompt, systemPrompt, true)
		},
		ParseStream: parseCodexStreamJSON,
	}
	return NewCLIProvider(config, model, fs)
}
