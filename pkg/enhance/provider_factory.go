package enhance

import (
	"github.com/spf13/afero"
)

// NewClaudeProvider creates a new Claude CLI provider.
func NewClaudeProvider(model string, fs afero.Fs) Provider {
	config := CLIProviderConfig{
		Name:         "claude",
		BinaryName:   "claude",
		DefaultModel: "claude-sonnet-4-20250514",
		UseAPIKeyEnv: true,
		APIKeyEnvVar: "ANTHROPIC_API_KEY",
		BuildArgs: func(modelName, prompt, systemPrompt string) []string {
			args := []string{
				"-p",
				"--output-format", "text",
				"--model", modelName,
				"--dangerously-skip-permissions",
			}
			if systemPrompt != "" {
				args = append(args, "--append-system-prompt", systemPrompt)
			}
			args = append(args, prompt)
			return args
		},
	}
	return NewCLIProvider(config, model, fs)
}

// NewOpenCodeProvider creates a new OpenCode CLI provider.
func NewOpenCodeProvider(model string, fs afero.Fs) Provider {
	config := CLIProviderConfig{
		Name:         "opencode",
		BinaryName:   "opencode",
		DefaultModel: "anthropic/claude-sonnet-4-20250514",
		UseAPIKeyEnv: false, // Uses native auth system
		BuildArgs: func(modelName, prompt, systemPrompt string) []string {
			args := []string{"run"}
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
		},
	}
	return NewCLIProvider(config, model, fs)
}

// NewCodexProvider creates a new Codex CLI provider.
func NewCodexProvider(model string, fs afero.Fs) Provider {
	config := CLIProviderConfig{
		Name:         "codex",
		BinaryName:   "codex",
		DefaultModel: "gpt-5-codex",
		UseAPIKeyEnv: false, // Uses native auth system (requires `codex` login first)
		BuildArgs: func(modelName, prompt, systemPrompt string) []string {
			args := []string{"exec"}
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
		},
	}
	return NewCLIProvider(config, model, fs)
}
