package enhance

import (
	"context"
	"io"
)

// ProviderType represents the AI provider type.
type ProviderType string

const (
	ProviderClaude   ProviderType = "claude"
	ProviderOpenCode ProviderType = "opencode"
	ProviderCodex    ProviderType = "codex"
	ProviderCursor   ProviderType = "cursor"
)

// Provider defines the interface for AI CLI providers.
type Provider interface {
	Name() string
	EnsureCLI() error
	Enhance(ctx context.Context, prompt, systemPrompt string) error
	SetDebug(debug bool)
	SetAPIKey(apiKey string) // May be no-op for some providers
	SetOutput(w io.Writer)   // Sets a writer for streaming CLI output
}
