package enhance

import (
	"context"
)

// ProviderType represents the AI provider type
type ProviderType string

const (
	ProviderClaude   ProviderType = "claude"
	ProviderOpenCode ProviderType = "opencode"
)

// Provider defines the interface for AI CLI providers.
type Provider interface {
	Name() string
	EnsureCLI() error
	Enhance(ctx context.Context, prompt, systemPrompt string) error
	SetDebug(debug bool)
	SetAPIKey(apiKey string) // May be no-op for some providers
}
