package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIKeyConnectionsAddDeleteAndMerge(t *testing.T) {
	t.Parallel()

	providerSlices := []struct {
		name string
		get  func(*Connections) []APIKeyConnection
	}{
		{"openai", func(c *Connections) []APIKeyConnection { return c.OpenAI }},
		{"opencode", func(c *Connections) []APIKeyConnection { return c.OpenCode }},
		{"openrouter", func(c *Connections) []APIKeyConnection { return c.OpenRouter }},
		{"google", func(c *Connections) []APIKeyConnection { return c.Google }},
		{"typesafe", func(c *Connections) []APIKeyConnection { return c.Typesafe }},
	}

	for _, provider := range providerSlices {
		t.Run(provider.name, func(t *testing.T) {
			t.Parallel()
			connections := &Connections{}
			cfg := &Config{Environments: map[string]Environment{"default": {Connections: connections}}}
			require.NoError(t, cfg.AddConnection("default", "primary", provider.name, map[string]any{"api_key": "test-key"}))
			require.Equal(t, "test-key", provider.get(connections)[0].GetAPIKey())

			merged := &Connections{}
			require.NoError(t, merged.MergeFrom(connections))
			assert.Equal(t, provider.get(connections), provider.get(merged))

			require.NoError(t, cfg.DeleteConnection("default", "primary"))
			assert.Empty(t, provider.get(connections))
		})
	}
}
