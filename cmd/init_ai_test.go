package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestIsAITemplateArgument(t *testing.T) {
	t.Parallel()

	require.True(t, isAITemplateArgument("ai"))
	require.True(t, isAITemplateArgument("ai-agents-md"))
	require.True(t, isAITemplateArgument("ai-skill-self-heal"))
	require.False(t, isAITemplateArgument("default"))
}

func TestInstallAITemplateCreatesAgentsFile(t *testing.T) {
	t.Parallel()

	targetRoot := t.TempDir()
	template := lookupAITemplate(aiAgentsTemplateName)
	require.NotNil(t, template)

	result, err := installAITemplate(t.Context(), *template, targetRoot, aiInstallOptions{})
	require.NoError(t, err)

	agentsPath := filepath.Join(targetRoot, "AGENTS.md")
	require.Contains(t, result.Created, agentsPath)

	content, err := os.ReadFile(agentsPath)
	require.NoError(t, err)
	require.Contains(t, string(content), aiAgentsSectionStart)
	require.Contains(t, string(content), "Bruin Pipeline Agent Guidance")
	require.NoFileExists(t, filepath.Join(targetRoot, ".bruin.yml"))
}

func TestInstallAITemplateAddsAgentsSectionToExistingFile(t *testing.T) {
	t.Parallel()

	targetRoot := t.TempDir()
	agentsPath := filepath.Join(targetRoot, "AGENTS.md")
	require.NoError(t, os.WriteFile(agentsPath, []byte("# Existing\n\nKeep this.\n"), 0o644))

	template := lookupAITemplate(aiAgentsTemplateName)
	require.NotNil(t, template)

	result, err := installAITemplate(t.Context(), *template, targetRoot, aiInstallOptions{
		conflictResolver: func(path string) (aiConflictAction, error) {
			require.Equal(t, agentsPath, path)
			return aiConflictAdd, nil
		},
	})
	require.NoError(t, err)
	require.Contains(t, result.Updated, agentsPath)

	content, err := os.ReadFile(agentsPath)
	require.NoError(t, err)
	require.Contains(t, string(content), "# Existing")
	require.Contains(t, string(content), "Keep this.")
	require.Equal(t, 1, strings.Count(string(content), aiAgentsSectionStart))
	require.Equal(t, 1, strings.Count(string(content), aiAgentsSectionEnd))
}

func TestInstallAITemplateSelfHealCreatesSkillsAndConnections(t *testing.T) {
	t.Parallel()

	targetRoot := t.TempDir()
	template := lookupAITemplate(aiSelfHealTemplateName)
	require.NotNil(t, template)

	result, err := installAITemplate(t.Context(), *template, targetRoot, aiInstallOptions{
		connectionResolver: func(connection aiConnectionType) (bool, error) {
			return true, nil
		},
	})
	require.NoError(t, err)

	expectedSkills := []string{
		"duplicate-investigate",
		"freshness-check",
		"maintenance-action",
		"pipeline-diagnose",
		"quality-check-investigate",
		"schema-drift-check",
	}
	for _, skill := range expectedSkills {
		require.FileExists(t, filepath.Join(targetRoot, ".agents", "skills", skill, "SKILL.md"))
	}
	require.Len(t, result.Created, len(expectedSkills))
	require.ElementsMatch(t, []string{"bruin", "github"}, result.ConnectionsAdded)

	cm := readTestBruinConfig(t, filepath.Join(targetRoot, ".bruin.yml"))
	env := cm.Environments["default"]
	require.Len(t, env.Connections.BruinCloud, 1)
	require.Equal(t, "bruin-cloud", env.Connections.BruinCloud[0].Name)
	require.Equal(t, "${BRUIN_CLOUD_API_TOKEN}", env.Connections.BruinCloud[0].APIToken)
	require.Len(t, env.Connections.GitHub, 1)
	require.Equal(t, "github", env.Connections.GitHub[0].Name)
	require.Equal(t, "${GITHUB_TOKEN}", env.Connections.GitHub[0].AccessToken)
	require.Equal(t, "<github-owner>", env.Connections.GitHub[0].Owner)
	require.Equal(t, "<github-repo>", env.Connections.GitHub[0].Repo)
}

func TestInstallAITemplateDoesNotDuplicateExistingConnections(t *testing.T) {
	t.Parallel()

	targetRoot := t.TempDir()
	configPath := filepath.Join(targetRoot, ".bruin.yml")
	require.NoError(t, os.WriteFile(configPath, []byte(`default_environment: default
environments:
    default:
        connections:
            github:
                - name: existing-github
                  access_token: "${EXISTING_GITHUB_TOKEN}"
                  owner: existing
                  repo: repo
`), 0o644))

	template := lookupAITemplate(aiSelfHealTemplateName)
	require.NotNil(t, template)

	result, err := installAITemplate(context.Background(), *template, targetRoot, aiInstallOptions{
		connectionResolver: func(connection aiConnectionType) (bool, error) {
			return true, nil
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"bruin"}, result.ConnectionsAdded)

	cm := readTestBruinConfig(t, configPath)
	env := cm.Environments["default"]
	require.Len(t, env.Connections.GitHub, 1)
	require.Equal(t, "existing-github", env.Connections.GitHub[0].Name)
	require.Len(t, env.Connections.BruinCloud, 1)
}

func TestParseGitHubRemote(t *testing.T) {
	t.Parallel()

	tests := map[string][2]string{
		"git@github.com:bruin-data/bruin.git":       {"bruin-data", "bruin"},
		"https://github.com/bruin-data/bruin.git":   {"bruin-data", "bruin"},
		"ssh://git@github.com/bruin-data/bruin.git": {"bruin-data", "bruin"},
	}

	for remote, want := range tests {
		owner, repo := parseGitHubRemote(remote)
		require.Equal(t, want[0], owner)
		require.Equal(t, want[1], repo)
	}
}

func readTestBruinConfig(t *testing.T, path string) config.Config {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	var cm config.Config
	require.NoError(t, yaml.Unmarshal(content, &cm))
	return cm
}
