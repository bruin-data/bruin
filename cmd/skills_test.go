package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSkillsCommand(t *testing.T) {
	t.Parallel()

	command := AISkills()

	assert.Equal(t, "skills", command.Name)
	assert.Empty(t, command.Commands)
}

func TestLoadBundledBruinSkillsFromMarkdownFiles(t *testing.T) {
	t.Parallel()

	skills, err := loadBundledBruinSkills()
	require.NoError(t, err)

	skill := findBundledSkill(t, skills, "bruin-semantic-layer")

	assert.Equal(t, "bruin-semantic-layer", skill.Name)

	var skillFile bundledSkillFile
	for _, file := range skill.Files {
		if file.Path == "SKILL.md" {
			skillFile = file
			break
		}
	}
	assert.Equal(t, "SKILL.md", skillFile.Path)
	assert.Contains(t, skillFile.Content, "# Bruin Semantic Layer")

	metadata, ok, err := parseSkillFrontmatter(skillFile.Content)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, skill.Name, metadata.Name)
	assert.NotEmpty(t, skill.Files)

	selfHealSkill := findBundledSkill(t, skills, "pipeline-diagnose")
	assert.Equal(t, []aiConnectionType{aiConnectionBruinCloud, aiConnectionGitHub}, selfHealSkill.RequiredConnections)
}

func TestSkillsInitCommand_RunCreatesAgentsHomeByDefault(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	result, err := (SkillsInitCommand{}).Run(root)
	require.NoError(t, err)

	require.Len(t, result.Skills, 7)
	skill := findSkillResult(t, result, "bruin-semantic-layer")
	assert.Equal(t, skillStatusInstalled, skill.Status)
	assert.Equal(t, filepath.Join(root, ".agents", "skills"), result.PrimarySkillsDir)
	assert.Equal(t, filepath.Join(root, ".agents", "skills", "bruin-semantic-layer"), skill.Path)

	content := readRequiredFile(t, filepath.Join(skill.Path, "SKILL.md"))
	assert.Contains(t, content, "name: bruin-semantic-layer")

	_, err = os.Stat(filepath.Join(root, ".claude"))
	assert.True(t, os.IsNotExist(err))
}

func TestSkillsInitCommand_RunWithOptionsInstallsSelectedSkill(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	result, err := (SkillsInitCommand{}).RunWithOptions(t.Context(), root, SkillsInstallOptions{
		SkillNames: []string{"bruin-semantic-layer"},
	})
	require.NoError(t, err)

	require.Len(t, result.Skills, 1)
	assert.Equal(t, "bruin-semantic-layer", result.Skills[0].Name)
	assert.FileExists(t, filepath.Join(root, ".agents", "skills", "bruin-semantic-layer", "SKILL.md"))
	require.NoDirExists(t, filepath.Join(root, ".agents", "skills", "pipeline-diagnose"))
}

func TestSkillsInitCommand_RunUsesClaudeWhenOnlyClaudeExists(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, ".claude"), 0o755))

	result, err := (SkillsInitCommand{}).Run(root)
	require.NoError(t, err)

	require.Len(t, result.Skills, 7)
	assert.Equal(t, filepath.Join(root, ".claude", "skills"), result.PrimarySkillsDir)
	semanticSkill := findSkillResult(t, result, "bruin-semantic-layer")
	assert.Equal(t, filepath.Join(root, ".claude", "skills", "bruin-semantic-layer"), semanticSkill.Path)
	assert.FileExists(t, filepath.Join(root, ".claude", "skills", "bruin-semantic-layer", "SKILL.md"))

	_, err = os.Stat(filepath.Join(root, ".agents"))
	assert.True(t, os.IsNotExist(err))
}

func TestSkillsInitCommand_RunLinksClaudeToAgentsWhenBothHomesExist(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, ".agents"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, ".claude"), 0o755))

	result, err := (SkillsInitCommand{}).Run(root)
	require.NoError(t, err)

	require.Len(t, result.Skills, 7)
	skill := findSkillResult(t, result, "bruin-semantic-layer")
	require.Len(t, skill.Links, 1)

	primarySkillPath := filepath.Join(root, ".agents", "skills", "bruin-semantic-layer")
	linkPath := filepath.Join(root, ".claude", "skills", "bruin-semantic-layer")
	assert.Equal(t, primarySkillPath, skill.Path)
	assert.Equal(t, linkPath, skill.Links[0])

	info, err := os.Lstat(linkPath)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink)

	target, err := os.Readlink(linkPath)
	require.NoError(t, err)
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(linkPath), target)
	}
	target, err = filepath.Abs(target)
	require.NoError(t, err)
	primary, err := filepath.Abs(primarySkillPath)
	require.NoError(t, err)
	assert.Equal(t, primary, filepath.Clean(target))
	assert.FileExists(t, filepath.Join(linkPath, "SKILL.md"))
}

func TestSkillsInitCommand_RunUpdatesChangedSkillContent(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	skillPath := filepath.Join(root, ".agents", "skills", "bruin-semantic-layer")
	require.NoError(t, os.MkdirAll(skillPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(skillPath, "SKILL.md"), []byte(`---
name: bruin-semantic-layer
description: old
version: 0.1.0
---

old marker
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(skillPath, "stale.md"), []byte("stale"), 0o644))

	result, err := (SkillsInitCommand{}).Run(root)
	require.NoError(t, err)

	semanticSkill := findSkillResult(t, result, "bruin-semantic-layer")
	assert.Equal(t, skillStatusUpdated, semanticSkill.Status)

	content := readRequiredFile(t, filepath.Join(skillPath, "SKILL.md"))
	assert.Contains(t, content, "name: bruin-semantic-layer")
	assert.NotContains(t, content, "old marker")
	_, err = os.Stat(filepath.Join(skillPath, "stale.md"))
	assert.True(t, os.IsNotExist(err))
}

func TestSkillsInitCommand_RunKeepsCurrentSkillContent(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	firstResult, err := (SkillsInitCommand{}).Run(root)
	require.NoError(t, err)
	require.Len(t, firstResult.Skills, 7)
	for _, skill := range firstResult.Skills {
		assert.Equal(t, skillStatusInstalled, skill.Status, skill.Name)
	}

	secondResult, err := (SkillsInitCommand{}).Run(root)
	require.NoError(t, err)
	require.Len(t, secondResult.Skills, 7)
	for _, skill := range secondResult.Skills {
		assert.Equal(t, skillStatusCurrent, skill.Status, skill.Name)
	}
}

func TestSkillsInitCommand_RunUpdatesChangedSkillEvenWithNewerVersion(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	skillPath := filepath.Join(root, ".agents", "skills", "bruin-semantic-layer")
	require.NoError(t, os.MkdirAll(skillPath, 0o755))
	newerSkill := `---
name: bruin-semantic-layer
description: custom newer skill
version: 9.0.0
---

newer marker
`
	require.NoError(t, os.WriteFile(filepath.Join(skillPath, "SKILL.md"), []byte(newerSkill), 0o644))

	result, err := (SkillsInitCommand{}).Run(root)
	require.NoError(t, err)

	semanticSkill := findSkillResult(t, result, "bruin-semantic-layer")
	assert.Equal(t, skillStatusUpdated, semanticSkill.Status)
	assert.NotEqual(t, newerSkill, readRequiredFile(t, filepath.Join(skillPath, "SKILL.md")))
}

func TestSkillsInitCommand_RunWithOptionsSelfHealAddsConnections(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	result, err := (SkillsInitCommand{}).RunWithOptions(t.Context(), root, SkillsInstallOptions{
		SkillNames: []string{"pipeline-diagnose"},
		ConnectionResolver: func(connection aiConnectionType) (bool, error) {
			return true, nil
		},
	})
	require.NoError(t, err)

	require.Len(t, result.Skills, 1)
	pipelineSkill := findSkillResult(t, result, "pipeline-diagnose")
	assert.Equal(t, "pipeline-diagnose", pipelineSkill.Name)
	assert.FileExists(t, filepath.Join(root, ".agents", "skills", "pipeline-diagnose", "SKILL.md"))
	assert.ElementsMatch(t, []string{"bruin", "github"}, result.ConnectionsAdded)

	cm := readTestBruinConfig(t, filepath.Join(root, ".bruin.yml"))
	env := cm.Environments["default"]
	require.Len(t, env.Connections.BruinCloud, 1)
	assert.Equal(t, "bruin-cloud", env.Connections.BruinCloud[0].Name)
	assert.Equal(t, "${BRUIN_CLOUD_API_TOKEN}", env.Connections.BruinCloud[0].APIToken)
	require.Len(t, env.Connections.GitHub, 1)
	assert.Equal(t, "github", env.Connections.GitHub[0].Name)
	assert.Equal(t, "${GITHUB_TOKEN}", env.Connections.GitHub[0].AccessToken)
	assert.Equal(t, "<github-owner>", env.Connections.GitHub[0].Owner)
	assert.Equal(t, "<github-repo>", env.Connections.GitHub[0].Repo)
}

func TestSkillsInitCommand_RunWithOptionsInstallsAgentsConfig(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	result, err := (SkillsInitCommand{}).RunWithOptions(t.Context(), root, SkillsInstallOptions{
		InstallAgentsConfig: true,
	})
	require.NoError(t, err)

	assert.Empty(t, result.Skills)
	assert.Equal(t, filepath.Join(root, "AGENTS.md"), result.AgentsConfigPath)
	assert.Equal(t, skillStatusInstalled, result.AgentsConfigStatus)

	content := readRequiredFile(t, filepath.Join(root, "AGENTS.md"))
	assert.Contains(t, content, aiAgentsSectionStart)
	assert.Contains(t, content, "Bruin Pipeline Agent Guidance")
	require.NoDirExists(t, filepath.Join(root, ".agents"))
}

func TestSkillsInitCommand_RunWithOptionsUpdatesAgentsConfigSection(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	agentsPath := filepath.Join(root, "AGENTS.md")
	require.NoError(t, os.WriteFile(agentsPath, []byte(`# Existing

keep this

<!-- BEGIN BRUIN AI AGENTS -->
old guidance
<!-- END BRUIN AI AGENTS -->
`), 0o644))

	result, err := (SkillsInitCommand{}).RunWithOptions(t.Context(), root, SkillsInstallOptions{
		InstallAgentsConfig: true,
	})
	require.NoError(t, err)

	assert.Equal(t, agentsPath, result.AgentsConfigPath)
	assert.Equal(t, skillStatusUpdated, result.AgentsConfigStatus)

	content := readRequiredFile(t, agentsPath)
	assert.Contains(t, content, "# Existing")
	assert.Contains(t, content, "keep this")
	assert.Contains(t, content, "Bruin Pipeline Agent Guidance")
	assert.NotContains(t, content, "old guidance")
	assert.Equal(t, 1, strings.Count(content, aiAgentsSectionStart))
	assert.Equal(t, 1, strings.Count(content, aiAgentsSectionEnd))
}

func TestSkillsInitCommand_RunWithOptionsAllInstallsSkillsAndAgentsConfig(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	result, err := (SkillsInitCommand{}).RunWithOptions(t.Context(), root, SkillsInstallOptions{
		InstallAll:          true,
		InstallAgentsConfig: true,
	})
	require.NoError(t, err)

	require.Len(t, result.Skills, 7)
	assert.Equal(t, filepath.Join(root, "AGENTS.md"), result.AgentsConfigPath)
	assert.Equal(t, skillStatusInstalled, result.AgentsConfigStatus)
	assert.FileExists(t, filepath.Join(root, ".agents", "skills", "bruin-semantic-layer", "SKILL.md"))
}

func readRequiredFile(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(content)
}

func findBundledSkill(t *testing.T, skills []bundledSkill, name string) bundledSkill {
	t.Helper()

	for _, skill := range skills {
		if skill.Name == name {
			return skill
		}
	}
	t.Fatalf("bundled skill %s not found", name)
	return bundledSkill{}
}

func findSkillResult(t *testing.T, result *SkillsInitResult, name string) SkillInstallResult {
	t.Helper()

	for _, skill := range result.Skills {
		if skill.Name == name {
			return skill
		}
	}
	t.Fatalf("skill result %s not found", name)
	return SkillInstallResult{}
}
