package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSkillsCommand(t *testing.T) {
	t.Parallel()

	command := Skills()

	assert.Equal(t, "skills", command.Name)
	require.Len(t, command.Commands, 1)
	assert.Equal(t, "init", command.Commands[0].Name)
}

func TestLoadBundledBruinSkillsFromMarkdownFiles(t *testing.T) {
	t.Parallel()

	skills, err := loadBundledBruinSkills()
	require.NoError(t, err)

	var skill bundledSkill
	for _, bundled := range skills {
		if bundled.Name == "bruin-semantic-layer" {
			skill = bundled
			break
		}
	}

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
}

func TestSkillsInitCommand_RunCreatesAgentsHomeByDefault(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	result, err := (SkillsInitCommand{}).Run(root)
	require.NoError(t, err)

	require.Len(t, result.Skills, 1)
	skill := result.Skills[0]
	assert.Equal(t, skillStatusInstalled, skill.Status)
	assert.Equal(t, filepath.Join(root, ".agents", "skills"), result.PrimarySkillsDir)
	assert.Equal(t, filepath.Join(root, ".agents", "skills", "bruin-semantic-layer"), skill.Path)

	content := readRequiredFile(t, filepath.Join(skill.Path, "SKILL.md"))
	assert.Contains(t, content, "name: bruin-semantic-layer")

	_, err = os.Stat(filepath.Join(root, ".claude"))
	assert.True(t, os.IsNotExist(err))
}

func TestSkillsInitCommand_RunUsesClaudeWhenOnlyClaudeExists(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, ".claude"), 0o755))

	result, err := (SkillsInitCommand{}).Run(root)
	require.NoError(t, err)

	require.Len(t, result.Skills, 1)
	assert.Equal(t, filepath.Join(root, ".claude", "skills"), result.PrimarySkillsDir)
	assert.Equal(t, filepath.Join(root, ".claude", "skills", "bruin-semantic-layer"), result.Skills[0].Path)
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

	require.Len(t, result.Skills, 1)
	skill := result.Skills[0]
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

	require.Len(t, result.Skills, 1)
	assert.Equal(t, skillStatusUpdated, result.Skills[0].Status)

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
	require.Len(t, firstResult.Skills, 1)
	assert.Equal(t, skillStatusInstalled, firstResult.Skills[0].Status)

	secondResult, err := (SkillsInitCommand{}).Run(root)
	require.NoError(t, err)
	require.Len(t, secondResult.Skills, 1)
	assert.Equal(t, skillStatusCurrent, secondResult.Skills[0].Status)
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

	require.Len(t, result.Skills, 1)
	assert.Equal(t, skillStatusUpdated, result.Skills[0].Status)
	assert.NotEqual(t, newerSkill, readRequiredFile(t, filepath.Join(skillPath, "SKILL.md")))
}

func readRequiredFile(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(content)
}
