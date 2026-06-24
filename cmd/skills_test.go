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
	assert.Equal(t, metadata.Version, skill.Version)
	_, err = parseSkillVersion(skill.Version)
	require.NoError(t, err)
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
	assert.Contains(t, content, "version: "+semanticLayerSkillVersion(t))

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

func TestSkillsInitCommand_RunUpdatesOlderSkillVersion(t *testing.T) {
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
	assert.Equal(t, "0.1.0", result.Skills[0].PreviousVersion)

	content := readRequiredFile(t, filepath.Join(skillPath, "SKILL.md"))
	assert.Contains(t, content, "version: "+semanticLayerSkillVersion(t))
	assert.NotContains(t, content, "old marker")
	_, err = os.Stat(filepath.Join(skillPath, "stale.md"))
	assert.True(t, os.IsNotExist(err))
}

func TestSkillsInitCommand_RunSkipsNewerSkillVersion(t *testing.T) {
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
	assert.Equal(t, skillStatusNewer, result.Skills[0].Status)
	assert.Equal(t, "9.0.0", result.Skills[0].PreviousVersion)
	assert.Equal(t, newerSkill, readRequiredFile(t, filepath.Join(skillPath, "SKILL.md")))
}

func readRequiredFile(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(content)
}

func semanticLayerSkillVersion(t *testing.T) string {
	t.Helper()

	skills, err := loadBundledBruinSkills()
	require.NoError(t, err)
	for _, skill := range skills {
		if skill.Name == "bruin-semantic-layer" {
			return skill.Version
		}
	}
	t.Fatal("bruin semantic layer skill not found")
	return ""
}
