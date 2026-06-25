package cmd

import (
	"context"
	"fmt"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/git"
	bundledskills "github.com/bruin-data/bruin/skills"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v3"
	"gopkg.in/yaml.v3"
)

const (
	skillHomeAgents = ".agents"
	skillHomeClaude = ".claude"
	skillsDirName   = "skills"

	skillStatusInstalled = "installed"
	skillStatusUpdated   = "updated"
	skillStatusCurrent   = "current"
)

type bundledSkill struct {
	Name  string
	Files []bundledSkillFile
}

type bundledSkillFile struct {
	Path    string
	Content string
}

type skillFrontmatter struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}

type skillInstallPlan struct {
	PrimaryHome      string
	PrimarySkillsDir string
	LinkSkillsDirs   []string
}

type SkillsInitCommand struct{}

type SkillsInitResult struct {
	Root             string
	PrimarySkillsDir string
	Skills           []SkillInstallResult
}

type SkillInstallResult struct {
	Name   string
	Path   string
	Status string
	Links  []string
}

func loadBundledBruinSkills() ([]bundledSkill, error) {
	entries, err := bundledskills.Files.ReadDir(".")
	if err != nil {
		return nil, errors.Wrap(err, "failed to read bundled skills")
	}

	skills := make([]bundledSkill, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		skill, err := loadBundledSkill(entry.Name())
		if err != nil {
			return nil, err
		}
		skills = append(skills, skill)
	}

	if len(skills) == 0 {
		return nil, errors.New("no bundled skills found")
	}

	return skills, nil
}

func loadBundledSkill(dir string) (bundledSkill, error) {
	skillFile := dir + "/SKILL.md"
	skillContent, err := bundledskills.Files.ReadFile(skillFile)
	if err != nil {
		return bundledSkill{}, errors.Wrapf(err, "failed to read bundled skill metadata from %s", skillFile)
	}

	metadata, hasMetadata, err := parseSkillFrontmatter(string(skillContent))
	if err != nil {
		return bundledSkill{}, errors.Wrapf(err, "failed to parse bundled skill metadata from %s", skillFile)
	}
	if !hasMetadata {
		return bundledSkill{}, fmt.Errorf("bundled skill %s must start with YAML frontmatter", skillFile)
	}
	if metadata.Name == "" {
		return bundledSkill{}, fmt.Errorf("bundled skill %s is missing frontmatter name", skillFile)
	}
	if metadata.Name != dir {
		return bundledSkill{}, fmt.Errorf("bundled skill directory %q does not match frontmatter name %q", dir, metadata.Name)
	}
	skill := bundledSkill{
		Name: metadata.Name,
	}
	err = iofs.WalkDir(bundledskills.Files, dir, func(path string, d iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		content, err := bundledskills.Files.ReadFile(path)
		if err != nil {
			return err
		}

		relativePath := strings.TrimPrefix(path, dir+"/")
		skill.Files = append(skill.Files, bundledSkillFile{
			Path:    relativePath,
			Content: string(content),
		})
		return nil
	})
	if err != nil {
		return bundledSkill{}, errors.Wrapf(err, "failed to load bundled skill files from %s", dir)
	}

	return skill, nil
}

// Skills returns the parent command for Bruin agent skills.
func Skills() *cli.Command {
	return &cli.Command{
		Name:  "skills",
		Usage: "manage Bruin agent skills",
		Commands: []*cli.Command{
			SkillsInit(),
		},
	}
}

// SkillsInit creates the command that installs bundled Bruin agent skills.
func SkillsInit() *cli.Command {
	return &cli.Command{
		Name:  "init",
		Usage: "install or update Bruin agent skills in .agents or .claude",
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()

			root, err := resolveSkillsRoot()
			if err != nil {
				errorPrinter.Printf("Failed to resolve the project root: %v\n", err)
				return cli.Exit("", 1)
			}

			r := SkillsInitCommand{}
			result, err := r.Run(root)
			if err != nil {
				errorPrinter.Printf("Failed to initialize Bruin skills: %v\n", err)
				return cli.Exit("", 1)
			}

			printSkillsInitResult(result)
			return nil
		},
	}
}

func (r SkillsInitCommand) Run(root string) (*SkillsInitResult, error) {
	if root == "" {
		return nil, errors.New("project root is required")
	}

	plan, err := resolveSkillInstallPlan(root)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(plan.PrimarySkillsDir, 0o755); err != nil {
		return nil, errors.Wrapf(err, "failed to create skills directory %s", plan.PrimarySkillsDir)
	}

	bundledSkills, err := loadBundledBruinSkills()
	if err != nil {
		return nil, err
	}

	result := &SkillsInitResult{
		Root:             root,
		PrimarySkillsDir: plan.PrimarySkillsDir,
		Skills:           make([]SkillInstallResult, 0, len(bundledSkills)),
	}

	for _, skill := range bundledSkills {
		installResult, err := installBundledSkill(plan.PrimarySkillsDir, skill)
		if err != nil {
			return nil, err
		}

		for _, linkSkillsDir := range plan.LinkSkillsDirs {
			linkPath, err := ensureBundledSkillLink(linkSkillsDir, installResult.Path, skill)
			if err != nil {
				return nil, err
			}
			installResult.Links = append(installResult.Links, linkPath)
		}

		result.Skills = append(result.Skills, installResult)
	}

	return result, nil
}

func resolveSkillsRoot() (string, error) {
	repoRoot, err := git.FindRepoFromPath(".")
	if err == nil {
		return repoRoot.Path, nil
	}

	if !errors.Is(err, git.ErrNoGitRepoFound) {
		return "", err
	}

	return os.Getwd()
}

func resolveSkillInstallPlan(root string) (skillInstallPlan, error) {
	agentsHome := filepath.Join(root, skillHomeAgents)
	claudeHome := filepath.Join(root, skillHomeClaude)

	agentsExists, err := agentHomeExists(agentsHome)
	if err != nil {
		return skillInstallPlan{}, err
	}

	claudeExists, err := agentHomeExists(claudeHome)
	if err != nil {
		return skillInstallPlan{}, err
	}

	switch {
	case agentsExists && claudeExists:
		plan := skillInstallPlan{
			PrimaryHome:      agentsHome,
			PrimarySkillsDir: filepath.Join(agentsHome, skillsDirName),
		}

		sameHome, err := sameAgentHome(agentsHome, claudeHome)
		if err != nil {
			return skillInstallPlan{}, err
		}
		if !sameHome {
			plan.LinkSkillsDirs = append(plan.LinkSkillsDirs, filepath.Join(claudeHome, skillsDirName))
		}

		return plan, nil
	case agentsExists:
		return skillInstallPlan{
			PrimaryHome:      agentsHome,
			PrimarySkillsDir: filepath.Join(agentsHome, skillsDirName),
		}, nil
	case claudeExists:
		return skillInstallPlan{
			PrimaryHome:      claudeHome,
			PrimarySkillsDir: filepath.Join(claudeHome, skillsDirName),
		}, nil
	default:
		if err := os.MkdirAll(agentsHome, 0o755); err != nil {
			return skillInstallPlan{}, errors.Wrapf(err, "failed to create agent home %s", agentsHome)
		}
		return skillInstallPlan{
			PrimaryHome:      agentsHome,
			PrimarySkillsDir: filepath.Join(agentsHome, skillsDirName),
		}, nil
	}
}

func agentHomeExists(path string) (bool, error) {
	if _, err := os.Lstat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to inspect %s", path)
	}

	info, err := os.Stat(path)
	if err != nil {
		return false, errors.Wrapf(err, "%s exists but could not be resolved", path)
	}

	if !info.IsDir() {
		return false, fmt.Errorf("%s exists but is not a directory", path)
	}

	return true, nil
}

func sameAgentHome(left, right string) (bool, error) {
	leftInfo, err := os.Stat(left)
	if err != nil {
		return false, errors.Wrapf(err, "failed to inspect %s", left)
	}

	rightInfo, err := os.Stat(right)
	if err != nil {
		return false, errors.Wrapf(err, "failed to inspect %s", right)
	}

	return os.SameFile(leftInfo, rightInfo), nil
}

func installBundledSkill(skillsDir string, skill bundledSkill) (SkillInstallResult, error) {
	skillPath := filepath.Join(skillsDir, skill.Name)
	status, err := bundledSkillStatus(skillPath, skill)
	if err != nil {
		return SkillInstallResult{}, err
	}

	result := SkillInstallResult{
		Name:   skill.Name,
		Path:   skillPath,
		Status: status,
	}

	if status == skillStatusCurrent {
		return result, nil
	}

	if status == skillStatusUpdated {
		if err := os.RemoveAll(skillPath); err != nil {
			return SkillInstallResult{}, errors.Wrapf(err, "failed to remove old skill files from %s", skillPath)
		}
	}

	if err := ensureSkillDirectory(skillPath); err != nil {
		return SkillInstallResult{}, err
	}

	for _, file := range skill.Files {
		target, err := skillFileTarget(skillPath, file.Path)
		if err != nil {
			return SkillInstallResult{}, err
		}

		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return SkillInstallResult{}, errors.Wrapf(err, "failed to create directory for %s", target)
		}

		if err := os.WriteFile(target, []byte(file.Content), 0o644); err != nil { //nolint:gosec
			return SkillInstallResult{}, errors.Wrapf(err, "failed to write %s", target)
		}
	}

	return result, nil
}

func bundledSkillStatus(skillPath string, skill bundledSkill) (string, error) {
	exists, err := pathExists(skillPath)
	if err != nil {
		return "", err
	}
	if !exists {
		return skillStatusInstalled, nil
	}

	metadata, hasMetadata, err := readSkillFrontmatter(filepath.Join(skillPath, "SKILL.md"))
	if err != nil {
		return "", err
	}
	if hasMetadata && metadata.Name != "" && metadata.Name != skill.Name {
		return "", fmt.Errorf("refusing to overwrite %s: existing skill name is %q", skillPath, metadata.Name)
	}

	matches, err := skillContentMatches(skillPath, skill)
	if err != nil {
		return "", err
	}
	if matches {
		return skillStatusCurrent, nil
	}

	return skillStatusUpdated, nil
}

func skillContentMatches(skillPath string, skill bundledSkill) (bool, error) {
	expected := make(map[string]string, len(skill.Files))
	for _, file := range skill.Files {
		target, err := skillFileTarget(skillPath, file.Path)
		if err != nil {
			return false, err
		}

		relativePath, err := filepath.Rel(skillPath, target)
		if err != nil {
			return false, err
		}

		expectedPath := filepath.ToSlash(relativePath)
		expected[expectedPath] = file.Content

		content, err := os.ReadFile(target)
		if err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
			return false, errors.Wrapf(err, "failed to read installed skill file %s", target)
		}
		if string(content) != file.Content {
			return false, nil
		}
	}

	installedFileCount, err := countSkillFiles(skillPath)
	if err != nil {
		return false, err
	}

	return installedFileCount == len(expected), nil
}

func countSkillFiles(skillPath string) (int, error) {
	count := 0
	err := filepath.WalkDir(skillPath, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			count++
		}
		return nil
	})
	if err != nil {
		return 0, errors.Wrapf(err, "failed to inspect installed skill files from %s", skillPath)
	}

	return count, nil
}

func pathExists(path string) (bool, error) {
	if _, err := os.Lstat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to inspect %s", path)
	}
	return true, nil
}

func ensureSkillDirectory(skillPath string) error {
	info, err := os.Stat(skillPath)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(skillPath, 0o755)
		}
		return errors.Wrapf(err, "failed to inspect %s", skillPath)
	}

	if !info.IsDir() {
		return fmt.Errorf("%s exists but is not a directory", skillPath)
	}

	return nil
}

func skillFileTarget(skillPath, relativePath string) (string, error) {
	cleanPath := filepath.Clean(filepath.FromSlash(relativePath))
	if cleanPath == "." || filepath.IsAbs(cleanPath) || cleanPath == ".." || strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid bundled skill file path %q", relativePath)
	}

	return filepath.Join(skillPath, cleanPath), nil
}

func ensureBundledSkillLink(linkSkillsDir, primarySkillPath string, skill bundledSkill) (string, error) {
	if err := os.MkdirAll(linkSkillsDir, 0o755); err != nil {
		return "", errors.Wrapf(err, "failed to create skills directory %s", linkSkillsDir)
	}

	linkPath := filepath.Join(linkSkillsDir, skill.Name)
	relativeTarget, err := filepath.Rel(linkSkillsDir, primarySkillPath)
	if err != nil {
		relativeTarget = primarySkillPath
	}

	if exists, err := pathExists(linkPath); err != nil {
		return "", err
	} else if exists {
		alreadyLinked, err := prepareExistingSkillLink(linkPath, primarySkillPath, skill)
		if err != nil {
			return "", err
		}
		if alreadyLinked {
			return linkPath, nil
		}
	}

	if err := os.Symlink(relativeTarget, linkPath); err != nil {
		return "", errors.Wrapf(err, "failed to create symlink %s", linkPath)
	}

	return linkPath, nil
}

func prepareExistingSkillLink(linkPath, primarySkillPath string, skill bundledSkill) (bool, error) {
	info, err := os.Lstat(linkPath)
	if err != nil {
		return false, errors.Wrapf(err, "failed to inspect %s", linkPath)
	}

	if info.Mode()&os.ModeSymlink != 0 {
		pointsToPrimary, err := symlinkPointsTo(linkPath, primarySkillPath)
		if err != nil {
			return false, err
		}
		if pointsToPrimary {
			return true, nil
		}
		return false, os.Remove(linkPath)
	}

	metadata, hasMetadata, err := readSkillFrontmatter(filepath.Join(linkPath, "SKILL.md"))
	if err != nil {
		return false, err
	}
	if hasMetadata && metadata.Name != "" && metadata.Name != skill.Name {
		return false, fmt.Errorf("refusing to replace %s: existing skill name is %q", linkPath, metadata.Name)
	}

	return false, os.RemoveAll(linkPath)
}

func symlinkPointsTo(linkPath, targetPath string) (bool, error) {
	currentTarget, err := os.Readlink(linkPath)
	if err != nil {
		return false, errors.Wrapf(err, "failed to read symlink %s", linkPath)
	}

	if !filepath.IsAbs(currentTarget) {
		currentTarget = filepath.Join(filepath.Dir(linkPath), currentTarget)
	}

	currentTarget, err = filepath.Abs(currentTarget)
	if err != nil {
		return false, err
	}

	targetPath, err = filepath.Abs(targetPath)
	if err != nil {
		return false, err
	}

	return filepath.Clean(currentTarget) == filepath.Clean(targetPath), nil
}

func readSkillFrontmatter(skillFile string) (skillFrontmatter, bool, error) {
	data, err := os.ReadFile(skillFile)
	if err != nil {
		if os.IsNotExist(err) {
			return skillFrontmatter{}, false, nil
		}
		return skillFrontmatter{}, false, errors.Wrapf(err, "failed to read %s", skillFile)
	}

	metadata, ok, err := parseSkillFrontmatter(string(data))
	if err != nil {
		return skillFrontmatter{}, false, errors.Wrapf(err, "failed to parse %s", skillFile)
	}

	return metadata, ok, nil
}

func parseSkillFrontmatter(content string) (skillFrontmatter, bool, error) {
	lines := strings.Split(content, "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "---" {
		return skillFrontmatter{}, false, nil
	}

	for i := 1; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "---" {
			var metadata skillFrontmatter
			if err := yaml.Unmarshal([]byte(strings.Join(lines[1:i], "\n")), &metadata); err != nil {
				return skillFrontmatter{}, false, err
			}
			return metadata, true, nil
		}
	}

	return skillFrontmatter{}, false, nil
}

func printSkillsInitResult(result *SkillsInitResult) {
	successPrinter.Printf("Bruin skills initialized in %s\n", result.PrimarySkillsDir)
	for _, skill := range result.Skills {
		switch skill.Status {
		case skillStatusInstalled:
			infoPrinter.Printf("- installed %s\n", skill.Name)
		case skillStatusUpdated:
			infoPrinter.Printf("- updated %s\n", skill.Name)
		default:
			infoPrinter.Printf("- %s already current\n", skill.Name)
		}

		for _, link := range skill.Links {
			infoPrinter.Printf("  linked %s\n", link)
		}
	}
}
