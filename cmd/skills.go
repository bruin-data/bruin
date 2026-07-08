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
	tea "github.com/charmbracelet/bubbletea"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v3"
	"gopkg.in/yaml.v3"
)

const (
	skillHomeAgents = ".agents"
	skillHomeClaude = ".claude"
	skillsDirName   = "skills"

	agentsConfigSelectionName = "AGENTS.md"
	agentsConfigSourcePath    = "agents-md/AGENTS.md"
	aiAgentsSectionStart      = "<!-- BEGIN BRUIN AI AGENTS -->"
	aiAgentsSectionEnd        = "<!-- END BRUIN AI AGENTS -->"

	skillStatusInstalled = "installed"
	skillStatusUpdated   = "updated"
	skillStatusCurrent   = "current"

	skillSelectorHeaderHeight = 8
)

type bundledSkill struct {
	Name                string
	Description         string
	RequiredConnections []aiConnectionType
	Files               []bundledSkillFile
}

type bundledSkillFile struct {
	Path    string
	Content string
}

type skillFrontmatter struct {
	Name                string             `yaml:"name"`
	Description         string             `yaml:"description"`
	RequiredConnections []aiConnectionType `yaml:"connections"`
}

type skillInstallPlan struct {
	PrimaryHome      string
	PrimarySkillsDir string
	LinkSkillsDirs   []string
}

type SkillsInitCommand struct{}

type SkillsInstallOptions struct {
	SkillNames          []string
	InstallAll          bool
	InstallAgentsConfig bool
	ConnectionResolver  func(connection aiConnectionType) (bool, error)
}

type SkillsInitResult struct {
	Root               string
	PrimarySkillsDir   string
	Skills             []SkillInstallResult
	AgentsConfigPath   string
	AgentsConfigStatus string
	ConnectionsAdded   []string
	ConnectionsSkipped []string
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
		if _, err := bundledskills.Files.ReadFile(entry.Name() + "/SKILL.md"); err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				continue
			}
			return nil, errors.Wrapf(err, "failed to inspect bundled skill %s", entry.Name())
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
		Name:                metadata.Name,
		Description:         metadata.Description,
		RequiredConnections: metadata.RequiredConnections,
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

// AISkills creates the command that installs bundled Bruin AI skills.
func AISkills() *cli.Command {
	return &cli.Command{
		Name:      "skills",
		Usage:     "install or update Bruin agent skills in .agents or .claude",
		ArgsUsage: "[skill name|all]",
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()

			if c.Args().Len() > 1 {
				errorPrinter.Printf("Expected at most one skill name.\n")
				return cli.Exit("", 1)
			}

			bundledSkills, err := loadBundledBruinSkills()
			if err != nil {
				errorPrinter.Printf("Failed to load Bruin skills: %v\n", err)
				return cli.Exit("", 1)
			}

			opts := SkillsInstallOptions{}
			selectionArg := c.Args().Get(0)
			if selectionArg == "" {
				selection, cancelled, err := runSkillSelector(bundledSkills)
				if err != nil {
					errorPrinter.Printf("Error running the skill selector: %v\n", err)
					return cli.Exit("", 1)
				}
				if cancelled {
					return nil
				}
				opts.SkillNames = selection.SkillNames
				opts.InstallAgentsConfig = selection.InstallAgentsConfig
			} else {
				switch {
				case selectionArg == "all":
					opts.InstallAll = true
					opts.InstallAgentsConfig = true
				case isAgentsConfigSelection(selectionArg):
					opts.InstallAgentsConfig = true
				default:
					opts.SkillNames = []string{selectionArg}
				}
			}

			root, err := resolveSkillsRoot()
			if err != nil {
				errorPrinter.Printf("Failed to resolve the project root: %v\n", err)
				return cli.Exit("", 1)
			}

			r := SkillsInitCommand{}
			result, err := r.RunWithOptions(ctx, root, opts)
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
	return r.RunWithOptions(context.Background(), root, SkillsInstallOptions{InstallAll: true})
}

func (r SkillsInitCommand) RunWithOptions(ctx context.Context, root string, opts SkillsInstallOptions) (*SkillsInitResult, error) {
	if root == "" {
		return nil, errors.New("project root is required")
	}

	bundledSkills, err := loadBundledBruinSkills()
	if err != nil {
		return nil, err
	}

	selectedSkills, err := selectBundledSkills(bundledSkills, opts)
	if err != nil {
		return nil, err
	}

	result := &SkillsInitResult{
		Root:   root,
		Skills: make([]SkillInstallResult, 0, len(selectedSkills)),
	}

	if len(selectedSkills) > 0 {
		plan, err := resolveSkillInstallPlan(root)
		if err != nil {
			return nil, err
		}
		result.PrimarySkillsDir = plan.PrimarySkillsDir

		if err := os.MkdirAll(plan.PrimarySkillsDir, 0o755); err != nil {
			return nil, errors.Wrapf(err, "failed to create skills directory %s", plan.PrimarySkillsDir)
		}

		for _, skill := range selectedSkills {
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
	}

	if opts.InstallAgentsConfig {
		path, status, err := installAgentsConfig(root)
		if err != nil {
			return nil, err
		}
		result.AgentsConfigPath = path
		result.AgentsConfigStatus = status
	}

	requiredConnections := requiredConnectionsForSkills(selectedSkills)
	connectionResult, err := ensureAIConnections(ctx, root, requiredConnections, aiConnectionOptions{
		connectionResolver: opts.ConnectionResolver,
	})
	if err != nil {
		return nil, err
	}
	result.ConnectionsAdded = connectionResult.Added
	result.ConnectionsSkipped = connectionResult.Skipped

	return result, nil
}

type selectedAIInstall struct {
	SkillNames          []string
	InstallAgentsConfig bool
}

type skillSelectionItem struct {
	Name         string
	Description  string
	SkillName    string
	AgentsConfig bool
}

type skillSelectorModel struct {
	items     []skillSelectionItem
	cursor    int
	selected  map[int]bool
	pageStart int
	height    int
	cancelled bool
	submitted bool
}

func runSkillSelector(skills []bundledSkill) (selectedAIInstall, bool, error) {
	if !isStdinTerminal() {
		return selectedAIInstall{}, false, fmt.Errorf("skill name is required in non-interactive mode; available skills: %s, %s, all", agentsConfigSelectionName, strings.Join(skillSelectionNames(skills), ", "))
	}

	items := skillSelectionItems(skills)
	selected := make(map[int]bool, len(items))
	for i := range items {
		selected[i] = true
	}
	p := tea.NewProgram(skillSelectorModel{
		items:    items,
		selected: selected,
		height:   getTerminalHeight(),
	})
	m, err := p.Run()
	if err != nil {
		return selectedAIInstall{}, false, err
	}

	model, ok := m.(skillSelectorModel)
	if !ok {
		return selectedAIInstall{}, false, errors.New("skill selector returned an unexpected model")
	}
	if model.cancelled {
		return selectedAIInstall{}, true, nil
	}

	return selectedInstallFromModel(model), false, nil
}

func (m skillSelectorModel) Init() tea.Cmd {
	return tea.EnterAltScreen
}

func (m skillSelectorModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.height = msg.Height
		m = m.clampPage()
		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case keyCtrlC, "q", "esc":
			m.cancelled = true
			return m, tea.Quit
		case "enter":
			m.submitted = true
			return m, tea.Quit
		case " ":
			m.selected[m.cursor] = !m.selected[m.cursor]
		case "down", "j":
			m.cursor++
			if m.cursor >= len(m.items) {
				m.cursor = 0
				m.pageStart = 0
			}
		case "up", "k":
			m.cursor--
			if m.cursor < 0 {
				m.cursor = len(m.items) - 1
			}
		}
		m = m.clampPage()
	}
	return m, nil
}

func (m skillSelectorModel) View() string {
	var s strings.Builder
	s.WriteString("Select Bruin AI skills and config:\n\n")

	visibleCount := m.visibleCount()
	end := m.pageStart + visibleCount
	if end > len(m.items) {
		end = len(m.items)
	}

	for i := m.pageStart; i < end; i++ {
		cursor := " "
		if i == m.cursor {
			cursor = ">"
		}
		checkbox := "[ ]"
		if m.selected[i] {
			checkbox = "[x]"
		}

		item := m.items[i]
		fmt.Fprintf(&s, "%s %s %s", cursor, checkbox, item.Name)
		if item.Description != "" {
			fmt.Fprintf(&s, " - %s", item.Description)
		}
		s.WriteString("\n")
	}

	if len(m.items) > visibleCount {
		fmt.Fprintf(&s, "\nshowing options %d-%d of %d\n", m.pageStart+1, end, len(m.items))
	}
	s.WriteString("\n(space to toggle, enter to install, q to quit)\n")
	return s.String()
}

func (m skillSelectorModel) visibleCount() int {
	visibleCount := m.height - skillSelectorHeaderHeight
	if visibleCount < 1 {
		return 1
	}
	if visibleCount > len(m.items) {
		return len(m.items)
	}
	return visibleCount
}

func (m skillSelectorModel) clampPage() skillSelectorModel {
	if len(m.items) == 0 {
		m.cursor = 0
		m.pageStart = 0
		return m
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
	if m.cursor >= len(m.items) {
		m.cursor = len(m.items) - 1
	}

	visibleCount := m.visibleCount()
	if m.cursor < m.pageStart {
		m.pageStart = m.cursor
	}
	if m.cursor >= m.pageStart+visibleCount {
		m.pageStart = m.cursor - visibleCount + 1
	}

	maxStart := len(m.items) - visibleCount
	if maxStart < 0 {
		maxStart = 0
	}
	if m.pageStart > maxStart {
		m.pageStart = maxStart
	}
	return m
}

func skillSelectionItems(skills []bundledSkill) []skillSelectionItem {
	items := make([]skillSelectionItem, 0, 1+len(skills))
	items = append(items, skillSelectionItem{
		Name:         agentsConfigSelectionName,
		Description:  "initialize or update repository agent guidance",
		AgentsConfig: true,
	})
	for _, skill := range skills {
		items = append(items, skillSelectionItem{
			Name:        skill.Name,
			Description: skill.Description,
			SkillName:   skill.Name,
		})
	}
	return items
}

func selectedInstallFromModel(model skillSelectorModel) selectedAIInstall {
	var selected selectedAIInstall
	for i, item := range model.items {
		if !model.selected[i] {
			continue
		}
		if item.AgentsConfig {
			selected.InstallAgentsConfig = true
			continue
		}
		selected.SkillNames = append(selected.SkillNames, item.SkillName)
	}
	return selected
}

func skillSelectionNames(skills []bundledSkill) []string {
	names := make([]string, 0, len(skills))
	for _, skill := range skills {
		names = append(names, skill.Name)
	}
	return names
}

func selectBundledSkills(skills []bundledSkill, opts SkillsInstallOptions) ([]bundledSkill, error) {
	if opts.InstallAll {
		return skills, nil
	}
	if len(opts.SkillNames) == 0 {
		return nil, nil
	}

	byName := make(map[string]bundledSkill, len(skills))
	for _, skill := range skills {
		byName[skill.Name] = skill
	}

	selected := make([]bundledSkill, 0, len(opts.SkillNames))
	for _, name := range opts.SkillNames {
		skill, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("bruin AI skill %q not found. Available skills: %s", name, strings.Join(skillSelectionNames(skills), ", "))
		}
		selected = append(selected, skill)
	}

	return selected, nil
}

func requiredConnectionsForSkills(skills []bundledSkill) []aiConnectionType {
	seen := map[aiConnectionType]bool{}
	required := make([]aiConnectionType, 0)
	for _, skill := range skills {
		for _, connection := range skill.RequiredConnections {
			if seen[connection] {
				continue
			}
			seen[connection] = true
			required = append(required, connection)
		}
	}
	return required
}

func isAgentsConfigSelection(value string) bool {
	return strings.EqualFold(value, agentsConfigSelectionName) || value == "agents-md"
}

func installAgentsConfig(root string) (string, string, error) {
	content, err := bundledskills.Files.ReadFile(agentsConfigSourcePath)
	if err != nil {
		return "", "", errors.Wrap(err, "failed to read bundled AGENTS.md guidance")
	}

	targetPath := filepath.Join(root, agentsConfigSelectionName)
	existing, err := os.ReadFile(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.WriteFile(targetPath, content, 0o644); err != nil { //nolint:gosec
				return "", "", errors.Wrapf(err, "failed to write %s", targetPath)
			}
			return targetPath, skillStatusInstalled, nil
		}
		return "", "", errors.Wrapf(err, "failed to read %s", targetPath)
	}

	updated, err := mergeAgentsConfig(string(existing), string(content))
	if err != nil {
		return "", "", err
	}
	if updated == string(existing) {
		return targetPath, skillStatusCurrent, nil
	}
	if err := os.WriteFile(targetPath, []byte(updated), 0o644); err != nil { //nolint:gosec
		return "", "", errors.Wrapf(err, "failed to write %s", targetPath)
	}
	return targetPath, skillStatusUpdated, nil
}

func mergeAgentsConfig(existing, templateContent string) (string, error) {
	section, err := extractAgentsSection(templateContent)
	if err != nil {
		return "", err
	}

	start := strings.Index(existing, aiAgentsSectionStart)
	end := strings.Index(existing, aiAgentsSectionEnd)
	if start >= 0 && end >= start {
		end += len(aiAgentsSectionEnd)
		updated := existing[:start] + strings.TrimSpace(section) + existing[end:]
		return ensureTrailingNewline(updated), nil
	}

	updated := strings.TrimRight(existing, "\n") + "\n\n" + strings.TrimSpace(section) + "\n"
	return updated, nil
}

func extractAgentsSection(content string) (string, error) {
	start := strings.Index(content, aiAgentsSectionStart)
	end := strings.Index(content, aiAgentsSectionEnd)
	if start < 0 || end < start {
		return "", errors.New("AGENTS.md template does not contain Bruin AI section markers")
	}

	return content[start : end+len(aiAgentsSectionEnd)], nil
}

func ensureTrailingNewline(value string) string {
	if strings.HasSuffix(value, "\n") {
		return value
	}
	return value + "\n"
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
	if result.PrimarySkillsDir == "" && result.AgentsConfigPath == "" {
		infoPrinter.Println("No Bruin AI skills or config selected.")
		return
	}
	if result.PrimarySkillsDir != "" {
		successPrinter.Printf("Bruin skills initialized in %s\n", result.PrimarySkillsDir)
	}
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
	if result.AgentsConfigPath != "" {
		switch result.AgentsConfigStatus {
		case skillStatusInstalled:
			infoPrinter.Printf("- installed %s\n", result.AgentsConfigPath)
		case skillStatusUpdated:
			infoPrinter.Printf("- updated %s\n", result.AgentsConfigPath)
		default:
			infoPrinter.Printf("- %s already current\n", result.AgentsConfigPath)
		}
	}
	if len(result.ConnectionsAdded) > 0 {
		infoPrinter.Printf("Added placeholder connections to .bruin.yml: %s\n", strings.Join(result.ConnectionsAdded, ", "))
	}
	if len(result.ConnectionsSkipped) > 0 {
		infoPrinter.Printf("Skipped optional connection setup: %s\n", strings.Join(result.ConnectionsSkipped, ", "))
	}
}
