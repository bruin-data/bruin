package cmd

import (
	"context"
	"errors"
	"fmt"
	fs2 "io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/bruin-data/bruin/templates"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/manifoldco/promptui"
	"github.com/urfave/cli/v3"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

const (
	aiCategoryTemplateName = "ai"
	aiAgentsTemplateName   = "ai-agents-md"
	aiSelfHealTemplateName = "ai-skill-self-heal"

	aiAgentsSectionStart = "<!-- BEGIN BRUIN AI AGENTS -->"
	aiAgentsSectionEnd   = "<!-- END BRUIN AI AGENTS -->"
)

type aiTemplate struct {
	Name                string
	TemplateDir         string
	RequiredConnections []aiConnectionType
}

type aiConnectionType string

const (
	aiConnectionBruinCloud aiConnectionType = "bruin"
	aiConnectionGitHub     aiConnectionType = "github"
)

type aiConflictAction string

const (
	aiConflictOverwrite aiConflictAction = "overwrite"
	aiConflictAdd       aiConflictAction = "add"
	aiConflictSkip      aiConflictAction = "skip"
)

type aiInstallOptions struct {
	conflictResolver   func(path string) (aiConflictAction, error)
	connectionResolver func(connection aiConnectionType) (bool, error)
}

var aiTemplates = []aiTemplate{
	{
		Name:        aiAgentsTemplateName,
		TemplateDir: "agents-md",
	},
	{
		Name:                aiSelfHealTemplateName,
		TemplateDir:         "skill-self-heal",
		RequiredConnections: []aiConnectionType{aiConnectionBruinCloud, aiConnectionGitHub},
	},
}

const (
	aiBruinCloudTokenPlaceholder = "${BRUIN_CLOUD_API_TOKEN}" //nolint:gosec // Environment variable reference, not a credential.
	aiGitHubTokenPlaceholder     = "${GITHUB_TOKEN}"          //nolint:gosec // Environment variable reference, not a credential.
)

var aiGitHubPathPartPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

func isAITemplateArgument(templateName string) bool {
	return templateName == aiCategoryTemplateName || lookupAITemplate(templateName) != nil
}

func handleAIInit(ctx context.Context, c *cli.Command, selectedViaInteractive bool) error {
	templateName := c.Args().Get(0)
	if c.Args().Get(1) != "" {
		errorPrinter.Printf("AI templates install into the repository root and do not accept a folder argument.\n")
		return cli.Exit("", 1)
	}
	if c.IsSet("in-place") {
		errorPrinter.Printf("AI templates always install into the repository root or current directory; --in-place is not supported.\n")
		return cli.Exit("", 1)
	}

	if templateName == aiCategoryTemplateName {
		selected, cancelled, err := runAISelector()
		if err != nil {
			errorPrinter.Printf("Error running the AI template selector: %v\n", err)
			return cli.Exit("", 1)
		}
		if cancelled {
			return nil
		}
		templateName = selected
		selectedViaInteractive = true
	}

	template := lookupAITemplate(templateName)
	if template == nil {
		errorPrinter.Printf("AI template '%s' not found\n", templateName)
		return cli.Exit("", 1)
	}

	targetRoot, err := aiTargetRoot()
	if err != nil {
		errorPrinter.Printf("Could not find target directory: %v\n", err)
		return cli.Exit("", 1)
	}

	result, err := installAITemplate(ctx, *template, targetRoot, aiInstallOptions{})
	if err != nil {
		errorPrinter.Printf("%v\n", err)
		return cli.Exit("", 1)
	}

	telemetry.SetTemplateName(template.Name)
	telemetry.SendEvent("template_selected", map[string]interface{}{
		"template_name":       template.Name,
		"template_category":   aiCategoryTemplateName,
		"interactive":         selectedViaInteractive,
		"created_files":       len(result.Created),
		"updated_files":       len(result.Updated),
		"skipped_conflicts":   len(result.Skipped),
		"connections_added":   len(result.ConnectionsAdded),
		"connections_skipped": len(result.ConnectionsSkipped),
	})

	successPrinter.Printf("\n\nAI template '%s' installed in '%s'.\n", template.Name, targetRoot)
	if len(result.ConnectionsAdded) > 0 {
		infoPrinter.Printf("Added placeholder connections to .bruin.yml: %s\n", strings.Join(result.ConnectionsAdded, ", "))
	}
	if len(result.ConnectionsSkipped) > 0 {
		infoPrinter.Printf("Skipped optional connection setup: %s\n", strings.Join(result.ConnectionsSkipped, ", "))
	}
	if len(result.Skipped) > 0 {
		infoPrinter.Printf("Skipped existing paths: %s\n", strings.Join(result.Skipped, ", "))
	}
	infoPrinter.Print("\nYou can review the installed files and customize them for this repository.\n\n")

	return nil
}

type aiInstallResult struct {
	Created            []string
	Updated            []string
	Skipped            []string
	ConnectionsAdded   []string
	ConnectionsSkipped []string
}

func installAITemplate(ctx context.Context, template aiTemplate, targetRoot string, opts aiInstallOptions) (*aiInstallResult, error) {
	result := &aiInstallResult{}
	sourceRoot := filepath.ToSlash(filepath.Join(aiCategoryTemplateName, template.TemplateDir))

	if err := fs2.WalkDir(templates.Templates, sourceRoot, func(path string, d fs2.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == sourceRoot || d.IsDir() {
			return nil
		}

		content, err := templates.Templates.ReadFile(path)
		if err != nil {
			return err
		}

		relPath := strings.TrimPrefix(path, sourceRoot+"/")
		targetPath := filepath.Join(targetRoot, filepath.FromSlash(relPath))

		outcome, err := writeAIFile(targetPath, content, opts)
		if err != nil {
			return err
		}
		switch outcome {
		case aiWriteCreated:
			result.Created = append(result.Created, targetPath)
		case aiWriteUpdated:
			result.Updated = append(result.Updated, targetPath)
		case aiWriteSkipped:
			result.Skipped = append(result.Skipped, targetPath)
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("could not install AI template %s: %w", template.Name, err)
	}

	if err := ensureAIConnections(ctx, targetRoot, template.RequiredConnections, opts, result); err != nil {
		return nil, err
	}

	return result, nil
}

type aiWriteOutcome string

const (
	aiWriteCreated aiWriteOutcome = "created"
	aiWriteUpdated aiWriteOutcome = "updated"
	aiWriteSkipped aiWriteOutcome = "skipped"
)

func writeAIFile(targetPath string, content []byte, opts aiInstallOptions) (aiWriteOutcome, error) {
	if _, err := os.Stat(targetPath); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
			return "", err
		}
		return aiWriteCreated, os.WriteFile(targetPath, content, 0o644) //nolint:gosec
	} else if err != nil {
		return "", err
	}

	action, err := resolveAIConflict(targetPath, opts)
	if err != nil {
		return "", err
	}

	switch action {
	case aiConflictOverwrite:
		return aiWriteUpdated, os.WriteFile(targetPath, content, 0o644) //nolint:gosec
	case aiConflictAdd:
		if filepath.Base(targetPath) == "AGENTS.md" {
			return aiWriteUpdated, addOrUpdateAgentsSection(targetPath, string(content))
		}
		return aiWriteSkipped, nil
	case aiConflictSkip:
		return aiWriteSkipped, nil
	default:
		return "", fmt.Errorf("unknown conflict action %q", action)
	}
}

func resolveAIConflict(path string, opts aiInstallOptions) (aiConflictAction, error) {
	if opts.conflictResolver != nil {
		return opts.conflictResolver(path)
	}
	if !isStdinTerminal() {
		return "", fmt.Errorf("AI template target already exists: %s. Re-run in an interactive terminal and choose overwrite, add, or skip", path)
	}

	prompt := promptui.Select{
		Label: path + " already exists. What would you like to do?",
		Items: []string{string(aiConflictAdd), string(aiConflictOverwrite), string(aiConflictSkip)},
	}
	_, selected, err := prompt.Run()
	if err != nil {
		return "", err
	}

	return aiConflictAction(selected), nil
}

func addOrUpdateAgentsSection(path, templateContent string) error {
	section, err := extractAgentsSection(templateContent)
	if err != nil {
		return err
	}

	existingBytes, err := os.ReadFile(path) //nolint:gosec
	if err != nil {
		return err
	}
	existing := string(existingBytes)

	start := strings.Index(existing, aiAgentsSectionStart)
	end := strings.Index(existing, aiAgentsSectionEnd)
	if start >= 0 && end >= start {
		end += len(aiAgentsSectionEnd)
		updated := existing[:start] + strings.TrimSpace(section) + existing[end:]
		return os.WriteFile(path, []byte(ensureTrailingNewline(updated)), 0o644) //nolint:gosec
	}

	updated := strings.TrimRight(existing, "\n") + "\n\n" + strings.TrimSpace(section) + "\n"
	return os.WriteFile(path, []byte(updated), 0o644) //nolint:gosec
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

func ensureAIConnections(ctx context.Context, targetRoot string, required []aiConnectionType, opts aiInstallOptions, result *aiInstallResult) error {
	if len(required) == 0 {
		return nil
	}

	configPath := filepath.Join(targetRoot, ".bruin.yml")
	cm, err := loadAIConfig(configPath)
	if err != nil {
		return err
	}

	for _, connection := range required {
		if aiConfigHasConnection(cm, connection) {
			continue
		}

		shouldAdd, err := resolveAIConnectionPrompt(connection, opts)
		if err != nil {
			return err
		}
		if !shouldAdd {
			result.ConnectionsSkipped = append(result.ConnectionsSkipped, string(connection))
			continue
		}

		if err := addAIConnection(ctx, cm, connection, targetRoot); err != nil {
			return err
		}
		result.ConnectionsAdded = append(result.ConnectionsAdded, string(connection))
	}

	if len(result.ConnectionsAdded) == 0 {
		return nil
	}

	configBytes, err := yaml.Marshal(cm)
	if err != nil {
		return fmt.Errorf("could not marshal .bruin.yml: %w", err)
	}
	if err := os.WriteFile(configPath, configBytes, 0o644); err != nil { //nolint:gosec
		return fmt.Errorf("could not write .bruin.yml: %w", err)
	}

	return nil
}

func loadAIConfig(configPath string) (*config.Config, error) {
	content, err := os.ReadFile(configPath) //nolint:gosec
	if errors.Is(err, os.ErrNotExist) {
		defaultEnv := config.Environment{Connections: &config.Connections{}}
		return &config.Config{
			DefaultEnvironmentName: "default",
			Environments: map[string]config.Environment{
				"default": defaultEnv,
			},
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("could not read .bruin.yml: %w", err)
	}

	var cm config.Config
	if err := yaml.Unmarshal(content, &cm); err != nil {
		return nil, fmt.Errorf("could not parse .bruin.yml: %w", err)
	}
	if cm.DefaultEnvironmentName == "" {
		cm.DefaultEnvironmentName = "default"
	}
	if cm.Environments == nil {
		cm.Environments = map[string]config.Environment{}
	}
	if _, ok := cm.Environments[cm.DefaultEnvironmentName]; !ok {
		cm.Environments[cm.DefaultEnvironmentName] = config.Environment{Connections: &config.Connections{}}
	}

	return &cm, nil
}

func aiConfigHasConnection(cm *config.Config, connection aiConnectionType) bool {
	for _, env := range cm.Environments {
		if env.Connections == nil {
			continue
		}
		switch connection {
		case aiConnectionBruinCloud:
			if len(env.Connections.BruinCloud) > 0 {
				return true
			}
		case aiConnectionGitHub:
			if len(env.Connections.GitHub) > 0 {
				return true
			}
		}
	}

	return false
}

func resolveAIConnectionPrompt(connection aiConnectionType, opts aiInstallOptions) (bool, error) {
	if opts.connectionResolver != nil {
		return opts.connectionResolver(connection)
	}
	if !isStdinTerminal() {
		return false, nil
	}

	prompt := promptui.Prompt{
		Label:     fmt.Sprintf("No %s connection found in .bruin.yml. Add a placeholder connection?", connection),
		IsConfirm: true,
	}
	_, err := prompt.Run()
	if err != nil {
		if errors.Is(err, promptui.ErrAbort) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func addAIConnection(ctx context.Context, cm *config.Config, connection aiConnectionType, targetRoot string) error {
	envName := cm.DefaultEnvironmentName
	if envName == "" {
		envName = "default"
		cm.DefaultEnvironmentName = envName
	}

	env := cm.Environments[envName]
	if env.Connections == nil {
		env.Connections = &config.Connections{}
	}

	switch connection {
	case aiConnectionBruinCloud:
		env.Connections.BruinCloud = append(env.Connections.BruinCloud, config.BruinCloudConnection{
			ConnectionMetadata: config.ConnectionMetadata{Name: "bruin-cloud"},
			APIToken:           aiBruinCloudTokenPlaceholder,
		})
	case aiConnectionGitHub:
		owner, repo := inferGitHubOwnerRepo(ctx, targetRoot)
		env.Connections.GitHub = append(env.Connections.GitHub, config.GitHubConnection{
			ConnectionMetadata: config.ConnectionMetadata{Name: "github"},
			AccessToken:        aiGitHubTokenPlaceholder,
			Owner:              owner,
			Repo:               repo,
		})
	default:
		return fmt.Errorf("unsupported AI connection type %q", connection)
	}

	cm.Environments[envName] = env
	return nil
}

func inferGitHubOwnerRepo(ctx context.Context, targetRoot string) (string, string) {
	cmd := exec.CommandContext(ctx, "git", "config", "--get", "remote.origin.url")
	cmd.Dir = targetRoot
	output, err := cmd.Output()
	if err != nil {
		return "<github-owner>", "<github-repo>"
	}

	owner, repo := parseGitHubRemote(strings.TrimSpace(string(output)))
	if owner == "" || repo == "" {
		return "<github-owner>", "<github-repo>"
	}

	return owner, repo
}

func parseGitHubRemote(remote string) (string, string) {
	remote = strings.TrimSuffix(remote, ".git")
	if strings.HasPrefix(remote, "git@") {
		parts := strings.SplitN(remote, ":", 2)
		if len(parts) != 2 {
			return "", ""
		}
		return ownerRepoFromPath(parts[1])
	}

	parsed, err := url.Parse(remote)
	if err == nil && parsed.Path != "" {
		return ownerRepoFromPath(parsed.Path)
	}

	return ownerRepoFromPath(remote)
}

func ownerRepoFromPath(path string) (string, string) {
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return "", ""
	}

	owner := parts[len(parts)-2]
	repo := parts[len(parts)-1]
	if !isGitHubPathPart(owner) || !isGitHubPathPart(repo) {
		return "", ""
	}

	return owner, repo
}

func isGitHubPathPart(value string) bool {
	return aiGitHubPathPartPattern.MatchString(value)
}

func isStdinTerminal() bool {
	return term.IsTerminal(int(os.Stdin.Fd())) //nolint:gosec // os.File.Fd returns the process file descriptor expected by term.IsTerminal.
}

func aiTargetRoot() (string, error) {
	repoRoot, err := git.FindRepoFromPath(".")
	if err == nil {
		return repoRoot.Path, nil
	}

	return os.Getwd()
}

func lookupAITemplate(name string) *aiTemplate {
	for i := range aiTemplates {
		if aiTemplates[i].Name == name {
			return &aiTemplates[i]
		}
	}

	return nil
}

func runAISelector() (string, bool, error) {
	originalChoices := choices
	choices = make([]string, 0, len(aiTemplates))
	for _, template := range aiTemplates {
		choices = append(choices, template.Name)
	}
	defer func() {
		choices = originalChoices
	}()

	p := tea.NewProgram(model{height: getTerminalHeight()})
	m, err := p.Run()
	if err != nil {
		return "", false, err
	}

	if m, ok := m.(model); ok {
		if m.quitting {
			return "", true, nil
		}
		return m.choice, false, nil
	}

	return "", false, errors.New("AI template selector returned an unexpected model")
}
