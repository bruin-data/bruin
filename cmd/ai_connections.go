package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/manifoldco/promptui"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

type aiConnectionType string

const (
	aiConnectionBruinCloud aiConnectionType = "bruin"
	aiConnectionGitHub     aiConnectionType = "github"
)

type aiConnectionOptions struct {
	connectionResolver func(connection aiConnectionType) (bool, error)
}

type aiConnectionResult struct {
	Added   []string
	Skipped []string
}

const (
	aiBruinCloudTokenPlaceholder = "${BRUIN_CLOUD_API_TOKEN}" //nolint:gosec // Environment variable reference, not a credential.
	aiGitHubTokenPlaceholder     = "${GITHUB_TOKEN}"          //nolint:gosec // Environment variable reference, not a credential.
)

var aiGitHubPathPartPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

func ensureAIConnections(ctx context.Context, targetRoot string, required []aiConnectionType, opts aiConnectionOptions) (aiConnectionResult, error) {
	result := aiConnectionResult{}
	if len(required) == 0 {
		return result, nil
	}

	configPath := filepath.Join(targetRoot, ".bruin.yml")
	cm, err := loadAIConfig(configPath)
	if err != nil {
		return result, err
	}

	for _, connection := range required {
		if aiConfigHasConnection(cm, connection) {
			continue
		}

		shouldAdd, err := resolveAIConnectionPrompt(connection, opts)
		if err != nil {
			return result, err
		}
		if !shouldAdd {
			result.Skipped = append(result.Skipped, string(connection))
			continue
		}

		if err := addAIConnection(ctx, cm, connection, targetRoot); err != nil {
			return result, err
		}
		result.Added = append(result.Added, string(connection))
	}

	if len(result.Added) == 0 {
		return result, nil
	}

	configBytes, err := yaml.Marshal(cm)
	if err != nil {
		return result, fmt.Errorf("could not marshal .bruin.yml: %w", err)
	}
	if err := os.WriteFile(configPath, configBytes, 0o644); err != nil { //nolint:gosec
		return result, fmt.Errorf("could not write .bruin.yml: %w", err)
	}

	return result, nil
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

func resolveAIConnectionPrompt(connection aiConnectionType, opts aiConnectionOptions) (bool, error) {
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
