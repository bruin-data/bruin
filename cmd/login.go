package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/cloudauth"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	bruinpath "github.com/bruin-data/bruin/pkg/path"
	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

type loginDependencies struct {
	globalStore func() (cloudauth.Store, error)
	authorize   func(context.Context, string, bool, io.Writer) (*cloudauth.Credential, error)
	interactive func() bool
}

func CloudLogin() *cli.Command {
	return loginCommand(loginDependencies{
		globalStore: cloudauth.DefaultStore,
		authorize: func(ctx context.Context, target string, noBrowser bool, writer io.Writer) (*cloudauth.Credential, error) {
			flow := cloudauth.OAuth{APIURL: cloudauth.APIURL(), OpenBrowser: openBrowser, Writer: writer, NoBrowser: noBrowser}
			return flow.Login(ctx, target)
		},
		interactive: isStdinTerminal,
	})
}

func loginCommand(deps loginDependencies) *cli.Command {
	return &cli.Command{
		Name:  "login",
		Usage: "Sign in to Bruin Cloud",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "repo", Usage: "Save a bruin connection in this Bruin project repository"},
			&cli.BoolFlag{Name: cloudAuthGlobal, Usage: "Save the token in the OS credential store"},
			&cli.BoolFlag{Name: "no-browser", Usage: "Print the authorization URL without opening a browser"},
			&cli.BoolFlag{Name: "reauth", Usage: "Explicitly replace the selected login after browser approval"},
			&cli.StringFlag{Name: "connection", Usage: "Bruin connection to create or update (repo only)"},
			&cli.StringFlag{Name: "environment", Usage: "Environment containing the bruin connection (repo only)"},
		},
		Action: func(ctx context.Context, c *cli.Command) error { return runOAuthLogin(ctx, c, deps) },
	}
}

type loginPrompt struct {
	reader      io.Reader
	writer      io.Writer
	interactive bool
}

func (p loginPrompt) choose(ctx context.Context, message string, options []string) (int, error) {
	if !p.interactive {
		return 0, errors.New("login needs an interactive terminal; specify --repo or --global, and --connection/--environment if needed; use --reauth to replace an existing login")
	}
	program := tea.NewProgram(newLoginMenu(message, options), tea.WithInput(p.reader), tea.WithOutput(p.writer), tea.WithContext(ctx), tea.WithoutSignalHandler())
	result, err := program.Run()
	if err != nil {
		return 0, fmt.Errorf("login cancelled: %w", err)
	}
	selected := result.(loginMenu)
	if !selected.accepted {
		return 0, errors.New("login cancelled")
	}
	return selected.list.Index(), nil
}

const loginMenuRows = 10

type loginMenuDelegate struct{}

func (d loginMenuDelegate) Height() int                             { return 1 }
func (d loginMenuDelegate) Spacing() int                            { return 0 }
func (d loginMenuDelegate) Update(_ tea.Msg, _ *list.Model) tea.Cmd { return nil }

func (d loginMenuDelegate) Render(w io.Writer, m list.Model, index int, item list.Item) {
	option, ok := item.(listItem)
	if !ok {
		return
	}
	cursor := " "
	if index == m.Index() {
		cursor = ">"
	}
	fmt.Fprintf(w, "  %s %s", cursor, option.title)
}

type loginMenu struct {
	list      list.Model
	accepted  bool
	cancelled bool
}

func newLoginMenu(message string, options []string) loginMenu {
	items := make([]list.Item, len(options))
	optionWidth := 0
	for i, option := range options {
		items[i] = listItem{title: option}
		optionWidth = max(optionWidth, lipgloss.Width(option))
	}
	rows := min(len(options), loginMenuRows)
	paginated := len(options) > rows
	height := rows + 2
	if paginated {
		height = rows + 4
	}
	menu := list.New(items, loginMenuDelegate{}, max(optionWidth+6, lipgloss.Width(message)+4), height)
	menu.Title = message
	menu.Styles.Title = lipgloss.NewStyle()
	menu.Styles.ArabicPagination = lipgloss.NewStyle()
	menu.Paginator.ActiveDot = "*"
	menu.Paginator.InactiveDot = "."
	menu.SetShowStatusBar(false)
	menu.SetShowHelp(false)
	menu.SetFilteringEnabled(false)
	menu.SetShowPagination(paginated)
	return loginMenu{list: menu}
}

func (m loginMenu) Init() tea.Cmd {
	return nil
}

func (m loginMenu) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		if msg.Width > 0 && msg.Width < m.list.Width() {
			m.list.SetWidth(msg.Width)
		}
	case tea.KeyMsg:
		switch msg.String() {
		case keyCtrlC, "ctrl+d", "esc", "q":
			m.cancelled = true
			return m, tea.Quit
		case keyEnter:
			m.accepted = true
			return m, tea.Quit
		}
	}
	var command tea.Cmd
	m.list, command = m.list.Update(msg)
	return m, command
}

func (m loginMenu) View() string {
	if m.cancelled {
		return ""
	}
	if m.accepted {
		return "✓ " + m.list.SelectedItem().(listItem).title + "\n"
	}
	return m.list.View() + "\n  ↑/↓: navigate • enter: select • esc: cancel\n"
}

func runOAuthLogin(ctx context.Context, c *cli.Command, deps loginDependencies) error {
	if c.Bool("repo") && c.Bool(cloudAuthGlobal) {
		return errors.New("--repo and --global cannot be used together")
	}
	if c.Args().Present() {
		return errors.New("unexpected argument; use 'bruin cloud login --help'")
	}
	input := c.Root().Reader
	if input == nil {
		input = os.Stdin
	}
	output := c.Root().Writer
	if output == nil {
		output = os.Stdout
	}
	prompt := loginPrompt{reader: input, writer: output, interactive: deps.interactive()}
	var path string
	if !c.Bool(cloudAuthGlobal) {
		var err error
		path, err = repoLoginConfigPath()
		if err != nil {
			return err
		}
	}
	target := "repo"
	switch {
	case c.Bool(cloudAuthGlobal):
		target = cloudAuthGlobal
	case c.Bool("repo"):
		if path == "" {
			return errors.New("--repo requires a Bruin project in a Git repository (.bruin.yml or pipeline.yml/pipeline.yaml); use --global")
		}
	default:
		if !prompt.interactive {
			return errors.New("specify --repo or --global in a non-interactive terminal")
		}
		if path == "" {
			target = cloudAuthGlobal
			_, _ = fmt.Fprintln(output, "No Bruin project found; using global login.")
		} else {
			choice, err := prompt.choose(ctx, "Where should Bruin save this login?", []string{"This repository", "Global"})
			if err != nil {
				return err
			}
			if choice == 1 {
				target = cloudAuthGlobal
			}
		}
	}
	if target == cloudAuthGlobal && (c.String("connection") != "" || c.String("environment") != "") {
		return errors.New("--connection and --environment require repo login")
	}
	var store cloudauth.Store
	var expected []byte
	var selected config.CloudConnectionRef
	var existing bool
	if target == "repo" {
		if os.Getenv("BRUIN_CONFIG_FILE_CONTENT") != "" {
			return errors.New("BRUIN_CONFIG_FILE_CONTENT is set; unset it before saving a repository login")
		}
		if err := checkRepoLoginFile(ctx, path); err != nil {
			return err
		}
		var err error
		expected, err = os.ReadFile(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		cm := emptyCloudConfig()
		if len(expected) > 0 {
			cm, err = config.LoadFromFileOrEnv(afero.NewOsFs(), path)
			if err != nil {
				return errors.New("could not load repository configuration; fix .bruin.yml before login")
			}
		}
		selected, existing, err = chooseLoginConnection(ctx, c, cm, prompt)
		if err != nil {
			return err
		}
	} else {
		var err error
		store, err = deps.globalStore()
		if err != nil {
			return err
		}
		credential, raw, err := store.Load()
		if err != nil {
			return err
		}
		expected = raw
		existing = credential != nil
	}
	if existing && !c.Bool("reauth") {
		label := "A global Bruin Cloud login already exists."
		if target == "repo" {
			label = fmt.Sprintf("Bruin connection %s/%s already exists.", selected.Environment, selected.Connection.Name)
		}
		choice, err := prompt.choose(ctx, label, []string{"Use existing login", "Sign in again", "Cancel"})
		if err != nil {
			return err
		}
		if choice == 2 {
			_, _ = fmt.Fprintln(output, "Login cancelled; existing credentials were preserved.")
			return nil
		}
		if choice == 0 {
			if target == "repo" {
				if selected.Connection.APIToken == "" {
					return errors.New("selected connection has no API token; sign in again")
				}
				if err := cloudauth.CheckDestination(selected.Connection.APIURL, cloudauth.APIURL()); err != nil {
					return err
				}
			} else if _, err := store.Read(cloudauth.APIURL()); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(output, "Existing login kept. No browser authorization was started.")
			warnLoginOverride(output, target)
			return nil
		}
	}
	if target == cloudAuthGlobal {
		if _, err := store.Open(); err != nil {
			return err
		}
	}
	credential, err := deps.authorize(ctx, target, c.Bool("no-browser"), output)
	if err != nil {
		return err
	}
	if target == "repo" {
		if err = checkRepoLoginFile(ctx, path); err == nil {
			selected.Connection.APIToken = credential.Token
			selected.Connection.APIURL = credential.APIURL
			err = config.SaveCloudConnection(afero.NewOsFs(), path, expected, selected, credential.DefaultTeam)
		}
	} else {
		err = store.Save(*credential, expected)
	}
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if cleanupErr := cloudauth.Revoke(cleanupCtx, credential); cleanupErr != nil {
			_, _ = fmt.Fprintf(output, "Could not revoke the unsaved token %s. Remove it in Cloud → Personal Access Tokens.\n", credential.TokenID)
		}
		return fmt.Errorf("could not save login; previous credentials were preserved: %w", err)
	}
	destination := path
	if target == cloudAuthGlobal {
		destination = store.Path + " (token in OS credential store)"
	}
	_, _ = fmt.Fprintf(output, "Signed in as %s. Saved to %s.\n", credential.Account, destination)
	if !credential.ExpiresAt.IsZero() {
		_, _ = fmt.Fprintf(output, "Expires: %s\n", credential.ExpiresAt.Format(time.RFC3339))
	}
	_, _ = fmt.Fprintf(output, "Abilities: %s\n", strings.Join(credential.Abilities, ", "))
	warnLoginOverride(output, target)
	return nil
}

func repoLoginConfigPath() (string, error) {
	path, err := cloudConfigFilePath()
	if errors.Is(err, git.ErrNoGitRepoFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if _, err := os.Lstat(path); err == nil {
		return path, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	pipelines, err := bruinpath.GetPipelinePaths(filepath.Dir(path), PipelineDefinitionFiles)
	if err != nil {
		return "", fmt.Errorf("could not find Bruin pipelines: %w", err)
	}
	if len(pipelines) == 0 {
		return "", nil
	}
	return path, nil
}

func chooseLoginConnection(ctx context.Context, c *cli.Command, cm *config.Config, prompt loginPrompt) (config.CloudConnectionRef, bool, error) {
	refs := cm.CloudConnections()
	seen := make(map[string]bool)
	for _, ref := range refs {
		key := ref.Environment + "\x00" + ref.Connection.Name
		if seen[key] {
			return config.CloudConnectionRef{}, false, errors.New("duplicate Bruin Cloud connection names; fix .bruin.yml before login")
		}
		seen[key] = true
	}
	var matching []config.CloudConnectionRef
	for _, ref := range refs {
		if c.String("environment") != "" && ref.Environment != c.String("environment") {
			continue
		}
		if c.String("connection") != "" && ref.Connection.Name != c.String("connection") {
			continue
		}
		matching = append(matching, ref)
	}
	if len(matching) > 1 {
		labels := make([]string, len(matching))
		for i, ref := range matching {
			labels[i] = ref.Environment + "/" + ref.Connection.Name
		}
		choice, err := prompt.choose(ctx, "Select a Bruin connection:", labels)
		if err != nil {
			return config.CloudConnectionRef{}, false, err
		}
		return matching[choice], true, nil
	}
	if len(matching) == 1 {
		return matching[0], true, nil
	}
	environment := c.String("environment")
	if environment == "" {
		environment = cm.DefaultEnvironmentName
	}
	if environment == "" {
		environment = defaultCloudEnvironment
	}
	name := c.String("connection")
	if name == "" {
		name = "cloud"
	}
	if env, ok := cm.Environments[environment]; ok && env.Connections != nil && env.Connections.Exists(name) {
		return config.CloudConnectionRef{}, false, fmt.Errorf("connection %q already exists with another type; choose --connection", name)
	}
	return config.CloudConnectionRef{Environment: environment, Connection: config.BruinCloudConnection{ConnectionMetadata: config.ConnectionMetadata{Name: name}}}, false, nil
}

func checkRepoLoginFile(ctx context.Context, path string) error {
	info, err := os.Lstat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil && !info.Mode().IsRegular() {
		return errors.New("refusing to write a login to a non-regular .bruin.yml file")
	}
	command := exec.CommandContext(ctx, "git", "ls-files", "--error-unmatch", "--", ".bruin.yml")
	command.Dir = filepath.Dir(path)
	err = command.Run()
	if err == nil {
		return errors.New(".bruin.yml is tracked by Git; use --global or stop tracking the file before saving a token")
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		return errors.New("could not check whether .bruin.yml is tracked by Git")
	}
	return nil
}

func warnLoginOverride(output io.Writer, target string) {
	if os.Getenv("BRUIN_CLOUD_API_KEY") != "" {
		_, _ = fmt.Fprintln(output, "BRUIN_CLOUD_API_KEY is set and takes precedence over this login.")
		return
	}
	if target == cloudAuthGlobal {
		if cm, err := loadCloudConfig(); err == nil && len(cm.CloudConnections()) > 0 {
			_, _ = fmt.Fprintln(output, "This repository has a bruin connection and takes precedence over the global login.")
		}
	}
}
