package cmd

import (
	"context"
	"fmt"
	fs2 "io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/bruin-data/bruin/templates"
)

const (
	DefaultTemplate      = "default"
	DefaultFolderName    = "bruin-pipeline"
	templateHeaderHeight = 7
)

var choices = []string{}

type model struct {
	cursor    int
	choice    string
	pageStart int
	height    int
	quitting  bool
}

func getTerminalHeight() int {
	if term.IsTerminal(int(os.Stdout.Fd())) {
		_, h, err := term.GetSize(int(os.Stdout.Fd()))
		if err == nil {
			return h
		}
	}
	return 24 // fallback default
}

func (m model) Init() tea.Cmd {
	// Set initial terminal height
	return tea.EnterAltScreen
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.height = msg.Height
		visibleCount := m.height - templateHeaderHeight
		if visibleCount >= len(choices) { // nolint:gocritic
			m.pageStart = 0
		} else if m.cursor < m.pageStart {
			m.pageStart = m.cursor
		} else if m.cursor >= m.pageStart+visibleCount {
			m.pageStart = m.cursor - visibleCount + 1
		}
		return m, nil
	case tea.KeyMsg:
		visibleCount := m.height - templateHeaderHeight
		if visibleCount < 1 {
			visibleCount = 1
		}
		if visibleCount >= len(choices) {
			m.pageStart = 0
		}
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			m.quitting = true
			return m, tea.Quit
		case "enter":
			m.choice = choices[m.cursor]
			return m, tea.Quit
		case "down", "j":
			m.cursor++
			if m.cursor >= len(choices) {
				m.cursor = 0
				m.pageStart = 0
			} else if m.cursor >= m.pageStart+visibleCount {
				m.pageStart++
			}
		case "up", "k":
			m.cursor--
			if m.cursor < 0 {
				m.cursor = len(choices) - 1
				m.pageStart = len(choices) - visibleCount
				if m.pageStart < 0 {
					m.pageStart = 0
				}
			} else if m.cursor < m.pageStart {
				m.pageStart--
				if m.pageStart < 0 {
					m.pageStart = 0
				}
			}
		}
	}
	return m, nil
}

func (m model) View() string {
	s := strings.Builder{}
	s.WriteString("Please select a template below:\n\n")
	visibleCount := m.height - templateHeaderHeight
	maxStart := len(choices) - visibleCount
	if maxStart < 0 {
		maxStart = 0
	}
	if m.pageStart > maxStart {
		m.pageStart = maxStart
	}
	end := m.pageStart + visibleCount
	if end > len(choices) {
		end = len(choices)
	}

	for i := m.pageStart; i < end; i++ {
		if m.cursor == i {
			s.WriteString(" [x] ")
		} else {
			s.WriteString(" [ ] ")
		}
		s.WriteString(choices[i])
		s.WriteString("\n")
	}

	if visibleCount == 1 {
		s.WriteString(fmt.Sprintf(
			"\ndisplaying options %d of %d\n",
			end, len(choices),
		))
	}

	if visibleCount > 1 {
		s.WriteString(fmt.Sprintf(
			"\ndisplaying options %d-%d of %d\n",
			m.pageStart+1, end, len(choices),
		))
	}

	s.WriteString("\n(press q to quit)\n")
	return s.String()
}

func mergeTemplateConfig(centralConfig *config.Config, templateBruinContent []byte) error {
	var templateConfig config.Config
	if err := yaml.Unmarshal(templateBruinContent, &templateConfig); err != nil {
		return fmt.Errorf("could not parse template's .bruin.yml: %w", err)
	}

	// Initialize environments map if it doesn't exist
	if centralConfig.Environments == nil {
		centralConfig.Environments = make(map[string]config.Environment)
	}

	// Merge environments and their connections from template into central config
	for templateEnvName, templateEnv := range templateConfig.Environments {
		if err := mergeEnvironment(centralConfig, templateEnvName, templateEnv); err != nil {
			return fmt.Errorf("failed to merge environment %s: %w", templateEnvName, err)
		}
	}

	return nil
}

func mergeEnvironment(centralConfig *config.Config, templateEnvName string, templateEnv config.Environment) error {
	if _, exists := centralConfig.Environments[templateEnvName]; !exists {
		centralConfig.Environments[templateEnvName] = templateEnv
		return nil
	}

	centralEnvCopy := centralConfig.Environments[templateEnvName]
	if centralEnvCopy.Connections == nil {
		centralEnvCopy.Connections = &config.Connections{}
	}

	// Merge the connections from template into central copy
	if err := centralEnvCopy.Connections.MergeFrom(templateEnv.Connections); err != nil {
		return err
	}

	centralConfig.Environments[templateEnvName] = centralEnvCopy
	return nil
}

func Init() *cli.Command {
	folders, err := templates.Templates.ReadDir(".")
	if err != nil {
		panic("Error retrieving bruin templates")
	}
	templateList := make([]string, 0)
	for _, entry := range folders {
		if entry.IsDir() && entry.Name() != "bootstrap" {
			templateList = append(templateList, entry.Name())
		}
	}

	choices = templateList
	initialHeight := getTerminalHeight()
	p := tea.NewProgram(model{height: initialHeight})
	return &cli.Command{
		Name:  "init",
		Usage: "init a Bruin pipeline",
		ArgsUsage: fmt.Sprintf(
			"[template name to be used: %s] [name of the folder where the pipeline will be created]",
			strings.Join(templateList, "|"),
		),
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "in-place",
				Usage: "initializes the template without creating a bruin repository parent folder",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()

			templateName := c.Args().Get(0)
			if len(templateName) == 0 {
				m, err := p.Run()
				if err != nil {
					fmt.Printf("Error running the select: %v\n", err)
					os.Exit(1)
				}

				if m, ok := m.(model); ok {
					if m.choice != "" {
						templateName = m.choice
					} else if m.quitting {
						return nil
					}
				}
			}

			_, err = templates.Templates.ReadDir(templateName)
			if err != nil {
				errorPrinter.Printf("Template '%s' not found\n", templateName)
				return cli.Exit("", 1)
			}

			inputPath := c.Args().Get(1)
			if inputPath == "" {
				if templateName == DefaultTemplate {
					inputPath = DefaultFolderName
				} else {
					inputPath = templateName
				}
			}

			dir, _ := filepath.Split(inputPath)
			if dir != "" {
				errorPrinter.Printf("Traversing up or down in the folder structure is not allowed, provide base folder name only.\n")
				return cli.Exit("", 1)
			}

			var bruinYmlPath string
			repoRoot, err := git.FindRepoFromPath(".")
			//nolint:nestif
			if err != nil {
				var targetDir string

				if c.IsSet("in-place") {
					// Initialize in given directory
					targetDir, err = os.Getwd()
					if err != nil {
						errorPrinter.Printf("Failed to get current working directory: %v\n", err)
						return cli.Exit("", 1)
					}
				} else {
					// Create a bruin root directory
					if err := os.MkdirAll("bruin", 0o755); err != nil {
						errorPrinter.Printf("Failed to create the bruin root folder: %v\n", err)
						return cli.Exit("", 1)
					}
					targetDir = "bruin"
				}

				// Initialize git repository in the target directory
				cmd := exec.Command("git", "init")
				cmd.Dir = targetDir
				out, err := cmd.CombinedOutput()
				if err != nil {
					errorPrinter.Printf("Could not initialize git repository in %s: %s\n", targetDir, string(out))
					return cli.Exit("", 1)
				}

				if c.IsSet("in-place") {
					// When using --in-place, use current directory for .bruin.yml and inputPath.
					bruinYmlPath = filepath.Join(targetDir, ".bruin.yml")
					inputPath = filepath.Join(targetDir, inputPath)
				} else {
					// When not using --in-place, use bruin subdirectory.
					bruinYmlPath = filepath.Join("bruin", ".bruin.yml")
					inputPath = filepath.Join("bruin", inputPath)
				}
			} else {
				bruinYmlPath = filepath.Join(repoRoot.Path, ".bruin.yml")
			}

			centralConfig, err := config.LoadOrCreateWithoutPathAbsolutization(afero.NewOsFs(), bruinYmlPath)
			if err != nil {
				errorPrinter.Printf("Could not write .bruin.yml file: %v\n", err)
				return err
			}

			// Read template's .bruin.yml if it exists
			templateBruinPath := templateName + "/.bruin.yml"
			templateBruinContent, err := templates.Templates.ReadFile(templateBruinPath)
			if err == nil { // Only process if file exists
				if err := mergeTemplateConfig(centralConfig, templateBruinContent); err != nil {
					errorPrinter.Printf("%v\n", err)
					return err
				}

				// Write back the updated config
				configBytes, err := yaml.Marshal(centralConfig)
				if err != nil {
					errorPrinter.Printf("Could not marshal .bruin.yml: %v\n", err)
					return err
				}

				if err := os.WriteFile(bruinYmlPath, configBytes, 0o644); err != nil { //nolint:gosec
					errorPrinter.Printf("Could not write .bruin.yml file: %v\n", err)
					return err
				}
			}

			err = fs2.WalkDir(templates.Templates, templateName, func(path string, d fs2.DirEntry, err error) error {
				if err != nil {
					return err
				}

				// Walk returns the root as if it was its own content
				if path == templateName {
					return nil
				}

				// Walk returns the root as if it was its own content
				if d.IsDir() {
					return nil
				}

				fileContents, err := templates.Templates.ReadFile(path)
				if err != nil {
					return err
				}

				relativePath, baseName := filepath.Split(path)
				relativePath = strings.TrimPrefix(relativePath, templateName)
				absolutePath := inputPath + relativePath

				// Skip .bruin.yml as we've already handled it
				if baseName == ".bruin.yml" {
					return nil
				}

				// ignore the error
				err = os.MkdirAll(absolutePath, os.ModePerm)
				if err != nil {
					errorPrinter.Printf("Could not create the %s folder: %v\n", absolutePath, err)
					return err
				}

				err = os.WriteFile(filepath.Join(absolutePath, baseName), fileContents, 0o644) //nolint:gosec
				if err != nil {
					errorPrinter.Printf("Could not write the %s file: %v\n", filepath.Join(absolutePath, baseName), err)
					return err
				}

				return nil
			})
			if err != nil {
				errorPrinter.Printf("Could not copy template %s: %s\n", templateName, err)
				return cli.Exit("", 1)
			}

			successPrinter.Printf("\n\nA new '%s' pipeline created successfully in folder '%s'.\n", templateName, inputPath)
			infoPrinter.Println("\nYou can run the following commands to get started:")
			infoPrinter.Printf("    bruin validate %s\n\n", inputPath)

			return nil
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}
