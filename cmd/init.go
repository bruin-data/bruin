package cmd

import (
	"fmt"
	fs2 "io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/bruin-data/bruin/templates"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

const (
	DefaultTemplate   = "default"
	DefaultFolderName = "bruin-pipeline"
)

var choices = []string{}

type model struct {
	cursor int
	choice string
}

func (m model) Init() tea.Cmd {
	return nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if msg, ok := msg.(tea.KeyMsg); ok {
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return m, tea.Quit
		case "enter":
			// Send the choice on the channel and exit.
			m.choice = choices[m.cursor]
			return m, tea.Quit
		case "down", "j":
			m.cursor++
			if m.cursor >= len(choices) {
				m.cursor = 0
			}
		case "up", "k":
			m.cursor--
			if m.cursor < 0 {
				m.cursor = len(choices) - 1
			}
		}
	}
	// Return the concrete type instead of the interface
	return m, nil // This line is fine as it is
}

func (m model) View() string {
	s := strings.Builder{}
	s.WriteString("Please select a template below\n\n")

	for i, choice := range choices {
		if m.cursor == i {
			s.WriteString(" [x] ")
		} else {
			s.WriteString(" [ ] ")
		}
		s.WriteString(choice)
		s.WriteString("\n")
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
		centralEnvCopy.Connections = templateEnv.Connections
	} else if err := centralEnvCopy.Connections.MergeFrom(templateEnv.Connections); err != nil {
		return fmt.Errorf("could not merge connections: %w", err)
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
		if entry.IsDir() {
			templateList = append(templateList, entry.Name())
		}
	}

	choices = templateList
	p := tea.NewProgram(model{})
	return &cli.Command{
		Name:  "init",
		Usage: "init a Bruin pipeline",
		ArgsUsage: fmt.Sprintf(
			"[template name to be used: %s] [name of the folder where the pipeline will be created]",
			strings.Join(templateList, "|"),
		),
		Flags: []cli.Flag{},
		Action: func(c *cli.Context) error {
			defer func() {
				if err := recover(); err != nil {
					log.Println("=======================================")
					log.Println("Bruin encountered an unexpected error, please report the issue to the Bruin team.")
					log.Println(err)
					log.Println("=======================================")
				}
			}()

			templateName := c.Args().Get(0)
			if len(templateName) == 0 {
				m, err := p.Run()
				if err != nil {
					fmt.Printf("Error running the select: %v\n", err)
					os.Exit(1)
				}

				if m, ok := m.(model); ok && m.choice != "" {
					templateName = m.choice
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

			// Check if current directory is in a git repository
			if _, err := git.FindRepoFromPath("."); err != nil {
				// Not in a git repo, create a bruin root directory
				if err := os.MkdirAll("bruin", 0o755); err != nil {
					errorPrinter.Printf("Failed to create the bruin root folder: %v\n", err)
					return cli.Exit("", 1)
				}

				// Initialize git repository in the bruin directory
				cmd := exec.Command("git", "init")
				cmd.Dir = "bruin"
				out, err := cmd.CombinedOutput()
				if err != nil {
					errorPrinter.Printf("Could not initialize git repository in bruin folder: %s\n", string(out))
					return cli.Exit("", 1)
				}

				// Update inputPath to be within bruin directory
				inputPath = filepath.Join("bruin", inputPath)
			}

			err = os.Mkdir(inputPath, 0o755)
			if err != nil {
				errorPrinter.Printf("Failed to create the folder %s: %v\n", inputPath, err)
				return cli.Exit("", 1)
			}

			var bruinYmlPath string
			if _, err := git.FindRepoFromPath("."); err != nil {
				// Not in a git repo, use bruin directory
				bruinYmlPath = "bruin/.bruin.yml"
			} else {
				// In a git repo, use current directory
				bruinYmlPath = ".bruin.yml"
			}

			centralConfig, err := config.LoadOrCreate(afero.NewOsFs(), bruinYmlPath)
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

				if err := os.WriteFile(bruinYmlPath, configBytes, 0o644); err != nil {
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
			infoPrinter.Printf("\n    cd %s\n", inputPath)
			infoPrinter.Printf("    bruin validate\n\n")

			return nil
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}
