package cmd

import (
	"fmt"
	fs2 "io/fs"
	"log"
	"os"
	path2 "path"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/templates"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

const (
	DefaultTemplate   = "default"
	DefaultFolderName = "bruin-pipeline"
)

var choices = templates.TemplateNames()

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
					fmt.Println("Oh no:", err)
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

			// Check if the folder already exists
			if _, err := os.Stat(inputPath); !os.IsNotExist(err) {
				errorPrinter.Printf("The folder %s already exists, please choose a different name\n", inputPath)
				return cli.Exit("", 1)
			}
			dir, _ := filepath.Split(inputPath)
			if dir != "" {
				errorPrinter.Printf("Traversing up or down in the folder structure is not allowed, provide base folder name only.\n")
				return cli.Exit("", 1)
			}

			err = os.Mkdir(inputPath, 0o755)
			if err != nil {
				errorPrinter.Printf("Failed to create the folder %s: %v\n", inputPath, err)
				return cli.Exit("", 1)
			}

			_, err = config.LoadOrCreate(afero.NewOsFs(), path2.Join(inputPath, ".bruin.yml"))
			if err != nil {
				errorPrinter.Printf("Could not write .bruin.yml file: %v\n", err)
				return err
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

			return nil
		},
	}
}
