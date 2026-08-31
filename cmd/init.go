package cmd

import (
	"context"
	"fmt"
	fs2 "io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/bruin-data/bruin/templates"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
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

// getTerminalHeight returns the current terminal height or a default value of 24 if unable to determine.
func getTerminalHeight() int {
	if term.IsTerminal(int(os.Stdout.Fd())) { //nolint:gosec // Fd() returns uintptr, safe to convert to int for terminal check
		_, h, err := term.GetSize(int(os.Stdout.Fd())) //nolint:gosec // Fd() returns uintptr, safe to convert to int for terminal size
		if err == nil {
			return h
		}
	}
	return 24 // fallback default
}

// Init initializes the bubble tea model and enters alternate screen mode.
func (m model) Init() tea.Cmd {
	// Set initial terminal height
	return tea.EnterAltScreen
}

// Update handles keyboard input and window size changes for the template selection UI.
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
		case keyCtrlC, "q", "esc":
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

// View renders the template selection UI with pagination support.
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
		fmt.Fprintf(
			&s,
			"\ndisplaying options %d of %d\n",
			end, len(choices),
		)
	}

	if visibleCount > 1 {
		fmt.Fprintf(
			&s,
			"\ndisplaying options %d-%d of %d\n",
			m.pageStart+1, end, len(choices),
		)
	}

	s.WriteString("\n(press q to quit)\n")
	return s.String()
}

// gitignoreEscaper escapes the characters git treats as pattern syntax, so a
// database path is matched literally.
var gitignoreEscaper = strings.NewReplacer(`\`, `\\`, "*", `\*`, "?", `\?`, "[", `\[`, "]", `\]`)

// gitignorePatternForPath turns a filesystem path into a gitignore pattern anchored
// to the directory holding the .gitignore, so it cannot match a same-named file
// elsewhere in the repository.
func gitignorePatternForPath(path string) string {
	return "/" + strings.TrimPrefix(gitignoreEscaper.Replace(filepath.ToSlash(path)), "/")
}

// ensureLocalDuckDBFilesAreIgnored gitignores every relative DuckDB path in the
// config, and its write-ahead log. DuckDB resolves a relative path against the
// config file, so the database lands next to .bruin.yml rather than inside the
// pipeline folder.
func ensureLocalDuckDBFilesAreIgnored(fs afero.Fs, bruinYmlPath string, cfg *config.Config) error {
	gitignoreDir := filepath.Dir(bruinYmlPath)

	seen := make(map[string]bool)
	for _, env := range cfg.Environments {
		if env.Connections == nil {
			continue
		}

		for _, conn := range env.Connections.DuckDB {
			// IsLocal rather than !IsAbs: it also rejects a Windows rooted path and
			// anything escaping the directory with "..", neither of which this
			// repository's .gitignore has any business covering.
			path := strings.TrimSpace(conn.Path)
			if path == "" || !filepath.IsLocal(path) || seen[path] {
				continue
			}
			seen[path] = true

			for _, pattern := range []string{gitignorePatternForPath(path), gitignorePatternForPath(path + ".wal")} {
				if err := git.EnsureGivenPatternIsInGitignore(fs, gitignoreDir, pattern); err != nil {
					return fmt.Errorf("failed to add %s: %w", pattern, err)
				}
			}
		}
	}

	return nil
}

// mergeTemplateConfig merges environments and connections from a template's .bruin.yml into the central config.
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

// mergeEnvironment merges a template environment and its connections into the central config's environment.
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

// Init creates and returns the CLI command for initializing new Bruin pipelines from templates.
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
	for _, t := range templateList {
		if t != "bootstrap" {
			choices = append(choices, t)
		}
	}

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
			selectedViaInteractive := false
			if len(templateName) == 0 {
				if len(choices) == 0 {
					errorPrinter.Printf("No templates available\n")
					return cli.Exit("", 1)
				}
				m, err := p.Run()
				if err != nil {
					fmt.Printf("Error running the select: %v\n", err)
					os.Exit(1)
				}

				if m, ok := m.(model); ok {
					if m.choice != "" {
						templateName = m.choice
						selectedViaInteractive = true
					} else if m.quitting {
						return nil
					}
				}

				// If still empty after interactive selection, something went wrong
				if templateName == "" {
					errorPrinter.Printf("No template selected\n")
					return cli.Exit("", 1)
				}
			}

			_, err = templates.Templates.ReadDir(templateName)
			if err != nil || !slices.Contains(templateList, templateName) {
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
			var configLocationNote string
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
				cmd := exec.CommandContext(ctx, "git", "init")
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
					configLocationNote = "This is your Bruin project root."
				} else {
					// When not using --in-place, use bruin subdirectory.
					bruinYmlPath = filepath.Join("bruin", ".bruin.yml")
					inputPath = filepath.Join("bruin", inputPath)
					configLocationNote = "This is the new Bruin project root created for you."
				}
			} else {
				bruinYmlPath = filepath.Join(repoRoot.Path, ".bruin.yml")
				configLocationNote = "This is your Git repo root, so it may sit several levels above the pipeline folder."
			}

			// existed must be sampled before anything writes to bruinYmlPath.
			configSummary := initConfigSummary{
				path:         bruinYmlPath,
				existed:      fileExists(bruinYmlPath),
				locationNote: configLocationNote,
			}

			// Handle ecommerce template with interactive stack picker
			if templateName == "ecommerce" {
				ecommerceChoices, err := runEcommerceStackPicker()
				if err != nil {
					errorPrinter.Printf("Error during stack selection: %v\n", err)
					return cli.Exit("", 1)
				}
				if ecommerceChoices == nil {
					return nil // user cancelled
				}

				// Load or create .bruin.yml, then merge ecommerce connections into it
				centralConfig, err := config.LoadOrCreateWithoutPathAbsolutization(afero.NewOsFs(), bruinYmlPath)
				if err != nil {
					errorPrinter.Printf("Could not load .bruin.yml file: %v\n", err)
					return cli.Exit("", 1)
				}

				bruinContent := generateBruinYML(ecommerceChoices)
				if err := mergeTemplateConfig(centralConfig, []byte(bruinContent)); err != nil {
					errorPrinter.Printf("Could not merge ecommerce config: %v\n", err)
					return cli.Exit("", 1)
				}

				configBytes, err := yaml.Marshal(centralConfig)
				if err != nil {
					errorPrinter.Printf("Could not marshal .bruin.yml: %v\n", err)
					return cli.Exit("", 1)
				}

				if err := os.WriteFile(bruinYmlPath, configBytes, 0o644); err != nil { //nolint:gosec
					errorPrinter.Printf("Could not write .bruin.yml file: %v\n", err)
					return cli.Exit("", 1)
				}
				configSummary.merged = true

				// Generate all pipeline files
				if err := generateEcommerceTemplate(inputPath, ecommerceChoices); err != nil {
					errorPrinter.Printf("Could not generate ecommerce template: %v\n", err)
					return cli.Exit("", 1)
				}

				telemetry.SetTemplateName(templateName)
				telemetry.SendEvent("template_selected", map[string]interface{}{
					"template_name": templateName,
					"interactive":   true,
					"warehouse":     ecommerceChoices.Warehouse,
					"payments":      ecommerceChoices.Payments,
					"marketing":     ecommerceChoices.Marketing,
					"ads":           ecommerceChoices.Ads,
					"analytics":     ecommerceChoices.Analytics,
				})

				successPrinter.Printf("\n\nEcommerce pipeline created successfully in folder '%s'.\n", inputPath)
				printEcommerceSummary(ecommerceChoices)
				printInitSummary(configSummary, inputPath)

				return nil
			}

			// Read template's .bruin.yml if it exists
			templateBruinPath := templateName + "/.bruin.yml"
			templateBruinContent, err := templates.Templates.ReadFile(templateBruinPath)
			if err == nil { // Only process if file exists
				centralConfig, err := config.LoadOrCreateWithoutPathAbsolutization(afero.NewOsFs(), bruinYmlPath)
				if err != nil {
					errorPrinter.Printf("Could not write .bruin.yml file: %v\n", err)
					return err
				}

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
				configSummary.merged = true

				// Cosmetic, so a failure here must not abort an init that has already
				// written the config.
				if err := ensureLocalDuckDBFilesAreIgnored(afero.NewOsFs(), bruinYmlPath, centralConfig); err != nil {
					errorPrinter.Printf("Could not add the DuckDB database to .gitignore: %v\n", err)
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
				err = os.MkdirAll(absolutePath, os.ModePerm) //nolint:gosec // G122: path is constructed from embedded template names, not user input; safe in this CLI context
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

			// Store template name for telemetry (will be sent with command_end event)
			telemetry.SetTemplateName(templateName)

			// Also send an immediate event to track template selection
			telemetry.SendEvent("template_selected", map[string]interface{}{
				"template_name": templateName,
				"interactive":   selectedViaInteractive,
			})

			successPrinter.Printf("\n\nA new '%s' pipeline created successfully in folder '%s'.\n", templateName, inputPath)
			printInitSummary(configSummary, inputPath)

			return nil
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

// fileExists reports whether the given path exists and is not a directory.
func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// absOrSame returns the absolute version of path, falling back to path itself
// if it cannot be resolved.
func absOrSame(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}

	return abs
}

// initConfigSummary captures what happened to .bruin.yml during init.
type initConfigSummary struct {
	path         string
	existed      bool
	merged       bool // false for templates that ship no .bruin.yml of their own
	locationNote string
}

// printInitSummary tells the user where the pipeline and the .bruin.yml config
// ended up, whether the config was created or reused, and what to do next.
// The config location is not obvious: inside an existing Git repo it goes to the
// repo root, which can be several levels above the pipeline folder.
func printInitSummary(cfg initConfigSummary, pipelinePath string) {
	pipelineAbs := absOrSame(pipelinePath)

	if !fileExists(cfg.path) {
		infoPrinter.Printf("\nPipeline: %s\n", pipelineAbs)
		infoPrinter.Println("\nNext steps:")
		infoPrinter.Println("  1. Create a .bruin.yml with your connection credentials")
		printInitRunSteps(pipelinePath)

		return
	}

	configAbs := absOrSame(cfg.path)

	infoPrinter.Printf("\nConfig:   %s\n", configAbs)
	infoPrinter.Printf("Pipeline: %s\n", pipelineAbs)

	switch {
	case cfg.existed && cfg.merged:
		infoPrinter.Printf("\nUsing existing .bruin.yml at %s (merged template config).\n", configAbs)
	case cfg.existed:
		infoPrinter.Printf("\nUsing existing .bruin.yml at %s (left unchanged).\n", configAbs)
	default:
		infoPrinter.Printf("\nCreated .bruin.yml at %s.\n", configAbs)
	}

	if cfg.locationNote != "" {
		infoPrinter.Println(cfg.locationNote)
	}

	infoPrinter.Println("\nNext steps:")
	infoPrinter.Printf("  1. Add your connection credentials to %s\n", configAbs)
	printInitRunSteps(pipelinePath)
}

func printInitRunSteps(pipelinePath string) {
	infoPrinter.Printf("  2. Run: bruin validate %s\n", pipelinePath)
	infoPrinter.Printf("  3. Run: bruin run %s\n\n", pipelinePath)
}
