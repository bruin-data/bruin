package cmd

import (
	"context"
	"errors"
	"fmt"
	fs2 "io/fs"
	"os"
	"os/exec"
	fspath "path"
	"path/filepath"
	"slices"
	"sort"
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

type templateMergeFile struct {
	// path is relative to the template's pipeline root, e.g. "assets/orders.sql"
	// or "macros/aggregations.sql".
	path    string
	content []byte
}

// mergeableTemplateDirs are the pipeline-level directories copied by merge mode.
// Assets routinely call macros, so copying "assets" alone would produce a
// pipeline that fails to render.
var mergeableTemplateDirs = []string{"assets", "macros"}

// mergeableTemplateFiles are the pipeline-root dependency files that Python
// assets resolve by walking up from their own directory.
var mergeableTemplateFiles = []string{"requirements.txt", "pyproject.toml", "uv.lock"}

// loadTemplateMergeFiles returns the mergeable files of every pipeline in a
// template. Pipeline roots are found via their assets directory, because some
// templates wrap or repeat their pipeline below the template root.
func loadTemplateMergeFiles(templateName string) ([]templateMergeFile, error) {
	assetRoots := make([]string, 0)
	err := fs2.WalkDir(templates.Templates, templateName, func(entryPath string, d fs2.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() && d.Name() == "assets" {
			assetRoots = append(assetRoots, entryPath)
			return fs2.SkipDir
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not inspect template %s: %w", templateName, err)
	}
	if len(assetRoots) == 0 {
		return nil, fmt.Errorf("template %q does not contain an assets directory", templateName)
	}

	filesByPath := make(map[string]templateMergeFile)
	for _, assetRoot := range assetRoots {
		pipelineRoot := fspath.Dir(assetRoot)

		for _, dirName := range mergeableTemplateDirs {
			if err := collectTemplateMergeDir(templateName, fspath.Join(pipelineRoot, dirName), dirName, filesByPath); err != nil {
				return nil, err
			}
		}

		for _, fileName := range mergeableTemplateFiles {
			content, err := templates.Templates.ReadFile(fspath.Join(pipelineRoot, fileName))
			if errors.Is(err, fs2.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("could not read %s from template %s: %w", fileName, templateName, err)
			}
			if err := addTemplateMergeFile(templateName, filesByPath, fileName, content); err != nil {
				return nil, err
			}
		}
	}

	return sortedTemplateMergeFiles(filesByPath), nil
}

// collectTemplateMergeDir records every file below root under prefix. A missing
// directory is not an error: most templates have no macros.
func collectTemplateMergeDir(templateName, root, prefix string, filesByPath map[string]templateMergeFile) error {
	if _, err := fs2.Stat(templates.Templates, root); err != nil {
		if errors.Is(err, fs2.ErrNotExist) {
			return nil
		}

		return fmt.Errorf("could not inspect %s in template %s: %w", root, templateName, err)
	}

	err := fs2.WalkDir(templates.Templates, root, func(entryPath string, d fs2.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		content, err := templates.Templates.ReadFile(entryPath)
		if err != nil {
			return err
		}

		relativePath := fspath.Join(prefix, strings.TrimPrefix(entryPath, root+"/"))

		return addTemplateMergeFile(templateName, filesByPath, relativePath, content)
	})
	if err != nil {
		return fmt.Errorf("could not read %s from template %s: %w", prefix, templateName, err)
	}

	return nil
}

func addTemplateMergeFile(templateName string, filesByPath map[string]templateMergeFile, relativePath string, content []byte) error {
	if !fs2.ValidPath(relativePath) || relativePath == "." {
		return fmt.Errorf("template %q contains an invalid file path %q", templateName, relativePath)
	}
	if _, exists := filesByPath[relativePath]; exists {
		return fmt.Errorf("template %q contains multiple files at %q", templateName, relativePath)
	}

	filesByPath[relativePath] = templateMergeFile{path: relativePath, content: content}

	return nil
}

func sortedTemplateMergeFiles(filesByPath map[string]templateMergeFile) []templateMergeFile {
	relativePaths := make([]string, 0, len(filesByPath))
	for relativePath := range filesByPath {
		relativePaths = append(relativePaths, relativePath)
	}
	sort.Strings(relativePaths)

	files := make([]templateMergeFile, 0, len(relativePaths))
	for _, relativePath := range relativePaths {
		files = append(files, filesByPath[relativePath])
	}

	return files
}

func loadEcommerceMergeFiles(choices *EcommerceChoices) ([]templateMergeFile, error) {
	generated, err := buildEcommerceFiles(choices)
	if err != nil {
		return nil, err
	}

	filesByPath := make(map[string]templateMergeFile)
	for relativePath, content := range generated {
		relativePath = filepath.ToSlash(relativePath)
		if !isMergeableTemplatePath(relativePath) {
			continue
		}
		if err := addTemplateMergeFile("ecommerce", filesByPath, relativePath, []byte(content)); err != nil {
			return nil, err
		}
	}

	return sortedTemplateMergeFiles(filesByPath), nil
}

func isMergeableTemplatePath(relativePath string) bool {
	for _, dirName := range mergeableTemplateDirs {
		if strings.HasPrefix(relativePath, dirName+"/") {
			return true
		}
	}

	return slices.Contains(mergeableTemplateFiles, relativePath)
}

func loadTemplateBruinConfig(templateName string) ([]byte, error) {
	content, err := templates.Templates.ReadFile(templateName + "/.bruin.yml")
	if errors.Is(err, fs2.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("could not read template config: %w", err)
	}

	return content, nil
}

// writeFileAtomically replaces path in one step, so a failed write cannot leave
// a truncated file behind. An existing file keeps its own mode: .bruin.yml holds
// credentials and may have been tightened to 0600 by hand.
func writeFileAtomically(path string, content []byte) error {
	mode := os.FileMode(0o644)
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode().Perm()
	} else if !os.IsNotExist(err) {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := tmp.Write(content); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	return os.Rename(tmpName, path)
}

// mergeTemplateConfigIntoProject resolves the project from the destination
// pipeline, then merges the template config into the project-level .bruin.yml.
func mergeTemplateConfigIntoProject(pipelinePath string, templateBruinContent []byte) (initConfigSummary, error) {
	repoRoot, err := git.FindRepoFromPath(pipelinePath)
	if err != nil {
		return initConfigSummary{}, fmt.Errorf("could not locate the Git project containing pipeline %q: %w", pipelinePath, err)
	}

	bruinYmlPath := filepath.Join(repoRoot.Path, ".bruin.yml")
	summary := initConfigSummary{
		path:         bruinYmlPath,
		existed:      fileExists(bruinYmlPath),
		merged:       true,
		locationNote: "This is your Git repo root, so it may sit several levels above the pipeline folder.",
	}

	centralConfig, err := config.LoadOrCreateWithoutPathAbsolutization(afero.NewOsFs(), bruinYmlPath)
	if err != nil {
		return initConfigSummary{}, fmt.Errorf("could not load .bruin.yml file: %w", err)
	}
	if err := mergeTemplateConfig(centralConfig, templateBruinContent); err != nil {
		return initConfigSummary{}, err
	}

	configBytes, err := yaml.Marshal(centralConfig)
	if err != nil {
		return initConfigSummary{}, fmt.Errorf("could not marshal .bruin.yml: %w", err)
	}
	if err := writeFileAtomically(bruinYmlPath, configBytes); err != nil {
		return initConfigSummary{}, fmt.Errorf("could not write .bruin.yml file: %w", err)
	}

	// Cosmetic, so a failure here must not abort a merge that has already
	// written the config.
	if err := ensureLocalDuckDBFilesAreIgnored(afero.NewOsFs(), bruinYmlPath, centralConfig); err != nil {
		errorPrinter.Printf("Could not add the DuckDB database to .gitignore: %v\n", err)
	}

	return summary, nil
}

// projectConfigSummary describes the project-level .bruin.yml without touching
// it, for templates that ship no config of their own. A pipeline outside a Git
// repository yields an empty summary, which prints as "no config yet".
func projectConfigSummary(pipelinePath string) initConfigSummary {
	repoRoot, err := git.FindRepoFromPath(pipelinePath)
	if err != nil {
		return initConfigSummary{}
	}

	bruinYmlPath := filepath.Join(repoRoot.Path, ".bruin.yml")

	return initConfigSummary{
		path:         bruinYmlPath,
		existed:      fileExists(bruinYmlPath),
		locationNote: "This is your Git repo root, so it may sit several levels above the pipeline folder.",
	}
}

// validateMergeDestination reports whether pipelinePath is an existing Bruin
// pipeline. It runs before anything is written so that a bad destination cannot
// leave a half-merged project behind.
func validateMergeDestination(pipelinePath string) error {
	info, err := os.Stat(pipelinePath)
	if err != nil {
		return fmt.Errorf("could not open pipeline path %q: %w", pipelinePath, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("pipeline path %q is not a directory", pipelinePath)
	}
	if _, err := getPipelineDefinitionFullPath(pipelinePath); err != nil {
		return err
	}

	return nil
}

// findNonDirectoryParent returns the first directory relativePath needs that
// already exists as something else, or "" when the path is clear. It walks
// top-down so every Lstat is rooted at a known directory and cannot fail with a
// bare ENOTDIR.
func findNonDirectoryParent(root, relativePath string) (string, error) {
	current := root
	currentRelative := ""
	for _, part := range strings.Split(filepath.Dir(relativePath), string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}

		current = filepath.Join(current, part)
		currentRelative = filepath.Join(currentRelative, part)

		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			// Nothing below a directory that does not exist yet can conflict.
			return "", nil
		}
		if err != nil {
			return "", fmt.Errorf("could not inspect destination directory %q: %w", current, err)
		}
		if !info.IsDir() {
			return currentRelative, nil
		}
	}

	return "", nil
}

// collectMergeConflicts reports every destination path that is already taken. It
// writes nothing, so a conflict leaves the project untouched.
func collectMergeConflicts(pipelinePath string, files []templateMergeFile) error {
	if err := validateMergeDestination(pipelinePath); err != nil {
		return err
	}

	destinationRoot := filepath.Clean(pipelinePath)
	conflicts := make([]string, 0)
	for _, file := range files {
		relativePath := filepath.FromSlash(file.path)
		if !filepath.IsLocal(relativePath) {
			return fmt.Errorf("template contains an invalid file path %q", file.path)
		}

		// Checked before the destination itself: a regular file in the parent
		// chain makes the Lstat below fail with ENOTDIR instead of reporting it.
		parentConflict, err := findNonDirectoryParent(destinationRoot, relativePath)
		if err != nil {
			return err
		}
		if parentConflict != "" {
			conflicts = append(conflicts, parentConflict)
			continue
		}

		destinationPath := filepath.Join(destinationRoot, relativePath)
		if _, err := os.Lstat(destinationPath); err == nil {
			conflicts = append(conflicts, relativePath)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("could not inspect destination file %q: %w", destinationPath, err)
		}
	}

	if len(conflicts) > 0 {
		sort.Strings(conflicts)
		conflicts = slices.Compact(conflicts)
		return fmt.Errorf("cannot merge template files because these paths already exist: %s", strings.Join(conflicts, ", "))
	}

	return nil
}

// writeTemplateFiles copies the files into the pipeline. It must only run after
// collectMergeConflicts has passed: that is what makes each destination safe to
// write and safe to remove again.
func writeTemplateFiles(pipelinePath string, files []templateMergeFile) error {
	destinationRoot := filepath.Clean(pipelinePath)
	written := make([]string, 0, len(files))
	for _, file := range files {
		destinationPath := filepath.Join(destinationRoot, filepath.FromSlash(file.path))
		if err := writeTemplateFile(destinationPath, file.content); err != nil {
			// Roll back, otherwise a half-finished copy reports its own files as
			// conflicts on the next attempt and the command can never succeed.
			return errors.Join(err, removeTemplateFiles(written))
		}

		written = append(written, destinationPath)
	}

	return nil
}

func writeTemplateFile(destinationPath string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o755); err != nil {
		return fmt.Errorf("could not create directory %q: %w", filepath.Dir(destinationPath), err)
	}

	out, err := os.OpenFile(destinationPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644) //nolint:gosec // destination is validated as a local path below the requested pipeline
	if err != nil {
		return fmt.Errorf("could not create file %q: %w", destinationPath, err)
	}
	// The file exists from here on, so every failure path removes it: a stub would
	// be reported as a conflict on the next attempt.
	if _, err := out.Write(content); err != nil {
		_ = out.Close()
		_ = os.Remove(destinationPath)
		return fmt.Errorf("could not write file %q: %w", destinationPath, err)
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(destinationPath)
		return fmt.Errorf("could not close file %q: %w", destinationPath, err)
	}

	return nil
}

// removeTemplateFiles deletes files a failed merge created. Directories are left
// alone: an empty directory does not block a retry.
func removeTemplateFiles(paths []string) error {
	cleanupErrors := make([]error, 0)
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("could not remove %q while undoing the merge: %w", path, err))
		}
	}

	return errors.Join(cleanupErrors...)
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
			&cli.BoolFlag{
				Name:  "merge",
				Usage: "merges the template's assets, macros and project config into an existing pipeline without overwriting existing files",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()

			inputPath := c.Args().Get(1)
			merge := c.Bool("merge")
			// Validated before the interactive template picker so a misuse of the
			// flag fails immediately instead of after five prompts.
			if merge {
				if inputPath == "" {
					errorPrinter.Printf("A path to an existing pipeline is required when using --merge.\n")
					return cli.Exit("", 1)
				}
				if c.Bool("in-place") {
					errorPrinter.Printf("--merge and --in-place cannot be used together.\n")
					return cli.Exit("", 1)
				}
			}

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

			if inputPath == "" {
				if templateName == DefaultTemplate {
					inputPath = DefaultFolderName
				} else {
					inputPath = templateName
				}
			}

			if merge {
				// Checked before the ecommerce stack picker so a bad destination
				// fails immediately rather than after five prompts.
				if err := validateMergeDestination(inputPath); err != nil {
					errorPrinter.Printf("Could not merge template %s: %v\n", templateName, err)
					return cli.Exit("", 1)
				}

				var (
					mergeFiles           []templateMergeFile
					templateBruinContent []byte
					ecommerceChoices     *EcommerceChoices
				)
				if templateName == "ecommerce" {
					ecommerceChoices, err = runEcommerceStackPicker()
					if err != nil {
						errorPrinter.Printf("Error during stack selection: %v\n", err)
						return cli.Exit("", 1)
					}
					if ecommerceChoices == nil {
						return nil
					}

					mergeFiles, err = loadEcommerceMergeFiles(ecommerceChoices)
					if err != nil {
						errorPrinter.Printf("Could not generate ecommerce assets: %v\n", err)
						return cli.Exit("", 1)
					}
					templateBruinContent = []byte(generateBruinYML(ecommerceChoices))
				} else {
					mergeFiles, err = loadTemplateMergeFiles(templateName)
					if err != nil {
						errorPrinter.Printf("Could not load template assets: %v\n", err)
						return cli.Exit("", 1)
					}

					templateBruinContent, err = loadTemplateBruinConfig(templateName)
					if err != nil {
						errorPrinter.Printf("Could not load template config: %v\n", err)
						return cli.Exit("", 1)
					}
				}

				// Conflicts first so a clash aborts before anything is written, then
				// the config, then the copy. Both writing steps are safe to repeat.
				if err := collectMergeConflicts(inputPath, mergeFiles); err != nil {
					errorPrinter.Printf("Could not merge template %s: %v\n", templateName, err)
					return cli.Exit("", 1)
				}

				configSummary := projectConfigSummary(inputPath)
				if templateBruinContent != nil {
					configSummary, err = mergeTemplateConfigIntoProject(inputPath, templateBruinContent)
					if err != nil {
						errorPrinter.Printf("Could not merge template config: %v\n", err)
						return cli.Exit("", 1)
					}
				}

				if err := writeTemplateFiles(inputPath, mergeFiles); err != nil {
					errorPrinter.Printf("Could not copy the '%s' template files into %s: %v\n", templateName, inputPath, err)
					return cli.Exit("", 1)
				}

				telemetryFields := map[string]interface{}{
					"template_name": templateName,
					"interactive":   selectedViaInteractive,
					"merge":         true,
				}
				if ecommerceChoices != nil {
					// The stack picker always runs, so this path is interactive
					// regardless of how the template itself was chosen.
					telemetryFields["interactive"] = true
					telemetryFields["warehouse"] = ecommerceChoices.Warehouse
					telemetryFields["payments"] = ecommerceChoices.Payments
					telemetryFields["marketing"] = ecommerceChoices.Marketing
					telemetryFields["ads"] = ecommerceChoices.Ads
					telemetryFields["analytics"] = ecommerceChoices.Analytics
				}
				telemetry.SetTemplateName(templateName)
				telemetry.SendEvent("template_selected", telemetryFields)

				fileLabel := "files"
				if len(mergeFiles) == 1 {
					fileLabel = "file"
				}
				successPrinter.Printf("\n\nMerged %d %s from the '%s' template into pipeline '%s'.\n", len(mergeFiles), fileLabel, templateName, inputPath)
				if ecommerceChoices != nil {
					printEcommerceSummary(ecommerceChoices)
				}
				printInitSummary(configSummary, inputPath)

				return nil
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
