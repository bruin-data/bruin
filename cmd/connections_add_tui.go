package cmd

import (
	"fmt"
	"os"
	path2 "path"
	"sort"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

// TUI wizard steps.
const (
	stepSelectEnv  = 0
	stepEnterName  = 1
	stepSelectType = 2
	stepFillFields = 3
	stepSaving     = 4
	stepDone       = 5
	stepCancelled  = 6
)

const keyEnter = "enter"

// Styling for the TUI.
var (
	tuiTitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FF6B35")).
			MarginBottom(1)

	tuiStepStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#4F46E5")).
			Bold(true)

	tuiHelpStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#9CA3AF"))

	tuiSuccessStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#059669")).
			Bold(true)

	tuiErrorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#EF4444"))
)

// saveResultMsg is sent after attempting to save the connection.
type saveResultMsg struct {
	err error
}

// listItem implements list.Item for environment and type selection.
type listItem struct {
	title string
}

func (i listItem) Title() string       { return i.title }
func (i listItem) Description() string { return "" }
func (i listItem) FilterValue() string { return i.title }

// addConnectionModel is the bubbletea model for the interactive wizard.
type addConnectionModel struct {
	step int

	// Step 0: environment selection
	envList list.Model
	envs    []string

	// Step 1: name input
	nameInput textinput.Model
	nameErr   string

	// Step 2: type selection
	typeList  list.Model
	typeNames []string

	// Step 3: credential fields
	fieldDefs    []config.ConnectionFieldDef
	fieldInputs  []textinput.Model
	fieldFocused int

	// Collected results
	environment string
	connName    string
	connType    string

	// Save error (displayed in step 3 so user can fix and retry)
	saveErr string

	// Config for duplicate checking and saving
	cfg *config.Config

	// Window size
	width  int
	height int
}

func newAddConnectionModel(cfg *config.Config) addConnectionModel {
	m := addConnectionModel{
		step: stepSelectEnv,
		cfg:  cfg,
	}

	// Step 0: environment list
	envs := cfg.GetEnvironmentNames()
	sort.Strings(envs)
	m.envs = envs

	items := make([]list.Item, len(envs))
	for i, e := range envs {
		items[i] = listItem{title: e}
	}
	envDelegate := list.NewDefaultDelegate()
	envDelegate.ShowDescription = false
	m.envList = list.New(items, envDelegate, 60, 14)
	m.envList.Title = "Select an environment"
	m.envList.SetShowStatusBar(false)
	m.envList.SetFilteringEnabled(false)

	// Step 1: name input
	m.nameInput = textinput.New()
	m.nameInput.Placeholder = "my-connection"
	m.nameInput.CharLimit = 128

	// Step 2: type list
	typeNames := config.GetConnectionTypeNames()
	m.typeNames = typeNames

	typeItems := make([]list.Item, len(typeNames))
	for i, t := range typeNames {
		typeItems[i] = listItem{title: t}
	}
	typeDelegate := list.NewDefaultDelegate()
	typeDelegate.ShowDescription = false
	m.typeList = list.New(typeItems, typeDelegate, 60, 20)
	m.typeList.Title = "Select a connection type"
	m.typeList.SetShowStatusBar(true)
	m.typeList.SetFilteringEnabled(true)

	return m
}

func (m addConnectionModel) Init() tea.Cmd {
	return nil
}

func (m addConnectionModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.envList.SetSize(msg.Width-4, msg.Height-6)
		m.typeList.SetSize(msg.Width-4, msg.Height-6)
		return m, nil
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			m.step = stepCancelled
			return m, tea.Quit
		}
	case saveResultMsg:
		if msg.err != nil {
			// Save failed: go back to field entry with error displayed
			m.saveErr = msg.err.Error()
			m.step = stepFillFields
			if len(m.fieldInputs) > 0 {
				m.fieldInputs[m.fieldFocused].Focus()
				return m, m.fieldInputs[m.fieldFocused].Cursor.BlinkCmd()
			}
			return m, nil
		}
		// Save succeeded
		m.step = stepDone
		return m, tea.Quit
	}

	switch m.step {
	case stepSelectEnv:
		return m.updateSelectEnv(msg)
	case stepEnterName:
		return m.updateEnterName(msg)
	case stepSelectType:
		return m.updateSelectType(msg)
	case stepFillFields:
		return m.updateFillFields(msg)
	}
	return m, nil
}

func (m addConnectionModel) updateSelectEnv(msg tea.Msg) (tea.Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyMsg); ok && keyMsg.String() == keyEnter {
		if sel, ok := m.envList.SelectedItem().(listItem); ok {
			m.environment = sel.title
			m.step = stepEnterName
			m.nameInput.Focus()
			return m, m.nameInput.Cursor.BlinkCmd()
		}
	}
	var cmd tea.Cmd
	m.envList, cmd = m.envList.Update(msg)
	return m, cmd
}

func (m addConnectionModel) updateEnterName(msg tea.Msg) (tea.Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch keyMsg.String() {
		case "esc":
			m.step = stepSelectEnv
			m.nameErr = ""
			return m, nil
		case keyEnter:
			name := strings.TrimSpace(m.nameInput.Value())
			if name == "" {
				m.nameErr = "Name cannot be empty"
				return m, nil
			}
			env, exists := m.cfg.Environments[m.environment]
			if exists && env.Connections.Exists(name) {
				m.nameErr = fmt.Sprintf("Connection '%s' already exists in '%s'", name, m.environment)
				return m, nil
			}
			m.connName = name
			m.nameErr = ""
			m.step = stepSelectType
			return m, nil
		}
	}
	var cmd tea.Cmd
	m.nameInput, cmd = m.nameInput.Update(msg)
	return m, cmd
}

func (m addConnectionModel) updateSelectType(msg tea.Msg) (tea.Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch keyMsg.String() {
		case "esc":
			if m.typeList.FilterState() == list.Filtering {
				// let the list handle esc to clear filter
				break
			}
			m.step = stepEnterName
			return m, m.nameInput.Cursor.BlinkCmd()
		case keyEnter:
			if m.typeList.FilterState() == list.Filtering {
				break
			}
			if sel, ok := m.typeList.SelectedItem().(listItem); ok {
				m.connType = sel.title
				m = m.initFieldInputs()
				m.step = stepFillFields
				if len(m.fieldInputs) > 0 {
					return m, m.fieldInputs[0].Cursor.BlinkCmd()
				}
				return m, nil
			}
		}
	}
	var cmd tea.Cmd
	m.typeList, cmd = m.typeList.Update(msg)
	return m, cmd
}

func (m addConnectionModel) initFieldInputs() addConnectionModel {
	fields := config.GetConnectionFieldsForType(m.connType)
	m.fieldDefs = fields
	m.fieldInputs = make([]textinput.Model, len(fields))
	m.fieldFocused = 0
	m.saveErr = ""

	for i, f := range fields {
		ti := textinput.New()
		ti.Placeholder = f.Name
		ti.CharLimit = 512

		if f.DefaultValue != "" {
			ti.Placeholder = fmt.Sprintf("%s (default: %s)", f.Name, f.DefaultValue)
		}

		if isSecretField(f.Name) {
			ti.EchoMode = textinput.EchoPassword
		}

		if i == 0 {
			ti.Focus()
		}
		m.fieldInputs[i] = ti
	}
	return m
}

// saveConnection attempts to save and returns a tea.Cmd that sends a saveResultMsg.
func (m addConnectionModel) saveConnection() tea.Msg { //nolint:ireturn
	creds := collectCredentials(m.fieldDefs, m.fieldInputs)

	if err := m.cfg.AddConnection(m.environment, m.connName, m.connType, creds); err != nil {
		return saveResultMsg{err: fmt.Errorf("failed to add connection: %w", err)}
	}

	if err := m.cfg.Persist(); err != nil {
		return saveResultMsg{err: fmt.Errorf("failed to persist config: %w", err)}
	}

	return saveResultMsg{err: nil}
}

func (m addConnectionModel) updateFillFields(msg tea.Msg) (tea.Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch keyMsg.String() {
		case "esc":
			m.step = stepSelectType
			m.saveErr = ""
			return m, nil
		case "tab", "down":
			if m.fieldFocused < len(m.fieldInputs)-1 {
				m.fieldInputs[m.fieldFocused].Blur()
				m.fieldFocused++
				m.fieldInputs[m.fieldFocused].Focus()
				return m, m.fieldInputs[m.fieldFocused].Cursor.BlinkCmd()
			}
		case "shift+tab", "up":
			if m.fieldFocused > 0 {
				m.fieldInputs[m.fieldFocused].Blur()
				m.fieldFocused--
				m.fieldInputs[m.fieldFocused].Focus()
				return m, m.fieldInputs[m.fieldFocused].Cursor.BlinkCmd()
			}
		case keyEnter:
			// If on the last field or no fields, attempt save
			if m.fieldFocused == len(m.fieldInputs)-1 || len(m.fieldInputs) == 0 {
				m.saveErr = ""
				m.step = stepSaving
				return m, m.saveConnection
			}
			// Otherwise, move to next field
			m.fieldInputs[m.fieldFocused].Blur()
			m.fieldFocused++
			m.fieldInputs[m.fieldFocused].Focus()
			return m, m.fieldInputs[m.fieldFocused].Cursor.BlinkCmd()
		}
	}

	if m.fieldFocused < len(m.fieldInputs) {
		var cmd tea.Cmd
		m.fieldInputs[m.fieldFocused], cmd = m.fieldInputs[m.fieldFocused].Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m addConnectionModel) View() string {
	var b strings.Builder

	b.WriteString(tuiTitleStyle.Render("Add Connection"))
	b.WriteString("\n")

	switch m.step {
	case stepSelectEnv:
		b.WriteString(tuiStepStyle.Render("Step 1/4: "))
		b.WriteString("Select environment\n\n")
		b.WriteString(m.envList.View())

	case stepEnterName:
		b.WriteString(tuiStepStyle.Render("Step 2/4: "))
		b.WriteString("Enter connection name\n\n")
		fmt.Fprintf(&b, "  Environment: %s\n\n", m.environment)
		b.WriteString("  Name: ")
		b.WriteString(m.nameInput.View())
		b.WriteString("\n")
		if m.nameErr != "" {
			b.WriteString("\n  ")
			b.WriteString(tuiErrorStyle.Render(m.nameErr))
			b.WriteString("\n")
		}
		b.WriteString("\n")
		b.WriteString(tuiHelpStyle.Render("  enter: confirm • esc: back"))

	case stepSelectType:
		b.WriteString(tuiStepStyle.Render("Step 3/4: "))
		b.WriteString("Select connection type\n\n")
		fmt.Fprintf(&b, "  Environment: %s  |  Name: %s\n\n", m.environment, m.connName)
		b.WriteString(m.typeList.View())

	case stepFillFields:
		b.WriteString(tuiStepStyle.Render("Step 4/4: "))
		b.WriteString("Enter credentials\n\n")
		fmt.Fprintf(&b, "  Environment: %s  |  Name: %s  |  Type: %s\n\n", m.environment, m.connName, m.connType)

		if len(m.fieldInputs) == 0 {
			b.WriteString("  No fields required for this connection type.\n")
			b.WriteString("  Press enter to save.\n")
		} else {
			for i, f := range m.fieldDefs {
				label := f.Name
				if f.IsRequired {
					label += " *"
				}
				if i == m.fieldFocused {
					fmt.Fprintf(&b, "  %s\n", tuiStepStyle.Render(label))
				} else {
					fmt.Fprintf(&b, "  %s\n", label)
				}
				fmt.Fprintf(&b, "  %s\n\n", m.fieldInputs[i].View())
			}
		}

		if m.saveErr != "" {
			b.WriteString("\n  ")
			b.WriteString(tuiErrorStyle.Render("Error: " + m.saveErr))
			b.WriteString("\n\n")
		}
		b.WriteString(tuiHelpStyle.Render("  tab/shift+tab: navigate • enter: next/submit • esc: back"))

	case stepSaving:
		b.WriteString("Saving connection...\n")

	case stepDone:
		b.WriteString(tuiSuccessStyle.Render("Connection saved successfully!"))
		b.WriteString("\n")

	case stepCancelled:
		b.WriteString("Cancelled.\n")
	}

	return b.String()
}

// isSecretField returns true for field names that likely contain sensitive data.
func isSecretField(name string) bool {
	lower := strings.ToLower(name)
	secrets := []string{"password", "secret", "token", "api_key", "private_key", "access_key"}
	for _, s := range secrets {
		if strings.Contains(lower, s) {
			return true
		}
	}
	return false
}

// collectCredentials converts the text input values into a credentials map,
// performing type coercion for int and bool fields.
func collectCredentials(fields []config.ConnectionFieldDef, inputs []textinput.Model) map[string]any {
	creds := make(map[string]any)
	for i, f := range fields {
		val := strings.TrimSpace(inputs[i].Value())
		if val == "" {
			if f.DefaultValue != "" {
				val = f.DefaultValue
			} else {
				continue
			}
		}
		switch f.Type {
		case "int":
			if n, err := strconv.Atoi(val); err == nil {
				creds[f.Name] = n
			} else {
				creds[f.Name] = val
			}
		case "bool":
			if b, err := strconv.ParseBool(val); err == nil {
				creds[f.Name] = b
			} else {
				creds[f.Name] = val
			}
		default:
			creds[f.Name] = val
		}
	}
	return creds
}

// runInteractiveAddConnection loads the config, runs the TUI, and persists the result.
func runInteractiveAddConnection(c *cli.Command) error {
	path := "."
	if c.Args().Present() {
		path = c.Args().First()
	}

	configFilePath := c.String("config-file")
	if configFilePath == "" {
		repoRoot, err := git.FindRepoFromPath(path)
		if err != nil {
			errorPrinter.Printf("Failed to find the git repository root: %v\n", err)
			return cli.Exit("", 1)
		}

		configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
	}

	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
	if err != nil {
		errorPrinter.Printf("Failed to load or create config: %v\n", err)
		return cli.Exit("", 1)
	}

	envs := cm.GetEnvironmentNames()
	if len(envs) == 0 {
		errorPrinter.Println("No environments found in configuration. Please add an environment first.")
		return cli.Exit("", 1)
	}

	m := newAddConnectionModel(cm)

	p := tea.NewProgram(m, tea.WithOutput(os.Stderr))
	result, err := p.Run()
	if err != nil {
		errorPrinter.Printf("TUI error: %v\n", err)
		return cli.Exit("", 1)
	}

	final, ok := result.(addConnectionModel)
	if !ok || final.step != stepDone {
		return nil
	}

	infoPrinter.Printf("Successfully added connection: %s\n", final.connName)
	return nil
}
