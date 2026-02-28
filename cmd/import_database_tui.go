package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/afero"
)

var supportedImportConnectionTypes = map[string]bool{
	"google_cloud_platform": true,
	"snowflake":             true,
	"postgres":              true,
	"mssql":                 true,
	"mysql":                 true,
	"databricks":            true,
	"duckdb":                true,
	"motherduck":            true,
	"clickhouse":            true,
	"oracle":                true,
	"athena":                true,
	"synapse":               true,
	"redshift":              true,
	"sqlite":                true,
}

const (
	dbtuiStepConnection = iota
	dbtuiStepLoading
	dbtuiStepSchemas
	dbtuiStepImporting
	dbtuiStepDone
)

var (
	dbtuiHeaderStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color(colorOrange))

	dbtuiSubtitleStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color(colorDarkGray))

	dbtuiDimStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(colorDarkGray))

	dbtuiHintStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(colorDarkGray))

	dbtuiValueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#D1D5DB"))

	dbtuiErrorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#EF4444")).
			Bold(true)

	dbtuiBreadcrumbStyle = lipgloss.NewStyle().
				Background(lipgloss.Color(colorDarkBg)).
				Foreground(lipgloss.Color(colorLightGray)).
				Padding(0, 1)

	dbtuiStepIndicatorActive = lipgloss.NewStyle().
					Foreground(lipgloss.Color(colorOrange)).
					Bold(true)

	dbtuiStepIndicatorDone = lipgloss.NewStyle().
				Foreground(lipgloss.Color(colorSuccess))

	dbtuiStepIndicatorPending = lipgloss.NewStyle().
					Foreground(lipgloss.Color(colorDarkGray))
)

type dbSummaryLoadedMsg struct {
	conn    any
	summary *ansisql.DBDatabase
	err     error
}

type importCompleteMsg struct {
	importedCount int
	mergedCount   int
	warnings      []importWarning
	err           error
}

type importConnectionItem struct {
	name     string
	connType string
}

func (i importConnectionItem) Title() string       { return i.name }
func (i importConnectionItem) Description() string { return i.connType }
func (i importConnectionItem) FilterValue() string { return i.name }

type importSchemaItem struct {
	schema *ansisql.DBSchema
	index  int // position in dbSummary.Schemas
}

func (i importSchemaItem) Title() string       { return i.schema.Name }
func (i importSchemaItem) Description() string { return fmt.Sprintf("%d tables", len(i.schema.Tables)) }
func (i importSchemaItem) FilterValue() string { return i.schema.Name }

type schemaCheckboxDelegate struct {
	selectedItems map[int]bool // keyed by list index
}

func (d schemaCheckboxDelegate) Height() int                             { return 1 }
func (d schemaCheckboxDelegate) Spacing() int                            { return 0 }
func (d schemaCheckboxDelegate) Update(_ tea.Msg, _ *list.Model) tea.Cmd { return nil }

func (d schemaCheckboxDelegate) Render(w io.Writer, m list.Model, index int, item list.Item) {
	si, ok := item.(importSchemaItem)
	if !ok {
		return
	}

	isCurrent := index == m.Index()
	isSel := d.selectedItems[si.index]

	checkbox := "○"
	if isSel {
		checkbox = "●"
	}

	name := si.schema.Name
	meta := fmt.Sprintf(" %d tables", len(si.schema.Tables))
	line := fmt.Sprintf(" %s  %s%s", checkbox, name, meta)

	width := m.Width()
	if len(line) > width {
		line = line[:width-1] + "…"
	}
	if len(line) < width {
		line += strings.Repeat(" ", width-len(line))
	}

	switch {
	case isCurrent && isSel:
		style := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(lipgloss.Color("#7C3AED")).
			Bold(true)
		fmt.Fprint(w, style.Render(line))
	case isCurrent:
		style := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(lipgloss.Color("#7C3AED"))
		fmt.Fprint(w, style.Render(line))
	case isSel:
		style := lipgloss.NewStyle().
			Foreground(lipgloss.Color(colorSuccess))
		fmt.Fprint(w, style.Render(line))
	default:
		style := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#D1D5DB"))
		fmt.Fprint(w, style.Render(line))
	}
}

type importDatabaseModel struct {
	ctx          context.Context //nolint:containedctx // bubbletea pattern for async commands
	pipelinePath string
	environment  string
	configFile   string
	fillColumns  bool

	step int

	cfg       *config.Config
	connList  list.Model
	connItems []importConnectionItem

	spinner      spinner.Model
	loadingError error

	selectedConnName string
	conn             any
	dbSummary        *ansisql.DBDatabase
	schemaList       list.Model
	schemaDelegate   schemaCheckboxDelegate
	schemaSelected   map[int]bool // keyed by schema index in dbSummary.Schemas

	importedCount  int
	mergedCount    int
	importWarnings []importWarning
	importError    error
	importDone     bool

	windowWidth  int
	windowHeight int
	quitting     bool
}

func (m *importDatabaseModel) Init() tea.Cmd {
	return nil
}

func newImportSpinner() spinner.Model {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color(colorOrange))
	return s
}

func (m *importDatabaseModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.windowWidth = msg.Width
		m.windowHeight = msg.Height
		h := msg.Height - 4
		w := msg.Width - 2
		switch m.step {
		case dbtuiStepConnection:
			m.connList.SetSize(w, h)
		case dbtuiStepSchemas:
			m.schemaList.SetSize(w, h)
		}
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)

	case dbSummaryLoadedMsg:
		if msg.err != nil {
			m.loadingError = msg.err
			return m, nil
		}
		m.conn = msg.conn
		m.dbSummary = msg.summary
		m.buildSchemaList()
		m.step = dbtuiStepSchemas
		return m, nil

	case importCompleteMsg:
		m.importedCount = msg.importedCount
		m.mergedCount = msg.mergedCount
		m.importWarnings = msg.warnings
		m.importError = msg.err
		m.importDone = true
		m.step = dbtuiStepDone
		return m, tea.Quit

	case spinner.TickMsg:
		if m.step == dbtuiStepLoading || m.step == dbtuiStepImporting {
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			return m, cmd
		}
	}

	var cmd tea.Cmd
	switch m.step {
	case dbtuiStepConnection:
		m.connList, cmd = m.connList.Update(msg)
	case dbtuiStepSchemas:
		m.schemaList, cmd = m.schemaList.Update(msg)
	}
	return m, cmd
}

func (m *importDatabaseModel) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.String() == "ctrl+c" {
		m.quitting = true
		return m, tea.Quit
	}

	switch m.step {
	case dbtuiStepConnection:
		return m.handleConnectionKey(msg)
	case dbtuiStepLoading:
		return m.handleLoadingKey(msg)
	case dbtuiStepSchemas:
		return m.handleSchemaKey(msg)
	}
	return m, nil
}

func (m *importDatabaseModel) handleConnectionKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		m.quitting = true
		return m, tea.Quit
	case keyEnter:
		idx := m.connList.Index()
		if idx >= 0 && idx < len(m.connItems) {
			m.selectedConnName = m.connItems[idx].name
			m.step = dbtuiStepLoading
			m.loadingError = nil
			m.spinner = newImportSpinner()
			return m, tea.Batch(m.spinner.Tick, m.loadDatabaseSummaryCmd())
		}
		return m, nil
	}
	var cmd tea.Cmd
	m.connList, cmd = m.connList.Update(msg)
	return m, cmd
}

func (m *importDatabaseModel) handleLoadingKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.loadingError = nil
		m.step = dbtuiStepConnection
	case keyEnter:
		if m.loadingError != nil {
			m.loadingError = nil
			m.step = dbtuiStepConnection
		}
	}
	return m, nil
}

func (m *importDatabaseModel) handleSchemaKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.schemaList.FilterState() == list.Filtering {
		var cmd tea.Cmd
		m.schemaList, cmd = m.schemaList.Update(msg)
		return m, cmd
	}

	switch msg.String() {
	case "esc":
		if m.schemaList.FilterState() == list.FilterApplied {
			m.schemaList.ResetFilter()
			return m, nil
		}
		m.step = dbtuiStepConnection
		return m, nil

	case " ", "x":
		if item, ok := m.schemaList.SelectedItem().(importSchemaItem); ok {
			m.schemaSelected[item.index] = !m.schemaSelected[item.index]
			m.schemaDelegate.selectedItems[item.index] = m.schemaSelected[item.index]
			m.schemaList.SetDelegate(m.schemaDelegate)
		}
		return m, nil

	case "a":
		for i := range m.dbSummary.Schemas {
			m.schemaSelected[i] = true
			m.schemaDelegate.selectedItems[i] = true
		}
		m.schemaList.SetDelegate(m.schemaDelegate)
		return m, nil

	case "n":
		for i := range m.dbSummary.Schemas {
			m.schemaSelected[i] = false
			m.schemaDelegate.selectedItems[i] = false
		}
		m.schemaList.SetDelegate(m.schemaDelegate)
		return m, nil

	case keyEnter:
		count := 0
		for _, v := range m.schemaSelected {
			if v {
				count++
			}
		}
		if count == 0 {
			return m, nil
		}
		m.step = dbtuiStepImporting
		m.spinner = newImportSpinner()
		return m, tea.Batch(m.spinner.Tick, m.executeImportCmd())
	}

	var cmd tea.Cmd
	m.schemaList, cmd = m.schemaList.Update(msg)
	return m, cmd
}

func (m *importDatabaseModel) buildSchemaList() {
	schemas := m.dbSummary.Schemas
	items := make([]list.Item, len(schemas))
	for i, s := range schemas {
		items[i] = importSchemaItem{schema: s, index: i}
	}

	m.schemaSelected = make(map[int]bool, len(schemas))
	m.schemaDelegate = schemaCheckboxDelegate{
		selectedItems: make(map[int]bool, len(schemas)),
	}

	m.schemaList = list.New(items, m.schemaDelegate, m.windowWidth-2, m.windowHeight-4)
	m.schemaList.Title = "Select Schemas to Import"
	m.schemaList.SetShowStatusBar(true)
	m.schemaList.SetFilteringEnabled(true)
	m.schemaList.Styles.Title = dbtuiHeaderStyle
	m.schemaList.SetShowHelp(false)
}

func (m *importDatabaseModel) View() string {
	if m.quitting && m.step != dbtuiStepDone {
		return ""
	}

	var b strings.Builder
	b.WriteString(m.renderStepBar())
	b.WriteString("\n")

	switch m.step {
	case dbtuiStepConnection:
		b.WriteString(m.connList.View())

	case dbtuiStepLoading:
		b.WriteString(m.renderLoading())

	case dbtuiStepSchemas:
		b.WriteString(m.schemaList.View())
		b.WriteString("\n")
		selCount := 0
		for _, v := range m.schemaSelected {
			if v {
				selCount++
			}
		}
		info := fmt.Sprintf("%d/%d selected", selCount, len(m.dbSummary.Schemas))
		b.WriteString(dbtuiHintStyle.Render("  space: toggle  a: all  n: none  /: search  enter: import  esc: back"))
		b.WriteString("  ")
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color(colorPurple)).Bold(true).Render(info))

	case dbtuiStepImporting:
		b.WriteString(m.renderImporting())
	}

	return b.String()
}

func (m *importDatabaseModel) renderStepBar() string {
	steps := []string{"Connection", "Schemas", "Import"}
	activeIdx := 0
	switch m.step {
	case dbtuiStepConnection, dbtuiStepLoading:
		activeIdx = 0
	case dbtuiStepSchemas:
		activeIdx = 1
	case dbtuiStepImporting, dbtuiStepDone:
		activeIdx = 2
	}

	var parts []string
	for i, name := range steps {
		switch {
		case i < activeIdx:
			parts = append(parts, dbtuiStepIndicatorDone.Render("● "+name))
		case i == activeIdx:
			parts = append(parts, dbtuiStepIndicatorActive.Render("● "+name))
		default:
			parts = append(parts, dbtuiStepIndicatorPending.Render("○ "+name))
		}
	}

	bar := strings.Join(parts, dbtuiDimStyle.Render("  ―  "))

	ctx := ""
	if m.selectedConnName != "" {
		ctx = dbtuiDimStyle.Render("  │  ") + dbtuiValueStyle.Render(m.selectedConnName)
	}

	return dbtuiBreadcrumbStyle.Width(m.windowWidth).Render(bar + ctx)
}

func (m *importDatabaseModel) renderLoading() string {
	var b strings.Builder
	b.WriteString("\n")
	if m.loadingError != nil {
		b.WriteString(dbtuiErrorStyle.Render("  Error: "))
		b.WriteString(dbtuiValueStyle.Render(m.loadingError.Error()))
		b.WriteString("\n\n")
		b.WriteString(dbtuiHintStyle.Render("  esc: go back"))
	} else {
		b.WriteString("  ")
		b.WriteString(m.spinner.View())
		b.WriteString(dbtuiSubtitleStyle.Render(fmt.Sprintf(" Connecting to %s and loading schema...", m.selectedConnName)))
	}
	return b.String()
}

func (m *importDatabaseModel) renderImporting() string {
	totalTables := 0
	for i, s := range m.dbSummary.Schemas {
		if m.schemaSelected[i] {
			totalTables += len(s.Tables)
		}
	}
	var b strings.Builder
	b.WriteString("\n  ")
	b.WriteString(m.spinner.View())
	b.WriteString(dbtuiSubtitleStyle.Render(fmt.Sprintf(" Importing %d tables into %s...", totalTables, m.pipelinePath)))
	return b.String()
}

func (m *importDatabaseModel) loadDatabaseSummaryCmd() tea.Cmd {
	connName := m.selectedConnName
	ctx := m.ctx
	cfg := m.cfg

	return func() tea.Msg {
		manager, errs := connection.NewManagerFromConfigWithContext(ctx, cfg)
		if len(errs) > 0 {
			return dbSummaryLoadedMsg{err: fmt.Errorf("failed to create connection manager: %w", errs[0])}
		}

		conn := manager.GetConnection(connName)
		if conn == nil {
			return dbSummaryLoadedMsg{err: fmt.Errorf("connection '%s' not found", connName)}
		}

		summarizer, ok := conn.(interface {
			GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error)
		})
		if !ok {
			return dbSummaryLoadedMsg{err: fmt.Errorf("connection '%s' does not support database summary", connName)}
		}

		summary, err := summarizer.GetDatabaseSummary(ctx)
		if err != nil {
			return dbSummaryLoadedMsg{err: fmt.Errorf("failed to load database summary: %w", err)}
		}

		return dbSummaryLoadedMsg{conn: conn, summary: summary}
	}
}

func (m *importDatabaseModel) executeImportCmd() tea.Cmd {
	ctx := m.ctx
	pipelinePath := m.pipelinePath
	connName := m.selectedConnName
	conn := m.conn
	fillColumns := m.fillColumns
	summary := m.dbSummary

	selectedSchemaIdxs := make(map[int]bool)
	for k, v := range m.schemaSelected {
		if v {
			selectedSchemaIdxs[k] = true
		}
	}

	return func() tea.Msg {
		fs := afero.NewOsFs()

		resolvedPath := resolvePipelinePath(pipelinePath)

		pipelineFound, err := GetPipelinefromPath(ctx, resolvedPath)
		if err != nil {
			return importCompleteMsg{err: fmt.Errorf("failed to get pipeline: %w", err)}
		}

		existingAssets := make(map[string]*pipeline.Asset, len(pipelineFound.Assets))
		for _, asset := range pipelineFound.Assets {
			existingAssets[strings.ToLower(asset.Name)] = asset
		}

		assetsPath := filepath.Join(resolvedPath, "assets")
		assetType := determineAssetTypeFromConnection(connName, conn)

		var (
			totalTables      int
			mergedTableCount int
			warnings         []importWarning
		)

		for i, schema := range summary.Schemas {
			if !selectedSchemaIdxs[i] {
				continue
			}
			for _, table := range schema.Tables {
				fullName := fmt.Sprintf("%s.%s", schema.Name, table.Name)

				createdAsset, warning := createAsset(ctx, assetsPath, schema.Name, table.Name, assetType, conn, fillColumns, table)
				if warning != "" {
					warnings = append(warnings, importWarning{tableName: fullName, message: warning})
				}
				if createdAsset == nil {
					continue
				}

				assetName := fmt.Sprintf("%s.%s", strings.ToLower(schema.Name), strings.ToLower(table.Name))
				if existingAssets[assetName] == nil {
					schemaFolder := filepath.Join(assetsPath, strings.ToLower(schema.Name))
					if mkErr := fs.MkdirAll(schemaFolder, 0o755); mkErr != nil {
						return importCompleteMsg{err: fmt.Errorf("failed to create directory %s: %w", schemaFolder, mkErr)}
					}
					if pErr := createdAsset.Persist(fs); pErr != nil {
						return importCompleteMsg{err: pErr}
					}
					existingAssets[assetName] = createdAsset
					totalTables++
				} else {
					existingAsset := existingAssets[assetName]
					existingColumns := make(map[string]pipeline.Column, len(existingAsset.Columns))
					for _, column := range existingAsset.Columns {
						existingColumns[column.Name] = column
					}
					for _, c := range createdAsset.Columns {
						if _, ok := existingColumns[c.Name]; !ok {
							existingAsset.Columns = append(existingAsset.Columns, c)
						}
					}
					if pErr := existingAsset.Persist(fs); pErr != nil {
						return importCompleteMsg{err: pErr}
					}
					mergedTableCount++
				}
			}
		}

		return importCompleteMsg{
			importedCount: totalTables,
			mergedCount:   mergedTableCount,
			warnings:      warnings,
		}
	}
}

func runImportDatabaseTUI(ctx context.Context, pipelinePath, environment, configFile string, fillColumns bool) error {
	fs := afero.NewOsFs()

	repoRoot, err := git.FindRepoFromPath(".")
	if err != nil {
		return fmt.Errorf("failed to find git repository root: %w", err)
	}

	if configFile == "" {
		configFile = filepath.Join(repoRoot.Path, ".bruin.yml")
	}

	cfg, err := config.LoadOrCreate(fs, configFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if environment != "" {
		if envErr := cfg.SelectEnvironment(environment); envErr != nil {
			return fmt.Errorf("failed to select environment '%s': %w", environment, envErr)
		}
	}

	if cfg.SelectedEnvironment == nil || cfg.SelectedEnvironment.Connections == nil {
		return errors.New("no environment selected or no connections configured")
	}

	connSummary := cfg.SelectedEnvironment.Connections.ConnectionsSummaryList()

	var connItems []importConnectionItem
	for name, connType := range connSummary {
		if supportedImportConnectionTypes[connType] {
			connItems = append(connItems, importConnectionItem{name: name, connType: connType})
		}
	}

	if len(connItems) == 0 {
		return errors.New("no database connections found that support import")
	}

	sort.Slice(connItems, func(i, j int) bool {
		return connItems[i].name < connItems[j].name
	})

	listItems := make([]list.Item, len(connItems))
	for i, c := range connItems {
		listItems[i] = c
	}

	delegate := list.NewDefaultDelegate()
	connList := list.New(listItems, delegate, 80, 20)
	connList.Title = "Select Connection"
	connList.SetShowStatusBar(true)
	connList.SetFilteringEnabled(true)
	connList.Styles.Title = dbtuiHeaderStyle

	model := &importDatabaseModel{
		ctx:          ctx,
		pipelinePath: pipelinePath,
		environment:  environment,
		configFile:   configFile,
		fillColumns:  fillColumns,
		step:         dbtuiStepConnection,
		cfg:          cfg,
		connList:     connList,
		connItems:    connItems,
	}

	p := tea.NewProgram(model, tea.WithAltScreen())
	finalModel, err := p.Run()
	if err != nil {
		return fmt.Errorf("TUI error: %w", err)
	}

	fm := finalModel.(*importDatabaseModel)

	if fm.quitting && fm.step != dbtuiStepDone {
		return nil
	}

	if fm.importError != nil {
		return fm.importError
	}

	if fm.importDone {
		successStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(colorSuccess)).Bold(true)
		fmt.Println(successStyle.Render(fmt.Sprintf("✓ Imported %d tables, merged %d from '%s' into '%s'",
			fm.importedCount, fm.mergedCount, fm.selectedConnName, pipelinePath)))

		if len(fm.importWarnings) > 0 {
			fmt.Printf("\nWarnings (%d):\n", len(fm.importWarnings))
			for _, w := range fm.importWarnings {
				warningPrinter.Printf("  - %s: %s\n", w.tableName, w.message)
			}
		}
	}

	return nil
}
