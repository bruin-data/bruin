package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"golang.org/x/term"
)

// EcommerceChoices holds the user's stack selections.
type EcommerceChoices struct {
	Warehouse string
	Payments  string
	Marketing string
	Ads       []string
	Analytics string
}

type stepOption struct {
	ID    string
	Label string
}

type ecommerceStep struct {
	Title       string
	Description string
	Options     []stepOption
	MultiSelect bool
}

var ecommerceSteps = []ecommerceStep{
	{
		Title:       "Data Warehouse",
		Description: "Choose where your data will be stored",
		Options: []stepOption{
			{ID: warehouseClickHouse, Label: "ClickHouse - Column-oriented analytics database"},
			{ID: warehouseBigQuery, Label: "BigQuery - Google Cloud serverless warehouse"},
			{ID: warehouseSnowflake, Label: "Snowflake - Multi-cloud data warehouse"},
		},
	},
	{
		Title:       "Payments",
		Description: "Choose your payment processor",
		Options: []stepOption{
			{ID: paymentsShopifyPayment, Label: "Shopify Payments - Built-in Shopify payment processing"},
			{ID: paymentsStripe, Label: "Stripe - Independent payment platform"},
		},
	},
	{
		Title:       "Email Marketing",
		Description: "Choose your email marketing platform",
		Options: []stepOption{
			{ID: marketingKlaviyo, Label: "Klaviyo - Ecommerce email & SMS marketing"},
			{ID: marketingHubSpot, Label: "HubSpot - CRM & marketing automation"},
		},
	},
	{
		Title:       "Advertising",
		Description: "Select your ad platforms (you can choose multiple)",
		Options: []stepOption{
			{ID: adsFacebook, Label: "Facebook Ads - Meta advertising platform"},
			{ID: adsGoogle, Label: "Google Ads - Google advertising platform"},
			{ID: adsTikTok, Label: "TikTok Ads - TikTok advertising platform"},
		},
		MultiSelect: true,
	},
	{
		Title:       "Web Analytics",
		Description: "Choose your web analytics platform",
		Options: []stepOption{
			{ID: analyticsGA4, Label: "GA4 - Google Analytics 4"},
			{ID: analyticsMixpanel, Label: "Mixpanel - Product analytics"},
		},
	},
}

type ecommerceModel struct {
	step     int
	cursor   int
	selected map[int]bool // for multi-select (ads step)
	choices  EcommerceChoices
	quitting bool
	height   int
}

func newEcommerceModel() ecommerceModel {
	return ecommerceModel{
		selected: make(map[int]bool),
		height:   getTerminalHeight(),
	}
}

func (m ecommerceModel) Init() tea.Cmd {
	return tea.EnterAltScreen
}

func (m ecommerceModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.height = msg.Height
		return m, nil
	case tea.KeyMsg:
		currentStep := ecommerceSteps[m.step]
		switch msg.String() {
		case keyCtrlC, "q", "esc":
			m.quitting = true
			return m, tea.Quit
		case "enter":
			if currentStep.MultiSelect {
				// Collect selected items
				var selectedIDs []string
				for i, opt := range currentStep.Options {
					if m.selected[i] {
						selectedIDs = append(selectedIDs, opt.ID)
					}
				}
				if len(selectedIDs) == 0 {
					// Must select at least one
					return m, nil
				}
				m.choices.Ads = selectedIDs
			} else {
				opt := currentStep.Options[m.cursor]
				switch m.step {
				case 0:
					m.choices.Warehouse = opt.ID
				case 1:
					m.choices.Payments = opt.ID
				case 2:
					m.choices.Marketing = opt.ID
				case 4:
					m.choices.Analytics = opt.ID
				}
			}

			m.step++
			m.cursor = 0
			m.selected = make(map[int]bool)

			if m.step >= len(ecommerceSteps) {
				return m, tea.Quit
			}
			return m, nil
		case "down", "j":
			m.cursor++
			if m.cursor >= len(currentStep.Options) {
				m.cursor = 0
			}
		case "up", "k":
			m.cursor--
			if m.cursor < 0 {
				m.cursor = len(currentStep.Options) - 1
			}
		case " ":
			if currentStep.MultiSelect {
				m.selected[m.cursor] = !m.selected[m.cursor]
			}
		}
	}
	return m, nil
}

func (m ecommerceModel) View() string {
	if m.step >= len(ecommerceSteps) {
		return ""
	}

	s := strings.Builder{}
	step := ecommerceSteps[m.step]

	fmt.Fprintf(&s, "  Ecommerce Pipeline Setup (%d/%d)\n", m.step+1, len(ecommerceSteps))
	fmt.Fprintf(&s, "  %s\n", step.Title)
	fmt.Fprintf(&s, "  %s\n\n", step.Description)

	for i, opt := range step.Options {
		cursor := "  "
		if m.cursor == i {
			cursor = "> "
		}

		if step.MultiSelect {
			check := "[ ]"
			if m.selected[i] {
				check = "[x]"
			}
			fmt.Fprintf(&s, " %s%s %s\n", cursor, check, opt.Label)
		} else {
			radio := "( )"
			if m.cursor == i {
				radio = "(*)"
			}
			fmt.Fprintf(&s, " %s%s %s\n", cursor, radio, opt.Label)
		}
	}

	s.WriteString("\n")
	if step.MultiSelect {
		s.WriteString("  space = toggle, enter = confirm, q = quit\n")
	} else {
		s.WriteString("  enter = select, q = quit\n")
	}
	return s.String()
}

// runEcommerceStackPicker launches the interactive stack picker and returns the user's choices.
func runEcommerceStackPicker() (*EcommerceChoices, error) {
	if !term.IsTerminal(int(os.Stdout.Fd())) { //nolint:gosec // Fd() returns uintptr, safe to convert
		return nil, errors.New("ecommerce template requires an interactive terminal")
	}

	p := tea.NewProgram(newEcommerceModel())
	result, err := p.Run()
	if err != nil {
		return nil, fmt.Errorf("error running stack picker: %w", err)
	}

	m, ok := result.(ecommerceModel)
	if !ok || m.quitting {
		return nil, nil
	}

	if m.step < len(ecommerceSteps) {
		return nil, nil
	}

	return &m.choices, nil
}

// generateEcommerceTemplate generates all files for the ecommerce template based on user choices.
func generateEcommerceTemplate(basePath string, choices *EcommerceChoices) error {
	files, err := buildEcommerceFiles(choices)
	if err != nil {
		return err
	}

	for relPath, content := range files {
		fullPath := filepath.Join(basePath, relPath)
		dir := filepath.Dir(fullPath)

		if err := os.MkdirAll(dir, os.ModePerm); err != nil { //nolint:gosec
			return fmt.Errorf("could not create directory %s: %w", dir, err)
		}

		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil { //nolint:gosec
			return fmt.Errorf("could not write file %s: %w", fullPath, err)
		}
	}

	return nil
}
