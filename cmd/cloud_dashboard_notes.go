package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/pkg/bruincloud"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/urfave/cli/v3"
)

func cloudDashboardsNotes() *cli.Command {
	return &cli.Command{
		Name:  "notes",
		Usage: "List and create dashboard notes",
		Commands: []*cli.Command{
			cloudDashboardNotesList(),
			cloudDashboardNotesCreate(),
		},
	}
}

func cloudDashboardNotesList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List dashboard notes",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{Name: "dashboard-id", Usage: "dashboard ID", Required: true},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")
			id := c.Int("dashboard-id")
			if id <= 0 {
				printError(errors.New("dashboard-id must be a positive integer"), output, "Invalid dashboard")
				return cli.Exit("", 1)
			}
			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}
			result, err := client.ListDashboardNotes(ctx, id)
			if err != nil {
				printError(err, output, "Failed to list dashboard notes")
				return cli.Exit("", 1)
			}
			if output == "json" {
				var response map[string]json.RawMessage
				if err := json.Unmarshal(result, &response); err != nil {
					printError(err, output, "Failed to parse dashboard notes")
					return cli.Exit("", 1)
				}
				data, _ := json.MarshalIndent(response, "", "  ")
				fmt.Println(string(data))
				return nil
			}
			var response struct {
				Notes          []bruincloud.DashboardNote `json:"notes"`
				DashboardModel *string                    `json:"dashboard_model"`
				ModelContexts  json.RawMessage            `json:"model_contexts"`
				Filters        []json.RawMessage          `json:"filters"`
			}
			if err := json.Unmarshal(result, &response); err != nil {
				printError(err, output, "Failed to parse dashboard notes")
				return cli.Exit("", 1)
			}
			models, err := parseDashboardNoteModelContexts(response.ModelContexts)
			if err != nil {
				printError(err, output, "Failed to parse dashboard notes")
				return cli.Exit("", 1)
			}
			printDashboardNotes(response.Notes)
			fmt.Println()
			printDashboardNoteDimensions(models, response.DashboardModel, noteFilters(response.Filters))
			return nil
		},
	}
}

func printDashboardNotes(notes []bruincloud.DashboardNote) {
	if len(notes) == 0 {
		infoPrinter.Println("No notes found for this dashboard.")
		return
	}
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"ID", "Model", "Scope", "Content", "Author", "Created At"})
	for _, note := range notes {
		model, scope, author := "-", "-", "-"
		if note.SemanticModel != nil {
			model = *note.SemanticModel
		}
		if note.Author != nil {
			author = *note.Author
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, note.Dimensions); err == nil {
			switch compact.String() {
			case "null", "[]", "{}":
			default:
				scope = compact.String()
			}
		}
		t.AppendRow(table.Row{note.ID, model, scope, note.Content, author, note.CreatedAt})
	}
	t.Render()
}

// printDashboardNoteDimensions lists the names accepted by `notes create --dimensions`:
// dimensions of each model used by the dashboard (for --model), and, when the
// dashboard has no model of its own, the filters Cloud matches without --model.
func printDashboardNoteDimensions(models map[string]bruincloud.DashboardNoteModelContext, dashboardModel *string, filters []bruincloud.DashboardNoteDimension) {
	hasDashboardModel := dashboardModel != nil && *dashboardModel != ""
	showFilters := !hasDashboardModel && len(filters) > 0
	if len(models) == 0 && !showFilters {
		infoPrinter.Println("No dimensions or filters available for notes on this dashboard.")
		return
	}
	if len(models) > 0 {
		names := make([]string, 0, len(models))
		for name := range models {
			names = append(names, name)
		}
		sort.Strings(names)
		infoPrinter.Println("Available dimensions for --model and --dimensions:")
		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"Model", "Dimension", "Type"})
		for _, name := range names {
			label := name
			if hasDashboardModel && *dashboardModel == name {
				label += " (default)"
			}
			dimensions := models[name].Dimensions
			if len(dimensions) == 0 {
				t.AppendRow(table.Row{label, "-", "-"})
			}
			for _, dimension := range dimensions {
				t.AppendRow(table.Row{label, dimension.Name, dimension.Type})
			}
		}
		t.Render()
	}
	if showFilters {
		if len(models) > 0 {
			fmt.Println()
		}
		infoPrinter.Println("Available filters for --dimensions without --model:")
		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"Filter", "Type"})
		for _, filter := range filters {
			t.AppendRow(table.Row{filter.Name, filter.Type})
		}
		t.Render()
	}
}

// noteFilters keeps the dashboard filters Cloud accepts as note dimensions and
// skips entries it ignores, so one malformed filter doesn't break the listing.
func noteFilters(raw []json.RawMessage) []bruincloud.DashboardNoteDimension {
	supported := map[string]bool{"select": true, "text": true, "number": true, "boolean": true, "date": true}
	filters := make([]bruincloud.DashboardNoteDimension, 0, len(raw))
	for _, entry := range raw {
		var filter struct {
			Name     any `json:"name"`
			Type     any `json:"type"`
			Multiple any `json:"multiple"`
		}
		if json.Unmarshal(entry, &filter) != nil {
			continue
		}
		name, _ := filter.Name.(string)
		filterType, _ := filter.Type.(string)
		if strings.TrimSpace(name) == "" || !supported[filterType] {
			continue
		}
		// Cloud only treats a literal true as multi-select.
		if filter.Multiple == true {
			filterType += " (multiple)"
		}
		filters = append(filters, bruincloud.DashboardNoteDimension{Name: strings.TrimSpace(name), Type: filterType})
	}
	return filters
}

// parseDashboardNoteModelContexts decodes the model_contexts map. Cloud
// serializes an empty map as an empty JSON array.
func parseDashboardNoteModelContexts(data json.RawMessage) (map[string]bruincloud.DashboardNoteModelContext, error) {
	var models map[string]bruincloud.DashboardNoteModelContext
	if len(data) == 0 || string(data) == "[]" || string(data) == "null" {
		return models, nil
	}
	if err := json.Unmarshal(data, &models); err != nil {
		return nil, fmt.Errorf("failed to parse dashboard model contexts: %w", err)
	}
	return models, nil
}

func cloudDashboardNotesCreate() *cli.Command {
	return &cli.Command{
		Name:  "create",
		Usage: "Create a dashboard note (requires a user-scoped token)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{Name: "dashboard-id", Usage: "dashboard ID", Required: true},
			&cli.StringFlag{Name: "content", Usage: "note content", Required: true},
			&cli.StringFlag{Name: "model", Usage: "semantic model used by the dashboard (defaults to the dashboard model)"},
			&cli.StringFlag{Name: "dimensions", Usage: "dimension values as a JSON object (scalar or array values)"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")
			id := c.Int("dashboard-id")
			if id <= 0 {
				printError(errors.New("dashboard-id must be a positive integer"), output, "Invalid dashboard")
				return cli.Exit("", 1)
			}
			if strings.TrimSpace(c.String("content")) == "" {
				printError(errors.New("content must not be empty"), output, "Invalid content")
				return cli.Exit("", 1)
			}
			fields := map[string]any{"content": c.String("content")}
			model := strings.TrimSpace(c.String("model"))
			if c.IsSet("model") {
				if model == "" {
					printError(errors.New("model must not be empty"), output, "Invalid model")
					return cli.Exit("", 1)
				}
				fields["semantic_model"] = model
			}
			var dimensions map[string]json.RawMessage
			if c.IsSet("dimensions") {
				if err := json.Unmarshal([]byte(c.String("dimensions")), &dimensions); err != nil || dimensions == nil {
					printError(errors.New("dimensions must be a JSON object"), output, "Invalid dimensions")
					return cli.Exit("", 1)
				}
				fields["dimensions"] = dimensions
			}
			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}
			// Without --model the server validates against the dashboard model or,
			// when it has none, the dashboard filters of any writable state.
			// The model pre-check reads the list endpoint, which needs the
			// dashboard:list ability; a token with only dashboard:update skips it.
			if model != "" {
				data, err := client.ListDashboardNotes(ctx, id)
				var apiErr *bruincloud.APIError
				switch {
				case errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusForbidden:
				case err != nil:
					printError(err, output, "Failed to get dashboard note context")
					return cli.Exit("", 1)
				default:
					if err := validateDashboardNoteContext(data, model); err != nil {
						printError(err, output, "Invalid note context")
						return cli.Exit("", 1)
					}
				}
			}
			note, err := client.CreateDashboardNote(ctx, id, fields)
			if err != nil {
				printError(err, output, "Failed to create dashboard note")
				return cli.Exit("", 1)
			}
			if output == "json" {
				var data bytes.Buffer
				if err := json.Indent(&data, note, "", "  "); err != nil {
					printError(err, output, "Failed to parse dashboard note")
					return cli.Exit("", 1)
				}
				fmt.Println(data.String())
				return nil
			}
			var created bruincloud.DashboardNote
			if err := json.Unmarshal(note, &created); err != nil {
				printError(err, output, "Failed to parse dashboard note")
				return cli.Exit("", 1)
			}
			successPrinter.Printf("Created note %d.\n", created.ID)
			return nil
		},
	}
}

// validateDashboardNoteContext checks only the model name.
func validateDashboardNoteContext(data json.RawMessage, model string) error {
	var response struct {
		ModelContexts json.RawMessage `json:"model_contexts"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return fmt.Errorf("failed to parse dashboard note context: %w", err)
	}
	models, err := parseDashboardNoteModelContexts(response.ModelContexts)
	if err != nil {
		return err
	}
	if _, ok := models[model]; !ok {
		names := make([]string, 0, len(models))
		for name := range models {
			names = append(names, name)
		}
		return fmt.Errorf("model %q is not used by this dashboard; available models: %s", model, dashboardNoteContextNames(names))
	}
	return nil
}

func dashboardNoteContextNames(names []string) string {
	if len(names) == 0 {
		return "(none)"
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
