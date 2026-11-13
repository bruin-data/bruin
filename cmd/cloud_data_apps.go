package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/bruin-data/bruin/pkg/bruincloud"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/urfave/cli/v3"
)

// The three files an app is made of, as pull writes them and push --dir reads them,
// plus the small state file that remembers which app and version they came from.
const (
	dataAppHTMLFile    = "data-app.html"
	dataAppQueriesFile = "data-app.queries.json"
	dataAppHTTPFile    = "data-app.http.json"
	dataAppStateFile   = ".bruin-data-app.json"
)

type dataAppState struct {
	ID      int    `json:"id"`
	Version string `json:"version"`
}

func CloudDataApps() *cli.Command {
	return &cli.Command{
		Name:  "data-apps",
		Usage: "Build Bruin Cloud data apps (start with 'bruin cloud data-apps guide')",
		Commands: []*cli.Command{
			cloudDataAppsGuide(),
			cloudDataAppsList(),
			cloudDataAppsSettings(),
			cloudDataAppsCreate(),
			cloudDataAppsGet(),
			cloudDataAppsPull(),
			cloudDataAppsPush(),
			cloudDataAppsUpdate(),
			cloudDataAppsDelete(),
		},
	}
}

func dataAppIDFlag() *cli.IntFlag {
	return &cli.IntFlag{Name: "data-app-id", Usage: "data app ID", Required: true}
}

// dataAppID reads --data-app-id, rejecting a non-positive one.
func dataAppID(c *cli.Command, output string) (int, bool) {
	id := c.Int("data-app-id")
	if id <= 0 {
		printError(fmt.Errorf("--data-app-id must be a positive integer, got %d", id), output, "Invalid --data-app-id")
		return 0, false
	}
	return id, true
}

func printDataAppGuideRef(ref *bruincloud.DataAppGuideRef) {
	if ref != nil && ref.URL != "" {
		infoPrinter.Printf("Before building or changing a data app, read the guide: bruin cloud data-apps guide (version %s)\n", ref.Version)
	}
}

func printDataAppJSON(v any) {
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(data))
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

func cloudDataAppsGuide() *cli.Command {
	return &cli.Command{
		Name:  "guide",
		Usage: "Print the guide (a skill) a coding agent follows to build a data app",
		Flags: []cli.Flag{apiKeyFlag(), outputFlag()},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			guide, err := client.GetDataAppGuide(ctx)
			if err != nil {
				printError(err, output, "Failed to get the data app guide")
				return cli.Exit("", 1)
			}

			if output == "json" {
				printDataAppJSON(guide)
				return nil
			}

			fmt.Println(guide.Content)
			return nil
		},
	}
}

func cloudDataAppsList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List the data apps you can view",
		Flags: []cli.Flag{apiKeyFlag(), outputFlag()},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			apps, err := client.ListDataApps(ctx)
			if err != nil {
				printError(err, output, "Failed to list data apps")
				return cli.Exit("", 1)
			}

			if output == "json" {
				printDataAppJSON(apps)
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"ID", "Title", "Agent ID", "Visibility", "Updated At"})
			for _, a := range apps {
				agent := "-"
				if a.AgentID != nil {
					agent = fmt.Sprint(*a.AgentID)
				}
				t.AppendRow(table.Row{a.ID, deref(a.Title), agent, a.Visibility, deref(a.UpdatedAt)})
			}
			t.Render()
			return nil
		},
	}
}

func cloudDataAppsSettings() *cli.Command {
	return &cli.Command{
		Name:  "settings",
		Usage: "Show what the team allows its data apps (read-only mode, approved domains)",
		Flags: []cli.Flag{apiKeyFlag(), outputFlag()},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			settings, err := client.GetDataAppSettings(ctx)
			if err != nil {
				printError(err, output, "Failed to get data app settings")
				return cli.Exit("", 1)
			}

			if output == "json" {
				printDataAppJSON(settings)
				return nil
			}

			fmt.Printf("Read-only warehouse queries: %t\n", settings.ReadOnly)
			if !settings.RestrictCredentialHosts {
				fmt.Println("Credentials can be sent to any domain.")
				return nil
			}
			if len(settings.AllowedHosts) == 0 {
				fmt.Println("Credentials can't be sent to any domain yet: a team admin approves domains in AI settings → Data Apps.")
				return nil
			}
			fmt.Printf("Credentials can only be sent to: %s\n", strings.Join(settings.AllowedHosts, ", "))
			return nil
		},
	}
}

func cloudDataAppsCreate() *cli.Command {
	return &cli.Command{
		Name:  "create",
		Usage: "Create an empty data app",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.StringFlag{Name: "title", Usage: "the app's title"},
			&cli.IntFlag{Name: "agent-id", Usage: "the agent whose connections the app's queries run on (can be set later with update)"},
			&cli.StringFlag{Name: "visibility", Usage: "private or team (default: private)"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			visibility := c.String("visibility")
			if visibility != "" && visibility != "team" && visibility != "private" {
				printError(fmt.Errorf("visibility must be 'team' or 'private', got %q", visibility), output, "Invalid --visibility")
				return cli.Exit("", 1)
			}
			if c.IsSet("agent-id") && c.Int("agent-id") <= 0 {
				printError(fmt.Errorf("--agent-id must be a positive integer, got %d", c.Int("agent-id")), output, "Invalid --agent-id")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			app, err := client.CreateDataApp(ctx, c.String("title"), c.Int("agent-id"), visibility)
			if err != nil {
				printError(err, output, "Failed to create data app")
				return cli.Exit("", 1)
			}

			if output == "json" {
				printDataAppJSON(app)
				return nil
			}

			successPrinter.Printf("Created data app %d", app.ID)
			if title := deref(app.Title); title != "" {
				successPrinter.Printf(" (%s)", title)
			}
			fmt.Println()
			if app.AgentID == nil {
				warningPrinter.Println("It has no agent yet, so its queries can't run: set one with 'bruin cloud data-apps update --agent-id'.")
			}
			printDataAppGuideRef(app.Guide)
			return nil
		},
	}
}

func cloudDataAppsGet() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Usage: "Show a data app: its agent, version, and the queries and requests it declares",
		Flags: []cli.Flag{apiKeyFlag(), outputFlag(), dataAppIDFlag()},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")
			id, ok := dataAppID(c, output)
			if !ok {
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			app, err := client.GetDataApp(ctx, id)
			if err != nil {
				printError(err, output, "Failed to get data app")
				return cli.Exit("", 1)
			}

			if output == "json" {
				printDataAppJSON(app)
				return nil
			}

			agent := "none (set one with 'bruin cloud data-apps update --agent-id')"
			if app.AgentID != nil {
				agent = fmt.Sprint(*app.AgentID)
			}
			fmt.Printf("ID:         %d\nTitle:      %s\nAgent:      %s\nVisibility: %s\nUpdated:    %s\nVersion:    %s\n",
				app.ID, deref(app.Title), agent, app.Visibility, deref(app.UpdatedAt), app.Version)
			fmt.Printf("HTML:       %d bytes\n", len(deref(app.Code)))
			fmt.Printf("Queries:    %s\n", strings.Join(manifestNames(app.Queries), ", "))
			fmt.Printf("Requests:   %s\n", strings.Join(manifestNames(app.HTTPRequests), ", "))
			return nil
		},
	}
}

// manifestNames lists the declared names in a manifest, or "-" for none.
func manifestNames(raw json.RawMessage) []string {
	var entries []struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(raw, &entries)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name)
	}
	if len(names) == 0 {
		return []string{"-"}
	}
	return names
}

func cloudDataAppsPull() *cli.Command {
	return &cli.Command{
		Name:  "pull",
		Usage: "Write a data app's HTML and manifests into a folder, to edit and push back",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			dataAppIDFlag(),
			&cli.StringFlag{Name: "dir", Usage: "folder to write into (default: the current folder)", Value: "."},
			&cli.BoolFlag{Name: "force", Usage: "overwrite local files that differ from the app"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")
			id, ok := dataAppID(c, output)
			if !ok {
				return cli.Exit("", 1)
			}
			dir := c.String("dir")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			app, err := client.GetDataApp(ctx, id)
			if err != nil {
				printError(err, output, "Failed to get data app")
				return cli.Exit("", 1)
			}

			files := map[string][]byte{
				dataAppHTMLFile:    []byte(deref(app.Code)),
				dataAppQueriesFile: indentManifest(app.Queries),
				dataAppHTTPFile:    indentManifest(app.HTTPRequests),
			}

			// Don't silently throw away local edits: a pull over changed files needs --force.
			if !c.Bool("force") {
				var changed []string
				for name, content := range files {
					existing, err := os.ReadFile(filepath.Join(dir, name))
					if err == nil && !sameDataAppFile(name, existing, content) {
						changed = append(changed, name)
					}
				}
				if len(changed) > 0 {
					printError(fmt.Errorf("%s in %s differ from the app; pass --force to overwrite them", strings.Join(changed, ", "), dir), output, "Local changes would be lost")
					return cli.Exit("", 1)
				}
			}

			if err := os.MkdirAll(dir, 0o755); err != nil {
				printError(err, output, "Failed to create --dir")
				return cli.Exit("", 1)
			}
			for name, content := range files {
				if err := os.WriteFile(filepath.Join(dir, name), content, 0o644); err != nil {
					printError(err, output, "Failed to write "+name)
					return cli.Exit("", 1)
				}
			}
			if err := writeDataAppState(dir, dataAppState{ID: app.ID, Version: app.Version}); err != nil {
				printError(err, output, "Failed to write "+dataAppStateFile)
				return cli.Exit("", 1)
			}

			if output == "json" {
				printDataAppJSON(map[string]any{"id": app.ID, "version": app.Version, "dir": dir})
				return nil
			}

			successPrinter.Printf("Pulled data app %d (version %s) into %s\n", app.ID, app.Version, dir)
			printDataAppGuideRef(app.Guide)
			return nil
		},
	}
}

// sameDataAppFile reports whether a local file already holds what pull would write. The
// manifests are compared as JSON, so reformatting one isn't mistaken for an edit.
func sameDataAppFile(name string, local, pulled []byte) bool {
	if bytes.Equal(local, pulled) {
		return true
	}
	if name == dataAppHTMLFile {
		return false
	}
	var a, b any
	if json.Unmarshal(local, &a) != nil || json.Unmarshal(pulled, &b) != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}

// indentManifest pretty-prints a manifest for editing; an absent one becomes [].
func indentManifest(raw json.RawMessage) []byte {
	if len(raw) == 0 || string(raw) == "null" {
		return []byte("[]\n")
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		return append(raw, '\n')
	}
	buf.WriteByte('\n')
	return buf.Bytes()
}

func readDataAppState(dir string) (*dataAppState, error) {
	data, err := os.ReadFile(filepath.Join(dir, dataAppStateFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var state dataAppState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", dataAppStateFile, err)
	}
	return &state, nil
}

func writeDataAppState(dir string, state dataAppState) error {
	data, _ := json.MarshalIndent(state, "", "  ")
	return os.WriteFile(filepath.Join(dir, dataAppStateFile), append(data, '\n'), 0o644)
}

// readManifest reads a manifest file and checks it is a JSON array.
func readManifest(path string) (json.RawMessage, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("%s must be a JSON array: %w", path, err)
	}
	return json.RawMessage(bytes.TrimSpace(data)), nil
}

func cloudDataAppsPush() *cli.Command {
	return &cli.Command{
		Name:  "push",
		Usage: "Save a data app's HTML and manifests to Bruin Cloud",
		Description: "Point at the folder pull wrote (--dir), or at the files directly (--html, --queries, --http).\n" +
			"Only the files you give are saved; the others stay as they are in Bruin Cloud.\n" +
			"After a pull, the push fails if someone saved the app since; pull again, re-apply\n" +
			"your change and push, or pass --force to overwrite their change.",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			dataAppIDFlag(),
			&cli.StringFlag{Name: "dir", Usage: "folder holding data-app.html, data-app.queries.json and data-app.http.json (as written by pull)"},
			&cli.StringFlag{Name: "html", Usage: "the app's HTML file"},
			&cli.StringFlag{Name: "queries", Usage: "the queries manifest (a JSON array)"},
			&cli.StringFlag{Name: "http", Usage: "the HTTP requests manifest (a JSON array)"},
			&cli.StringFlag{Name: "expected-version", Usage: "fail if the app is no longer at this version (defaults to the version pull recorded in --dir)"},
			&cli.BoolFlag{Name: "force", Usage: "save even if the app changed since your pull"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")
			id, ok := dataAppID(c, output)
			if !ok {
				return cli.Exit("", 1)
			}

			dir := c.String("dir")
			htmlPath, queriesPath, httpPath := c.String("html"), c.String("queries"), c.String("http")
			if dir != "" && (htmlPath != "" || queriesPath != "" || httpPath != "") {
				printError(errors.New("pass --dir or the files (--html, --queries, --http), not both"), output, "Ambiguous input")
				return cli.Exit("", 1)
			}
			if dir == "" && htmlPath == "" && queriesPath == "" && httpPath == "" {
				printError(errors.New("pass --dir, or at least one of --html, --queries, --http"), output, "Nothing to push")
				return cli.Exit("", 1)
			}

			var state *dataAppState
			if dir != "" {
				var err error
				if state, err = readDataAppState(dir); err != nil {
					printError(err, output, "Failed to read "+dataAppStateFile)
					return cli.Exit("", 1)
				}
				if state != nil && state.ID != id {
					printError(fmt.Errorf("%s was pulled from data app %d, not %d", dir, state.ID, id), output, "Wrong data app")
					return cli.Exit("", 1)
				}
				// With --dir, every one of the three files present is pushed.
				for _, f := range []struct {
					name string
					dst  *string
				}{{dataAppHTMLFile, &htmlPath}, {dataAppQueriesFile, &queriesPath}, {dataAppHTTPFile, &httpPath}} {
					if _, err := os.Stat(filepath.Join(dir, f.name)); err == nil {
						*f.dst = filepath.Join(dir, f.name)
					}
				}
				if htmlPath == "" && queriesPath == "" && httpPath == "" {
					printError(fmt.Errorf("%s has none of %s, %s, %s", dir, dataAppHTMLFile, dataAppQueriesFile, dataAppHTTPFile), output, "Nothing to push")
					return cli.Exit("", 1)
				}
			}

			var input bruincloud.DataAppSaveInput
			if htmlPath != "" {
				data, err := os.ReadFile(htmlPath)
				if err != nil {
					printError(err, output, "Failed to read --html")
					return cli.Exit("", 1)
				}
				if len(bytes.TrimSpace(data)) == 0 {
					printError(fmt.Errorf("%s is empty", htmlPath), output, "Invalid --html")
					return cli.Exit("", 1)
				}
				code := string(data)
				input.Code = &code
			}
			for _, m := range []struct {
				path string
				dst  *json.RawMessage
				flag string
			}{{queriesPath, &input.Queries, "--queries"}, {httpPath, &input.HTTPRequests, "--http"}} {
				if m.path == "" {
					continue
				}
				raw, err := readManifest(m.path)
				if err != nil {
					printError(err, output, "Invalid "+m.flag)
					return cli.Exit("", 1)
				}
				*m.dst = raw
			}

			if !c.Bool("force") {
				input.ExpectedVersion = c.String("expected-version")
				if input.ExpectedVersion == "" && state != nil {
					input.ExpectedVersion = state.Version
				}
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			result, err := client.SaveDataApp(ctx, id, input)
			if err != nil {
				printError(err, output, "Failed to push data app")
				return cli.Exit("", 1)
			}

			if dir != "" {
				if err := writeDataAppState(dir, dataAppState{ID: id, Version: result.Version}); err != nil {
					printError(err, output, "Pushed, but failed to update "+dataAppStateFile)
					return cli.Exit("", 1)
				}
			}

			if output == "json" {
				printDataAppJSON(result)
				return nil
			}

			successPrinter.Printf("Pushed data app %d (version %s)\n", id, result.Version)
			for _, w := range result.Warnings {
				warningPrinter.Printf("warning: %s\n", w)
			}
			return nil
		},
	}
}

func cloudDataAppsUpdate() *cli.Command {
	return &cli.Command{
		Name:  "update",
		Usage: "Rename a data app or set its agent (push saves the HTML and manifests)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			dataAppIDFlag(),
			&cli.StringFlag{Name: "title", Usage: "the new title"},
			&cli.IntFlag{Name: "agent-id", Usage: "the agent whose connections the app's queries run on (the app's creator or an admin only)"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")
			id, ok := dataAppID(c, output)
			if !ok {
				return cli.Exit("", 1)
			}

			fields := map[string]any{}
			if c.IsSet("title") {
				fields["title"] = c.String("title")
			}
			if c.IsSet("agent-id") {
				if c.Int("agent-id") <= 0 {
					printError(fmt.Errorf("--agent-id must be a positive integer, got %d", c.Int("agent-id")), output, "Invalid --agent-id")
					return cli.Exit("", 1)
				}
				fields["agent_id"] = c.Int("agent-id")
			}
			if len(fields) == 0 {
				printError(errors.New("pass --title and/or --agent-id"), output, "Nothing to update")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			app, err := client.UpdateDataApp(ctx, id, fields)
			if err != nil {
				printError(err, output, "Failed to update data app")
				return cli.Exit("", 1)
			}

			if output == "json" {
				printDataAppJSON(app)
				return nil
			}

			successPrinter.Printf("Updated data app %d\n", app.ID)
			return nil
		},
	}
}

func cloudDataAppsDelete() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete a data app (its creator or an admin only)",
		Flags: []cli.Flag{apiKeyFlag(), outputFlag(), dataAppIDFlag()},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")
			id, ok := dataAppID(c, output)
			if !ok {
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			if err := client.DeleteDataApp(ctx, id); err != nil {
				printError(err, output, "Failed to delete data app")
				return cli.Exit("", 1)
			}

			if output == "json" {
				fmt.Println(`{"success": true}`)
				return nil
			}

			successPrinter.Printf("Deleted data app %d.\n", id)
			return nil
		},
	}
}
