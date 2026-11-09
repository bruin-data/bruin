package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/date"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

type rawQueryRenderOptions struct {
	ConnectionName string
	Dialect        string
	SchemaPrefix   string
	StartDate      time.Time
	EndDate        time.Time
	FullRefresh    bool
	Vars           map[string]any
}

// runRawQueryRender renders SQL read from stdin through the same Jinja and
// schema-prefix pipeline `bruin query` uses. It intentionally does not create an
// asset, so materialization and hooks never apply.
func runRawQueryRender(ctx context.Context, c *cli.Command) error {
	output := c.String("output")
	fail := func(err error) error {
		printError(err, output, "Failed to render the query")
		return cli.Exit("", 1)
	}

	if inputPath := c.Args().Get(0); inputPath != "" {
		return fail(errors.New("--stdin cannot be combined with an asset path"))
	}

	connectionName := c.String("connection")
	if connectionName == "" {
		return fail(errors.New("--connection is required when rendering SQL from --stdin"))
	}

	sqlBytes, err := io.ReadAll(c.Reader)
	if err != nil {
		return fail(errors.Wrap(err, "failed to read the SQL query from stdin"))
	}
	sql := string(sqlBytes)
	if strings.TrimSpace(sql) == "" {
		return fail(errors.New("no SQL query provided on standard input"))
	}

	vars, err := parseTypedQueryVars(c.StringSlice("var"))
	if err != nil {
		return fail(err)
	}

	startDate, err := date.ParseTime(c.String("start-date"))
	if err != nil {
		return fail(errors.New("please give a valid start date: bruin render --stdin --start-date <start date>"))
	}
	endDate, err := date.ParseTime(c.String("end-date"))
	if err != nil {
		return fail(errors.New("please give a valid end date: bruin render --stdin --end-date <end date>"))
	}

	conn, dialect, schemaPrefix, err := resolveRawQueryRenderConnection(ctx, c, afero.NewOsFs())
	if err != nil {
		return fail(err)
	}

	rendered, err := renderRawQuerySQL(ctx, sql, conn, rawQueryRenderOptions{
		ConnectionName: connectionName,
		Dialect:        dialect,
		SchemaPrefix:   schemaPrefix,
		StartDate:      startDate,
		EndDate:        endDate,
		FullRefresh:    c.Bool("full-refresh"),
		Vars:           vars,
	})
	if err != nil {
		return fail(err)
	}

	return writeRawQueryRenderOutput(c.Writer, output, rendered)
}

// resolveRawQueryRenderConnection loads the selected environment and returns the
// connection, its SQL dialect and its schema prefix, mirroring direct-mode
// `bruin query`.
func resolveRawQueryRenderConnection(ctx context.Context, c *cli.Command, fs afero.Fs) (any, string, string, error) {
	env := c.String("environment")
	cm, configFilePath, err := loadQueryConfig(fs, env, c.String("config-file"))
	if err != nil {
		return nil, "", "", err
	}

	schemaPrefix := ""
	if cm.SelectedEnvironment != nil {
		schemaPrefix = cm.SelectedEnvironment.SchemaPrefix
	}

	ctx = context.WithValue(ctx, config.ConfigFilePathContextKey, configFilePath)
	ctx = context.WithValue(ctx, config.EnvironmentNameContextKey, cm.SelectedEnvironmentName)
	manager, errs := connectionManagerFromConfig(ctx, cm, makeLogger(false))
	if len(errs) > 0 {
		return nil, "", "", errors.Wrap(errs[0], "failed to create connection manager")
	}

	connectionName := c.String("connection")
	conn := manager.GetConnection(connectionName)
	if conn == nil {
		return nil, "", "", config.NewConnectionNotFoundError(ctx, "", connectionName)
	}

	return conn, sqlparser.ConnectionTypeToDialect(manager.GetConnectionType(connectionName)), schemaPrefix, nil
}

// renderRawQuerySQL renders Jinja first, then applies the environment schema
// prefix exactly like `bruin query` does for a direct query.
func renderRawQuerySQL(ctx context.Context, sql string, conn any, opts rawQueryRenderOptions) (string, error) {
	renderer := newQueryRenderer(opts.StartDate, opts.EndDate, "", opts.Vars, "")
	renderer.SetContextValue("schema_prefix", opts.SchemaPrefix)
	renderer.SetContextValue("full_refresh", opts.FullRefresh)

	rendered, err := renderer.Render(sql)
	if err != nil {
		return "", errors.Wrap(err, "failed to render the SQL query")
	}

	if opts.SchemaPrefix == "" {
		return rendered, nil
	}

	parser, err := sqlparser.NewSQLParser(false)
	if err != nil {
		return "", errors.Wrap(err, "failed to initialize the SQL parser for the schema prefix")
	}
	defer parser.Close()

	if err := parser.Start(); err != nil {
		return "", errors.Wrap(err, "failed to start the SQL parser for the schema prefix")
	}

	rendered, err = applySchemaPrefix(ctx, rendered, opts.Dialect, opts.SchemaPrefix, false, parser, opts.ConnectionName, conn)
	if err != nil {
		return "", errors.Wrap(err, "failed to apply the schema prefix")
	}

	return rendered, nil
}

func writeRawQueryRenderOutput(w io.Writer, output, rendered string) error {
	if output == "json" {
		payload, err := json.Marshal(map[string]string{"query": rendered})
		if err != nil {
			return errors.Wrap(err, "failed to marshal the rendered query")
		}
		if _, err := w.Write(payload); err != nil {
			return errors.Wrap(err, "failed to write the rendered query")
		}
		return nil
	}

	_, err := fmt.Fprintf(w, "%s\n", rendered)
	return err
}

// parseTypedQueryVars parses --var KEY=JSON, where every value is decoded as
// JSON when valid so SDK callers can pass JSON-encoded scalars, objects and
// arrays. Values that are not valid JSON stay literal strings.
func parseTypedQueryVars(rawVars []string) (map[string]any, error) {
	vars := make(map[string]any)

	for _, raw := range rawVars {
		key, value, found := strings.Cut(raw, "=")
		key = strings.TrimSpace(key)
		if !found || key == "" {
			return nil, fmt.Errorf("invalid variable %q: must be in key=value format", raw)
		}

		if err := setNestedVar(vars, key, parseTypedQueryVarValue(strings.TrimSpace(value))); err != nil {
			return nil, fmt.Errorf("invalid variable %q: %w", raw, err)
		}
	}

	return vars, nil
}

func parseTypedQueryVarValue(value string) any {
	if value == "" {
		return value
	}

	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.UseNumber()

	var parsed any
	if err := decoder.Decode(&parsed); err != nil {
		return value
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return value
	}

	return normalizeJSONNumbers(parsed)
}

// normalizeJSONNumbers turns JSON numbers into int64 or float64 so templates get
// "3" rather than "3.0" for integer values. json.Number otherwise leaks into
// template rendering.
func normalizeJSONNumbers(value any) any {
	switch typed := value.(type) {
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return integer
		}
		if float, err := typed.Float64(); err == nil {
			return float
		}
		return typed.String()
	case []any:
		for i, item := range typed {
			typed[i] = normalizeJSONNumbers(item)
		}
		return typed
	case map[string]any:
		for key, item := range typed {
			typed[key] = normalizeJSONNumbers(item)
		}
		return typed
	default:
		return value
	}
}
