package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/bruin-data/bruin/pkg/date"
	"github.com/bruin-data/bruin/pkg/devenv"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

// runRenderStdin implements `bruin render --stdin`: it renders the SQL on
// standard input the way `bruin query --query` would run it on the connection.
// There is no asset, so no materialization or hooks are applied.
func runRenderStdin(ctx context.Context, c *cli.Command) error {
	output := c.String("output")

	w := c.Root().Writer
	rendered, err := renderStdin(ctx, c, afero.NewOsFs())
	if err != nil {
		// Callers such as the Python SDK read {"error": ...} from stdout.
		if output == "json" {
			if js, marshalErr := marshal(ErrorResponse{Error: err.Error()}); marshalErr == nil {
				fmt.Fprintln(w, string(js))
			}
		} else {
			printError(err, output, "Failed to render the query")
		}
		return cli.Exit("", 1)
	}

	if output != "json" {
		_, err = fmt.Fprintln(w, rendered)
		return err
	}

	js, err := json.Marshal(map[string]string{"query": rendered})
	if err != nil {
		return err
	}
	_, err = w.Write(js)
	return err
}

func renderStdin(ctx context.Context, c *cli.Command, fs afero.Fs) (string, error) {
	connectionName := c.String("connection")
	switch {
	case c.Args().Present():
		return "", errors.New("--stdin renders the SQL on standard input and cannot be combined with an asset path")
	case connectionName == "":
		return "", errors.New("--connection is required with --stdin")
	case c.String("variant") != "" || c.Bool("apply-interval-modifiers"):
		return "", errors.New("--variant and --apply-interval-modifiers only apply to asset files, not to --stdin")
	}

	vars, err := parseRenderStdinVars(c.StringSlice("var"))
	if err != nil {
		return "", err
	}
	startDate, err := date.ParseTime(c.String("start-date"))
	if err != nil {
		return "", errors.Errorf("invalid --start-date %q, use YYYY-MM-DD, YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff", c.String("start-date"))
	}
	endDate, err := date.ParseTime(c.String("end-date"))
	if err != nil {
		return "", errors.Errorf("invalid --end-date %q, use YYYY-MM-DD, YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff", c.String("end-date"))
	}

	input, err := io.ReadAll(c.Root().Reader)
	if err != nil {
		return "", errors.Wrap(err, "failed to read the SQL from standard input")
	}
	if strings.TrimSpace(string(input)) == "" {
		return "", errors.New("no SQL given on standard input")
	}

	// Resolve the connection the way direct-mode `bruin query` does. A
	// connection marked use_cloud: true may have no local credentials, so it is
	// detected before the connection manager is built.
	env, configFile := c.String("environment"), c.String("config-file")
	cm, useCloud, err := connectionUsesCloud(fs, env, connectionName, configFile)
	if err != nil {
		return "", err
	}
	var conn any
	var dialect string
	if !useCloud {
		var connType string
		conn, connType, err = getConnectionAndTypeFromConfigWithContext(ctx, env, connectionName, fs, configFile)
		if err != nil {
			return "", err
		}
		dialect = sqlparser.ConnectionTypeToDialect(connType)
	}

	renderer := newQueryRenderer(startDate, endDate, directQueryPipelineName, vars, "")
	// `bruin query` only exposes variables at the top level. Asset SQL reads
	// pipeline variables as {{ var.name }}, so offer both.
	renderer.SetContextValue("var", vars)
	renderer.SetContextValue("full_refresh", c.Bool("full-refresh"))
	this := c.String("this")
	if this != "" {
		renderer.SetContextValue("this", this)
	}
	sql, err := renderDirectQuery(string(input), renderer, cm.SelectedEnvironment)
	if err != nil {
		return "", err
	}

	schemaPrefix := ""
	if cm.SelectedEnvironment != nil {
		schemaPrefix = cm.SelectedEnvironment.SchemaPrefix
	}
	// Like `bruin query`, tables are only rewritten on local connections: the
	// rewrite checks which prefixed tables exist on the database.
	if schemaPrefix == "" || useCloud {
		return sql, nil
	}

	return prefixStdinQuery(ctx, sql, dialect, schemaPrefix, this, connectionName, conn)
}

// prefixStdinQuery applies the schema prefix exactly like `bruin query`, then
// points references to the asset's own table, if one is given, at its prefixed
// name the way `bruin run` does.
func prefixStdinQuery(ctx context.Context, sql, dialect, schemaPrefix, this, connectionName string, conn any) (string, error) {
	parser, err := sqlparser.NewSQLParser(false)
	if err != nil {
		return "", errors.Wrap(err, "failed to initialize SQL parser")
	}
	defer parser.Close()
	if err := parser.Start(); err != nil {
		return "", errors.Wrap(err, "failed to start SQL parser")
	}

	sql, err = applySchemaPrefix(ctx, sql, dialect, schemaPrefix, false, parser, connectionName, conn)
	if err != nil {
		return "", errors.Wrap(err, "failed to apply schema prefix")
	}
	if this == "" || dialect == "" {
		return sql, nil
	}

	sql, err = devenv.RenameSelfReference(parser, dialect, this, schemaPrefix, sql)
	if err != nil {
		return "", errors.Wrap(err, "failed to apply schema prefix")
	}
	return sql, nil
}

// parseRenderStdinVars parses --var for --stdin. Keys accept dot notation like
// `bruin query`. Values are decoded as JSON, as `bruin render` and `bruin run`
// do, so callers such as the Python SDK can pass typed values; a value that is
// not valid JSON is kept as a string.
func parseRenderStdinVars(rawVars []string) (map[string]any, error) {
	return parseVars(rawVars, parseJSONVarValue)
}

func parseJSONVarValue(value string) any {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.UseNumber()

	var parsed any
	if err := decoder.Decode(&parsed); err != nil {
		return value
	}
	if strings.TrimSpace(value[decoder.InputOffset():]) != "" {
		return value
	}
	return fromJSONNumbers(parsed)
}

// fromJSONNumbers converts decoded JSON numbers to int64 or float64. An integer
// too large for int64, such as an unsigned 64-bit ID, keeps its digits as a
// string so it renders exactly as given instead of being rounded by float64.
func fromJSONNumbers(value any) any {
	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		if !strings.ContainsAny(v.String(), ".eE") {
			return v.String()
		}
		if f, err := v.Float64(); err == nil {
			return f
		}
		return v.String()
	case []any:
		for i := range v {
			v[i] = fromJSONNumbers(v[i])
		}
	case map[string]any:
		for k := range v {
			v[k] = fromJSONNumbers(v[k])
		}
	}
	return value
}
