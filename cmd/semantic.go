package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/bruin-data/bruin/pkg/semanticcheck"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	semantic "github.com/bruin-data/bruin/semantic-engine"
	"github.com/fatih/color"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

// Semantic groups the commands that operate on the repository-level semantic
// layer: validating models and running their quality checks.
func Semantic(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:  "semantic",
		Usage: "Validate semantic models and run their quality checks",
		Commands: []*cli.Command{
			semanticValidateCommand(isDebug),
			semanticCheckCommand(isDebug),
		},
	}
}

func semanticSharedFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    "environment",
			Aliases: []string{"e", "env"},
			Usage:   "the environment to use",
		},
		&cli.StringFlag{
			Name:    "config-file",
			Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
			Usage:   "the path to the .bruin.yml file",
		},
		&cli.StringFlag{
			Name:    "connection",
			Aliases: []string{"c"},
			Usage:   "connection to use for every model, overriding source.connection in the model files",
		},
		&cli.StringSliceFlag{
			Name:    "model",
			Aliases: []string{"m"},
			Usage:   "only include the given semantic model name, can be repeated",
		},
		&cli.StringFlag{
			Name:    "output",
			Aliases: []string{"o"},
			Usage:   "the output type, possible values are: plain, json",
			Value:   "plain",
		},
	}
}

func semanticValidateCommand(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "validate",
		Usage:     "validate semantic models and their quality checks, dry-running them on the warehouse when a connection is available",
		ArgsUsage: "[path to semantic directory]",
		Flags:     semanticSharedFlags(),
		Action: func(ctx context.Context, c *cli.Command) error {
			output := semanticOutput(c)
			if output == "json" {
				color.Output = io.Discard
			}

			catalog, err := loadSemanticCommandCatalog(ctx, c, *isDebug)
			if err != nil {
				printError(err, output, "Failed to load semantic models")
				return cli.Exit("", 1)
			}
			defer catalog.Close()

			report := validateSemanticCatalog(ctx, catalog)
			if output == "json" {
				if err := printSemanticJSON(report); err != nil {
					return err
				}
			} else {
				printSemanticValidationReport(report)
			}
			if !report.Valid {
				return cli.Exit("", 1)
			}
			return nil
		},
	}
}

func semanticCheckCommand(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "check",
		Aliases:   []string{"checks", "run-checks"},
		Usage:     "run the quality checks defined on semantic models against the warehouse",
		ArgsUsage: "[path to semantic directory]",
		Flags:     semanticSharedFlags(),
		Action: func(ctx context.Context, c *cli.Command) error {
			output := semanticOutput(c)
			if output == "json" {
				color.Output = io.Discard
			}

			catalog, err := loadSemanticCommandCatalog(ctx, c, *isDebug)
			if err != nil {
				printError(err, output, "Failed to load semantic models")
				return cli.Exit("", 1)
			}
			defer catalog.Close()

			report := runSemanticChecks(ctx, catalog)
			// A run that executes nothing should not look like a passing run in CI.
			if len(report.Results) == 0 && len(report.Errors) == 0 {
				printError(errors.New("the selected semantic models define no quality checks"), output, "No semantic quality checks to run")
				return cli.Exit("", 1)
			}
			if output == "json" {
				if err := printSemanticJSON(report); err != nil {
					return err
				}
			} else {
				printSemanticCheckReport(report)
			}
			if !report.Summary.OK() || len(report.Errors) > 0 {
				return cli.Exit("", 1)
			}
			return nil
		},
	}
}

func semanticOutput(c *cli.Command) string {
	return strings.ToLower(strings.TrimSpace(c.String("output")))
}

func printSemanticJSON(v interface{}) error {
	js, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}
	fmt.Println(string(js))
	return nil
}

// semanticCommandCatalog holds everything the semantic subcommands need: the
// loaded models, the ones that failed to load, and how to reach connections.
type semanticCommandCatalog struct {
	Path        string
	Models      map[string]*semantic.Model
	Invalid     map[string]error
	Selected    []string
	Connections *semanticConnectionResolver
	Prefixer    *semanticSchemaPrefixer
}

func (c *semanticCommandCatalog) Close() {
	c.Prefixer.Close()
}

func loadSemanticCommandCatalog(ctx context.Context, c *cli.Command, isDebug bool) (*semanticCommandCatalog, error) {
	pathArg := c.Args().Get(0)
	configFilePath, err := resolveSemanticConfigPath(c.String("config-file"), pathArg)
	if err != nil {
		return nil, err
	}

	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load the config file at '%s'", configFilePath)
	}
	if env := c.String("environment"); env != "" {
		if err := cm.SelectEnvironment(env); err != nil {
			return nil, errors.Wrapf(err, "failed to use the environment '%s'", env)
		}
	}

	semanticPath := pathArg
	if semanticPath == "" {
		semanticPath = filepath.Join(filepath.Dir(configFilePath), "semantic")
	}
	if abs, err := filepath.Abs(semanticPath); err == nil {
		semanticPath = abs
	}

	fs := afero.NewOsFs()
	// The loader treats a missing directory as "no models", which would make a
	// mistyped path silently report success. A path the user typed must exist;
	// the default '<repo>/semantic' may legitimately be absent.
	if pathArg != "" {
		info, err := fs.Stat(semanticPath)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read the semantic directory '%s'", semanticPath)
		}
		if !info.IsDir() {
			return nil, errors.Errorf("'%s' is not a directory; pass the directory that holds the semantic model files", semanticPath)
		}
	}

	models, invalid, err := semantic.LoadDirPartialFS(fs, semanticPath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load semantic models from '%s'", semanticPath)
	}

	selected, err := selectSemanticModels(models, invalid, c.StringSlice("model"))
	if err != nil {
		return nil, err
	}

	ctx = context.WithValue(ctx, config.ConfigFilePathContextKey, configFilePath)
	ctx = context.WithValue(ctx, config.EnvironmentNameContextKey, cm.SelectedEnvironmentName)

	return &semanticCommandCatalog{
		Path:     semanticPath,
		Models:   models,
		Invalid:  invalid,
		Selected: selected,
		Connections: &semanticConnectionResolver{
			ctx:      ctx,
			cm:       cm,
			override: c.String("connection"),
			logger:   makeLogger(isDebug),
		},
		Prefixer: &semanticSchemaPrefixer{prefix: cm.SelectedEnvironment.SchemaPrefix},
	}, nil
}

func resolveSemanticConfigPath(configFilePath, pathArg string) (string, error) {
	if configFilePath != "" {
		return configFilePath, nil
	}
	start := pathArg
	if start == "" {
		start = "."
	}
	repoRoot, err := git.FindRepoFromPath(start)
	if err != nil {
		return "", errors.Wrap(err, "failed to find the git repository root; pass --config-file to point at your .bruin.yml")
	}
	return filepath.Join(repoRoot.Path, ".bruin.yml"), nil
}

// selectSemanticModels returns the sorted names to operate on. Invalid models
// are included so that their load errors are reported; unknown names fail.
func selectSemanticModels(models map[string]*semantic.Model, invalid map[string]error, requested []string) ([]string, error) {
	if len(requested) == 0 {
		names := make([]string, 0, len(models)+len(invalid))
		names = append(names, semantic.Names(models)...)
		for name := range invalid {
			names = append(names, name)
		}
		sort.Strings(names)
		return names, nil
	}

	seen := make(map[string]bool, len(requested))
	names := make([]string, 0, len(requested))
	for _, raw := range requested {
		name := strings.TrimSpace(raw)
		if name == "" || seen[name] {
			continue
		}
		if _, ok := models[name]; !ok {
			if _, invalidOK := invalid[name]; !invalidOK {
				return nil, unknownSemanticModelError(name, models, invalid)
			}
		}
		seen[name] = true
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// unknownSemanticModelError reports a requested model that did not load. Files
// that failed to parse are keyed by path rather than model name, so their
// errors are listed too: the requested model is likely one of them.
func unknownSemanticModelError(name string, models map[string]*semantic.Model, invalid map[string]error) error {
	msg := fmt.Sprintf("semantic model %q not found; available models: %s", name, strings.Join(semantic.Names(models), ", "))
	if len(invalid) == 0 {
		return errors.New(msg)
	}
	paths := make([]string, 0, len(invalid))
	for path := range invalid {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	lines := []string{msg, "these semantic model files failed to load:"}
	for _, path := range paths {
		lines = append(lines, fmt.Sprintf("  %s: %v", path, invalid[path]))
	}
	return errors.New(strings.Join(lines, "\n"))
}

// semanticConnectionResolver lazily builds the connection manager and resolves
// the connection a model should use: the --connection override first, then
// source.connection from the model file.
type semanticConnectionResolver struct {
	ctx      context.Context //nolint:containedctx
	cm       *config.Config
	override string
	logger   logger.Logger

	manager    config.ConnectionAndDetailsGetter
	managerErr error
}

type resolvedSemanticConnection struct {
	Name string
	Conn interface{}
	// Dialect selects dialect-specific check SQL and is also the dialect the
	// schema-prefix rewrite parses it in, so the rewrite keeps that SQL as is.
	Dialect string
	// FromFlag is true when the name came from --connection rather than the
	// model file, which makes a missing connection a hard error.
	FromFlag bool
}

func (r *semanticConnectionResolver) connectionName(model *semantic.Model) (string, bool) {
	if r.override != "" {
		return r.override, true
	}
	if model != nil {
		return strings.TrimSpace(model.Source.Connection), false
	}
	return "", false
}

// resolve returns nil without error when the model has no connection configured.
func (r *semanticConnectionResolver) resolve(model *semantic.Model) (*resolvedSemanticConnection, error) {
	name, fromFlag := r.connectionName(model)
	if name == "" {
		return nil, nil //nolint:nilnil
	}

	if r.manager == nil && r.managerErr == nil {
		manager, errs := connectionManagerFromConfig(r.ctx, r.cm, r.logger)
		if len(errs) > 0 {
			r.managerErr = errors.Wrap(errs[0], "failed to create connection manager")
		} else {
			r.manager = manager
		}
	}
	if r.managerErr != nil {
		return nil, r.managerErr
	}

	conn := r.manager.GetConnection(name)
	if conn == nil {
		return nil, config.NewConnectionNotFoundError(r.ctx, "", name)
	}
	if !semanticcheck.SupportsQueries(conn) {
		return nil, errors.Errorf("connection '%s' does not support running queries", name)
	}
	return &resolvedSemanticConnection{
		Name:     name,
		Conn:     conn,
		Dialect:  semanticCheckDialect(r.manager.GetConnectionType(name)),
		FromFlag: fromFlag,
	}, nil
}

// semanticCheckDialect maps a connection type to the dialect used for
// dialect-specific check SQL. Sail parses as Trino for lineage but runs Spark
// SQL, which has RLIKE instead of REGEXP_LIKE.
func semanticCheckDialect(connectionType string) string {
	if connectionType == "sail" {
		return "spark"
	}
	return sqlparser.ConnectionTypeToDialect(connectionType)
}

// semanticSchemaPrefixer rewrites schema.table references to the selected
// environment's schema_prefix, so checks read the same tables as
// `bruin query --semantic-model`. The SQL parser starts on first use.
type semanticSchemaPrefixer struct {
	prefix string
	parser *sqlparser.SQLParser
	err    error
}

func (p *semanticSchemaPrefixer) rewrite(sql, dialect string) (string, error) {
	if p == nil || p.prefix == "" || dialect == "" {
		return sql, nil
	}
	if p.parser == nil && p.err == nil {
		parser, err := sqlparser.NewSQLParser(false)
		if err != nil {
			p.err = errors.Wrap(err, "failed to initialize SQL parser")
		} else if err := parser.Start(); err != nil {
			_ = parser.Close()
			p.err = errors.Wrap(err, "failed to start SQL parser")
		} else {
			p.parser = parser
		}
	}
	if p.err != nil {
		return "", p.err
	}
	rewritten, err := prefixTableSchemas(sql, dialect, p.prefix, p.parser)
	if err != nil {
		return "", errors.Wrap(err, "failed to apply schema prefix")
	}
	return rewritten, nil
}

func (p *semanticSchemaPrefixer) rewriteChecks(checks []semantic.CompiledCheck, dialect string) error {
	for i := range checks {
		sql, err := p.rewrite(checks[i].SQL, dialect)
		if err != nil {
			return err
		}
		checks[i].SQL = sql
		if checks[i].ValidationSQL != "" {
			if checks[i].ValidationSQL, err = p.rewrite(checks[i].ValidationSQL, dialect); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *semanticSchemaPrefixer) Close() {
	if p != nil && p.parser != nil {
		_ = p.parser.Close()
	}
}

// --- validate ---

type semanticModelValidation struct {
	Name       string                 `json:"name"`
	Connection string                 `json:"connection,omitempty"`
	Checks     int                    `json:"checks"`
	Valid      bool                   `json:"valid"`
	Errors     []string               `json:"errors,omitempty"`
	Warnings   []string               `json:"warnings,omitempty"`
	Results    []semanticcheck.Result `json:"check_validations,omitempty"`
}

type semanticValidationReport struct {
	Path   string                    `json:"path"`
	Valid  bool                      `json:"valid"`
	Models []semanticModelValidation `json:"models"`
}

func validateSemanticCatalog(ctx context.Context, catalog *semanticCommandCatalog) *semanticValidationReport {
	report := &semanticValidationReport{Path: catalog.Path, Valid: true, Models: []semanticModelValidation{}}
	for _, name := range catalog.Selected {
		entry := validateSemanticModel(ctx, catalog, name)
		if !entry.Valid {
			report.Valid = false
		}
		report.Models = append(report.Models, entry)
	}
	return report
}

func validateSemanticModel(ctx context.Context, catalog *semanticCommandCatalog, name string) semanticModelValidation {
	entry := semanticModelValidation{Name: name, Valid: true}
	fail := func(err error) semanticModelValidation {
		entry.Valid = false
		entry.Errors = append(entry.Errors, err.Error())
		return entry
	}

	if err, invalid := catalog.Invalid[name]; invalid {
		return fail(err)
	}
	model := catalog.Models[name]
	entry.Checks = semantic.CountChecks(model)

	engine, err := semantic.NewEngineWithModels(model, catalog.Models)
	if err != nil {
		return fail(err)
	}

	resolved, err := catalog.Connections.resolve(model)
	if err != nil {
		connName, fromFlag := catalog.Connections.connectionName(model)
		if fromFlag {
			return fail(err)
		}
		entry.Warnings = append(entry.Warnings, fmt.Sprintf("connection '%s' is not usable in this environment, skipped warehouse validation: %s", connName, err))
	}

	dialect := ""
	if resolved != nil {
		dialect = resolved.Dialect
		entry.Connection = resolved.Name
	}
	checks, err := engine.CompileChecks(semantic.CompileChecksOptions{Dialect: dialect})
	if err != nil {
		return fail(err)
	}
	validationQueries, err := engine.ValidationQueries()
	if err != nil {
		return fail(errors.Wrap(err, "failed to build the model validation queries"))
	}

	if resolved == nil {
		if len(entry.Warnings) == 0 {
			entry.Warnings = append(entry.Warnings, "no connection configured, skipped warehouse validation; set source.connection or pass --connection")
		}
		return entry
	}

	if err := catalog.Prefixer.rewriteChecks(checks, resolved.Dialect); err != nil {
		return fail(err)
	}
	for i, sql := range validationQueries {
		if validationQueries[i], err = catalog.Prefixer.rewrite(sql, resolved.Dialect); err != nil {
			return fail(err)
		}
	}

	for _, sql := range validationQueries {
		if err := semanticcheck.ValidateSQL(ctx, resolved.Conn, sql); err != nil {
			fail(errors.Wrapf(err, "model query failed validation on '%s'", resolved.Name))
		}
	}

	runner := &semanticcheck.Runner{}
	entry.Results = runner.Validate(ctx, resolved.Conn, checks)
	for _, result := range entry.Results {
		if result.Status != semanticcheck.StatusPassed {
			entry.Valid = false
			entry.Errors = append(entry.Errors, fmt.Sprintf("check '%s' failed validation on '%s': %s", result.ID, resolved.Name, result.Message))
		}
	}
	return entry
}

func printSemanticValidationReport(report *semanticValidationReport) {
	fmt.Println()
	infoPrinter.Printf("Validating semantic models in '%s'...\n\n", report.Path)

	invalidCount := 0
	for _, model := range report.Models {
		checksLabel := pluralize(model.Checks, "check", "checks")
		switch {
		case !model.Valid:
			invalidCount++
			errorPrinter.Printf("✘ %s (%s)\n", model.Name, checksLabel)
			for _, e := range model.Errors {
				errorPrinter.Printf("    └── %s\n", e)
			}
		case model.Connection != "":
			successPrinter.Printf("✓ %s (%s, validated on '%s')\n", model.Name, checksLabel, model.Connection)
		default:
			successPrinter.Printf("✓ %s (%s)\n", model.Name, checksLabel)
		}
		for _, w := range model.Warnings {
			warningPrinter.Printf("    └── %s\n", w)
		}
	}

	fmt.Println()
	if invalidCount > 0 {
		infoPrinter.Printf("✘ Checked %s and found %s, please check above.\n",
			pluralize(len(report.Models), "semantic model", "semantic models"),
			color.New(color.FgRed).Sprint(pluralize(invalidCount, "issue", "issues")))
		return
	}
	successPrinter.Printf("✓ Successfully validated %s, all good.\n", pluralize(len(report.Models), "semantic model", "semantic models"))
}

// --- check ---

type semanticCheckReport struct {
	Path    string                 `json:"path"`
	Results []semanticcheck.Result `json:"results"`
	Summary semanticcheck.Summary  `json:"summary"`
	Errors  []string               `json:"errors,omitempty"`
}

func runSemanticChecks(ctx context.Context, catalog *semanticCommandCatalog) *semanticCheckReport {
	report := &semanticCheckReport{Path: catalog.Path, Results: []semanticcheck.Result{}}
	runner := &semanticcheck.Runner{}

	for _, name := range catalog.Selected {
		if err, invalid := catalog.Invalid[name]; invalid {
			report.Errors = append(report.Errors, err.Error())
			continue
		}
		model := catalog.Models[name]
		if semantic.CountChecks(model) == 0 {
			continue
		}

		engine, err := semantic.NewEngineWithModels(model, catalog.Models)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("model '%s': %s", name, err))
			continue
		}

		resolved, err := catalog.Connections.resolve(model)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("model '%s': %s", name, err))
			continue
		}
		if resolved == nil {
			report.Errors = append(report.Errors, fmt.Sprintf("model '%s' has no connection; set source.connection in the model or pass --connection", name))
			continue
		}

		checks, err := engine.CompileChecks(semantic.CompileChecksOptions{Dialect: resolved.Dialect})
		if err != nil {
			report.Errors = append(report.Errors, err.Error())
			continue
		}
		if err := catalog.Prefixer.rewriteChecks(checks, resolved.Dialect); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("model '%s': %s", name, err))
			continue
		}

		report.Results = append(report.Results, runner.Run(ctx, resolved.Conn, checks)...)
	}

	semanticcheck.SortResults(report.Results)
	report.Summary = semanticcheck.Summarize(report.Results)
	return report
}

func printSemanticCheckReport(report *semanticCheckReport) {
	fmt.Println()
	infoPrinter.Printf("Running semantic quality checks in '%s'...\n\n", report.Path)

	currentModel := ""
	for _, result := range report.Results {
		if result.Model != currentModel {
			if currentModel != "" {
				fmt.Println()
			}
			currentModel = result.Model
			infoPrinter.Printf("%s\n", result.Model)
		}
		label := semanticResultLabel(result)
		switch result.Status {
		case semanticcheck.StatusPassed:
			successPrinter.Printf("  ✓ %s (%dms)\n", label, result.DurationMS)
		case semanticcheck.StatusSkipped:
			warningPrinter.Printf("  - %s: %s\n", label, result.Message)
		case semanticcheck.StatusFailed, semanticcheck.StatusError:
			errorPrinter.Printf("  ✘ %s (%dms)\n", label, result.DurationMS)
			errorPrinter.Printf("      └── %s\n", result.Message)
		}
	}

	for _, e := range report.Errors {
		if currentModel != "" {
			fmt.Println()
			currentModel = ""
		}
		errorPrinter.Printf("✘ %s\n", e)
	}

	fmt.Println()
	summary := report.Summary
	if summary.OK() && len(report.Errors) == 0 {
		successPrinter.Printf("✓ %s passed, all good.\n", pluralize(summary.Passed, "check", "checks"))
		return
	}
	parts := []string{color.New(color.FgGreen).Sprint(pluralize(summary.Passed, "check passed", "checks passed"))}
	if summary.Failed > 0 {
		parts = append(parts, color.New(color.FgRed).Sprint(pluralize(summary.Failed, "check failed", "checks failed")))
	}
	if summary.Errors > 0 {
		parts = append(parts, color.New(color.FgRed).Sprint(pluralize(summary.Errors, "check errored", "checks errored")))
	}
	if len(report.Errors) > 0 {
		parts = append(parts, color.New(color.FgRed).Sprint(pluralize(len(report.Errors), "model could not run", "models could not run")))
	}
	infoPrinter.Printf("✘ %s, please check above.\n", strings.Join(parts, ", "))
}

func semanticResultLabel(result semanticcheck.Result) string {
	switch result.Scope {
	case semantic.CheckScopeDimension:
		return fmt.Sprintf("dimension %s: %s", result.Target, result.Name)
	case semantic.CheckScopeMetric:
		return fmt.Sprintf("metric %s: %s", result.Target, result.Name)
	case semantic.CheckScopeModel:
		return "check " + result.Name
	default:
		return result.ID
	}
}

func pluralize(count int, singular, plural string) string {
	if count == 1 {
		return fmt.Sprintf("%d %s", count, singular)
	}
	return fmt.Sprintf("%d %s", count, plural)
}
