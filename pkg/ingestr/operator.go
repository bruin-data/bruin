package ingestr

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/gong"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

// versionPattern matches the bare family marker (vMAJOR) or a fully-qualified
// vMAJOR.MINOR.PATCH. MAJOR is any non-negative integer with no leading zero
// (other than the literal "0"). Family selection is decided separately:
// MAJOR == 0 routes to ingestr, anything else routes to gong.
var versionPattern = regexp.MustCompile(`^v(0|[1-9]\d*)(\.\d+\.\d+)?$`)

const (
	versionFamilyIngestr = "v0"
	versionFamilyGong    = "v1"
)

// resolvedEngine is the outcome of parsing parameters.version on an ingestr asset.
type resolvedEngine struct {
	// family is "v0" (ingestr) or "v1" (gong).
	family string
	// ingestrVersion is the PyPI version string (no leading "v"), empty for the bundled default.
	ingestrVersion string
	// gongVersion is the gong release tag (with leading "v"), empty for the bundled default.
	gongVersion string
}

// resolveIngestrEngine reads parameters.version, validates it, and returns the
// resolved engine. An empty version defaults to v0 (ingestr) unless use_gong is
// set — preserving the legacy auto-enable for gong-required sources/destinations.
// When parameters.version is set explicitly, use_gong is ignored with a
// deprecation warning.
func resolveIngestrEngine(asset *pipeline.Asset) (resolvedEngine, error) {
	versionParam := strings.TrimSpace(asset.Parameters["version"])
	useGongLegacy := asset.Parameters["use_gong"] == "true"

	if versionParam == "" {
		if useGongLegacy {
			return resolvedEngine{family: versionFamilyGong}, nil
		}
		return resolvedEngine{family: versionFamilyIngestr}, nil
	}

	match := versionPattern.FindStringSubmatch(versionParam)
	if match == nil {
		return resolvedEngine{}, fmt.Errorf("invalid parameters.version %q: expected vMAJOR or vMAJOR.MINOR.PATCH", versionParam)
	}

	if useGongLegacy {
		fmt.Fprintf(os.Stderr, "Warning: parameters.use_gong is ignored when parameters.version is set; version=%q wins.\n", versionParam)
	}

	major := match[1]
	if major == "0" {
		out := resolvedEngine{family: versionFamilyIngestr}
		if versionParam != versionFamilyIngestr {
			// Strip the leading "v" to get the PyPI version (e.g. v0.14.2 -> 0.14.2).
			out.ingestrVersion = strings.TrimPrefix(versionParam, "v")
		}
		return out, nil
	}

	out := resolvedEngine{family: versionFamilyGong}
	// Only fully-qualified versions get pinned; bare family markers (v1, v2, ...)
	// fall back to bruin's bundled gong default.
	if match[2] != "" {
		out.gongVersion = versionParam
	}
	return out, nil
}

// applyIngestrEngine applies the resolved engine choice to ctx and asset:
//   - For v0: clears use_gong (so downstream code paths run ingestr) and sets
//     CtxIngestrVersion when an exact version was requested. Warns if the source
//     or destination scheme is in gongSources/gongDestinations.
//   - For v1: ensures gong is installed (using the requested version) and sets
//     CtxGongPath, plus use_gong for legacy code paths that still inspect it.
func applyIngestrEngine(ctx context.Context, asset *pipeline.Asset, engine resolvedEngine, sourceScheme, destScheme string, installer gongInstaller) (context.Context, error) {
	if engine.family == versionFamilyIngestr {
		_, srcAuto := gongSources[sourceScheme]
		_, dstAuto := gongDestinations[destScheme]
		if srcAuto || dstAuto {
			fmt.Fprintf(os.Stderr,
				"Warning: parameters.version=v0 selected but source/destination (%s/%s) typically requires gong; running on ingestr anyway.\n",
				sourceScheme, destScheme,
			)
		}
		// Make sure no stale use_gong leaks through to downstream (e.g. the auto-enable
		// blocks earlier in Run set it). The user explicitly asked for v0.
		delete(asset.Parameters, "use_gong")
		if engine.ingestrVersion != "" {
			ctx = context.WithValue(ctx, python.CtxIngestrVersion, engine.ingestrVersion)
		}
		return ctx, nil
	}

	// v1 (gong)
	if asset.Parameters == nil {
		asset.Parameters = make(map[string]string)
	}
	asset.Parameters["use_gong"] = "true"

	if ctx.Value(python.CtxGongPath) != nil {
		// Already installed for this run (e.g. via --gong-path or a prior asset).
		return ctx, nil
	}
	if installer == nil {
		return ctx, errors.New("gong installer is not available but is required for parameters.version=v1")
	}
	gongPath, err := installer.EnsureGongInstalled(ctx, engine.gongVersion)
	if err != nil {
		return ctx, fmt.Errorf("failed to install gong %s: %w", displayGongVersion(engine.gongVersion), err)
	}
	return context.WithValue(ctx, python.CtxGongPath, gongPath), nil
}

func displayGongVersion(v string) string {
	if v == "" {
		return "(default)"
	}
	return v
}

type repoFinder interface {
	Repo(path string) (*git.Repo, error)
}

type ingestrRunner interface {
	RunIngestr(ctx context.Context, args, extraPackages []string, repo *git.Repo) error
}

type gongInstaller interface {
	EnsureGongInstalled(ctx context.Context, version string) (string, error)
}

type BasicOperator struct {
	conn          config.ConnectionGetter
	runner        ingestrRunner
	finder        repoFinder
	jinjaRenderer jinja.RendererInterface
	gong          gongInstaller
}

type SeedOperator struct {
	conn          config.ConnectionGetter
	runner        ingestrRunner
	finder        repoFinder
	jinjaRenderer jinja.RendererInterface
	gong          gongInstaller
}

type pipelineConnection interface {
	GetIngestrURI() (string, error)
}

func NewBasicOperator(conn config.ConnectionGetter, j jinja.RendererInterface) (*BasicOperator, error) {
	uvRunner := &python.UvPythonRunner{
		UvInstaller: &python.UvChecker{},
		Cmd:         &python.CommandRunner{},
	}

	return &BasicOperator{conn: conn, runner: uvRunner, finder: &git.RepoFinder{}, jinjaRenderer: j, gong: &gong.Checker{}}, nil
}

func (o *BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	var extraPackages []string

	asset := ti.GetAsset()

	// Render interval modifier templates if they exist
	if asset.IntervalModifiers.Start.Template != "" {
		renderedStart, err := asset.IntervalModifiers.Start.ResolveTemplateToNew(o.jinjaRenderer)
		if err != nil {
			return fmt.Errorf("failed to render start interval modifier template: %w", err)
		}
		asset.IntervalModifiers.Start = renderedStart
	}

	if asset.IntervalModifiers.End.Template != "" {
		renderedEnd, err := asset.IntervalModifiers.End.ResolveTemplateToNew(o.jinjaRenderer)
		if err != nil {
			return fmt.Errorf("failed to render end interval modifier template: %w", err)
		}
		asset.IntervalModifiers.End = renderedEnd
	}

	// Source connection
	sourceConnectionName, ok := asset.Parameters["source_connection"]
	if !ok {
		return errors.New("source connection not configured")
	}

	sourceConnection := o.conn.GetConnection(sourceConnectionName)
	if sourceConnection == nil {
		return config.NewConnectionNotFoundError(ctx, "source", sourceConnectionName)
	}

	sourceURI, err := sourceConnection.(pipelineConnection).GetIngestrURI()
	if err != nil {
		return fmt.Errorf("could not get the source uri: %w", err)
	}

	if sourceURI == "" {
		return errors.New("source uri is empty, which means the source connection is not configured correctly")
	}

	// Ensure the URI has the authority separator "//" after the scheme.
	// Some source configs build URIs without a Host, causing Go's url.URL.String()
	// to produce "scheme:?params" instead of "scheme://?params".
	if parts := strings.SplitN(sourceURI, ":", 2); len(parts) == 2 && !strings.HasPrefix(parts[1], "//") {
		sourceURI = parts[0] + "://" + parts[1]
	}

	// some connection types can be shared among sources, therefore inferring source URI from the connection type is not
	// always feasible. In the case of GSheets, we have to reuse the same GCP credentials, but change the prefix with gsheets://
	if asset.Parameters["source"] == "gsheets" {
		sourceURI = strings.ReplaceAll(sourceURI, "bigquery://", "gsheets://")
	}

	// Auto-enable gong for sources that require it
	parsed, err := url.Parse(sourceURI)
	if err != nil {
		return fmt.Errorf("failed to parse source URI: %w", err)
	}
	if _, ok := gongSources[parsed.Scheme]; ok {
		asset.Parameters["use_gong"] = "true"
	}

	// Handle CDC mode - transform PostgreSQL URI to CDC format and auto-set merge strategy
	if asset.Parameters["cdc"] == "true" {
		parsedURI, err := url.Parse(sourceURI)
		if err != nil {
			return fmt.Errorf("failed to parse source URI for CDC: %w", err)
		}

		parsedURI.Scheme = strings.ReplaceAll(parsedURI.Scheme, "postgresql", "postgres+cdc")

		q := parsedURI.Query()
		if pub := asset.Parameters["cdc_publication"]; pub != "" {
			q.Set("publication", pub)
		}
		if slot := asset.Parameters["cdc_slot"]; slot != "" {
			q.Set("slot", slot)
		}
		if mode := asset.Parameters["cdc_mode"]; mode != "" {
			q.Set("mode", mode)
		}
		if destSchema := asset.Parameters["cdc_dest_schema"]; destSchema != "" {
			q.Set("dest_schema", destSchema)
		}
		parsedURI.RawQuery = q.Encode()

		sourceURI = parsedURI.String()

		// Auto-set merge strategy for CDC if not already set
		if _, exists := asset.Parameters["incremental_strategy"]; !exists {
			asset.Parameters["incremental_strategy"] = "merge"
		}
	}

	sourceTable, ok := asset.Parameters["source_table"]
	if !ok {
		return errors.New("source table not configured")
	}

	fileType, ok := asset.Parameters["file_type"]
	if ok {
		sourceTable = sourceTable + "#" + fileType
	}

	destConnectionName, err := ti.GetPipeline().GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}

	destConnection := o.conn.GetConnection(destConnectionName)
	if destConnection == nil {
		return config.NewConnectionNotFoundError(ctx, "destination", destConnectionName)
	}

	destURI, err := destConnection.(pipelineConnection).GetIngestrURI()
	if err != nil {
		return errors.Wrap(err, "could not get the destination uri")
	}

	if destURI == "" {
		return errors.New("destination uri is empty, which means the connection is not configured correctly")
	}

	if strings.HasPrefix(destURI, "clickhouse://") {
		destURI = applyClickHouseEngineParams(destURI, asset.Parameters)
	}

	// Also enable gong when the destination requires it
	parsedDest, parseErr := url.Parse(destURI)
	if parseErr != nil {
		return fmt.Errorf("failed to parse destination URI: %w", parseErr)
	}
	if _, ok := gongDestinations[parsedDest.Scheme]; ok {
		asset.Parameters["use_gong"] = "true"
	}

	destTable := asset.Name

	extraPackages = python.AddExtraPackages(destURI, sourceURI, extraPackages)

	baseArgs := []string{
		"ingest",
		"--source-uri",
		sourceURI,
	}

	// Omit --source-table for CDC wildcard mode so ingestr replicates all tables
	if asset.Parameters["cdc"] != "true" || sourceTable != "*" {
		baseArgs = append(baseArgs, "--source-table", sourceTable)
	}

	baseArgs = append(baseArgs,
		"--dest-uri",
		destURI,
		"--dest-table",
		destTable,
		"--yes",
		"--progress",
		"log",
	)

	cmdArgs, err := python.ConsolidatedParameters(ctx, asset, baseArgs, &python.ColumnHintOptions{
		NormalizeColumnNames:   false,
		EnforceSchemaByDefault: false,
	})
	if err != nil {
		return err
	}

	path := asset.ExecutableFile.Path
	repo, err := o.finder.Repo(path)
	if err != nil {
		return errors.Wrap(err, "failed to find repo to run Ingestr")
	}

	if strings.HasPrefix(destURI, "duckdb://") {
		duck.LockDatabase(destURI)
		defer duck.UnlockDatabase(destURI)
	}

	if strings.HasPrefix(sourceURI, "duckdb://") && sourceURI != destURI {
		duck.LockDatabase(sourceURI)
		defer duck.UnlockDatabase(sourceURI)
	}

	engine, err := resolveIngestrEngine(asset)
	if err != nil {
		return err
	}
	sourceScheme, destScheme := schemeOf(sourceURI), schemeOf(destURI)
	ctx, err = applyIngestrEngine(ctx, asset, engine, sourceScheme, destScheme, o.gong)
	if err != nil {
		return err
	}

	return o.runner.RunIngestr(ctx, cmdArgs, extraPackages, repo)
}

func schemeOf(uri string) string {
	parsed, err := url.Parse(uri)
	if err != nil {
		return ""
	}
	return parsed.Scheme
}

// seedFileSchemes maps a file_type / extension token to the URI scheme that
// gongestr expects for local file sources. Keep the keys lower-case.
var seedFileSchemes = map[string]string{
	"csv":     "csv",
	"parquet": "parquet",
	"pq":      "parquet",
	"jsonl":   "jsonl",
	"ndjson":  "ndjson",
	"json":    "json",
	"avro":    "avro",
}

// resolveSeedSourceURI builds the source URI for a seed asset's local file or
// passes through an http(s) URL unchanged. The scheme is selected from the
// explicit file_type parameter when set, otherwise inferred from the file
// extension; unknown types fall back to csv for backward compatibility.
func resolveSeedSourceURI(seedPath, fileType, assetDir string) (string, error) {
	lowerPath := strings.ToLower(seedPath)
	if strings.HasPrefix(lowerPath, "http://") || strings.HasPrefix(lowerPath, "https://") {
		return seedPath, nil
	}

	var scheme string
	if ft := strings.ToLower(strings.TrimSpace(fileType)); ft != "" {
		mapped, ok := seedFileSchemes[ft]
		if !ok {
			return "", fmt.Errorf("unsupported seed file_type %q (supported: csv, parquet, json, jsonl, ndjson, avro)", fileType)
		}
		scheme = mapped
	} else {
		ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(seedPath)), ".")
		mapped, ok := seedFileSchemes[ext]
		if !ok {
			mapped = "csv"
		}
		scheme = mapped
	}

	return scheme + "://" + filepath.Join(assetDir, seedPath), nil
}

func applyClickHouseEngineParams(destURI string, params map[string]string) string {
	parsedURI, err := url.Parse(destURI)
	if err != nil {
		return destURI
	}

	q := parsedURI.Query()

	if engine, exists := params["engine"]; exists && engine != "" {
		q.Set("engine", engine)
	}

	for key, value := range params {
		key = strings.TrimSpace(key)
		if strings.HasPrefix(key, "engine.") && value != "" {
			q.Set(
				key,
				strings.TrimSpace(value),
			)
		}
	}

	parsedURI.RawQuery = q.Encode()
	return parsedURI.String()
}

func NewSeedOperator(conn config.ConnectionGetter, j jinja.RendererInterface) (*SeedOperator, error) {
	uvRunner := &python.UvPythonRunner{
		UvInstaller: &python.UvChecker{},
		Cmd:         &python.CommandRunner{},
	}

	return &SeedOperator{
		conn:          conn,
		runner:        uvRunner,
		finder:        &git.RepoFinder{},
		jinjaRenderer: j,
		gong:          &gong.Checker{},
	}, nil
}

func (o *SeedOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	var extraPackages []string
	// Source connection

	asset := ti.GetAsset()

	// Render interval modifier templates if they exist
	if asset.IntervalModifiers.Start.Template != "" {
		renderedStart, err := asset.IntervalModifiers.Start.ResolveTemplateToNew(o.jinjaRenderer)
		if err != nil {
			return fmt.Errorf("failed to render start interval modifier template: %w", err)
		}
		asset.IntervalModifiers.Start = renderedStart
	}

	if asset.IntervalModifiers.End.Template != "" {
		renderedEnd, err := asset.IntervalModifiers.End.ResolveTemplateToNew(o.jinjaRenderer)
		if err != nil {
			return fmt.Errorf("failed to render end interval modifier template: %w", err)
		}
		asset.IntervalModifiers.End = renderedEnd
	}

	sourceConnectionPath, ok := asset.Parameters["path"]
	if !ok {
		return errors.New("source connection not configured")
	}

	sourceURI, err := resolveSeedSourceURI(sourceConnectionPath, asset.Parameters["file_type"], filepath.Dir(asset.ExecutableFile.Path))
	if err != nil {
		return err
	}

	if parsedSource, err := url.Parse(sourceURI); err == nil {
		if _, ok := gongSources[parsedSource.Scheme]; ok {
			asset.Parameters["use_gong"] = "true"
		}
	}

	destConnectionName, err := ti.GetPipeline().GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}

	destConnection := o.conn.GetConnection(destConnectionName)
	if destConnection == nil {
		return config.NewConnectionNotFoundError(ctx, "destination", destConnectionName)
	}

	destURI, err := destConnection.(pipelineConnection).GetIngestrURI()
	if err != nil {
		return errors.Wrap(err, "could not get the destination uri")
	}
	if destURI == "" {
		return errors.New("destination uri is empty, which means the destination connection is not configured correctly")
	}

	destTable := o.resolveSeedDestinationTableName(destConnectionName, destURI, asset.Name)

	extraPackages = python.AddExtraPackages(destURI, sourceURI, extraPackages)

	cmdArgs, err := python.ConsolidatedParameters(ctx, asset, []string{
		"ingest",
		"--source-uri",
		sourceURI,
		"--source-table",
		"seed.raw",
		"--dest-uri",
		destURI,
		"--dest-table",
		destTable,
		"--yes",
		"--progress",
		"log",
	}, &python.ColumnHintOptions{
		NormalizeColumnNames:   true,
		EnforceSchemaByDefault: true,
	})
	if err != nil {
		return err
	}

	path := asset.ExecutableFile.Path
	repo, err := o.finder.Repo(path)
	if err != nil {
		return errors.Wrap(err, "failed to find repo to run Ingestr")
	}

	engine, err := resolveIngestrEngine(asset)
	if err != nil {
		return err
	}
	ctx, err = applyIngestrEngine(ctx, asset, engine, schemeOf(sourceURI), schemeOf(destURI), o.gong)
	if err != nil {
		return err
	}

	return o.runner.RunIngestr(ctx, cmdArgs, extraPackages, repo)
}

func (o *SeedOperator) resolveSeedDestinationTableName(connectionName, destURI, tableName string) string {
	if tableName == "" || strings.Contains(tableName, ".") {
		return tableName
	}

	parsedURI, err := url.Parse(destURI)
	if err != nil {
		return tableName
	}

	detailsGetter, ok := o.conn.(config.ConnectionDetailsGetter)
	if !ok {
		switch parsedURI.Scheme {
		case "athena", "clickhouse":
			return "default." + tableName
		default:
			return tableName
		}
	}

	details := detailsGetter.GetConnectionDetails(connectionName)
	if details == nil {
		switch parsedURI.Scheme {
		case "athena", "clickhouse":
			return "default." + tableName
		default:
			return tableName
		}
	}

	switch parsedURI.Scheme {
	case "athena":
		database := "default"
		athenaConn, ok := details.(*config.AthenaConnection)
		if ok && athenaConn != nil && athenaConn.Database != "" {
			database = athenaConn.Database
		}
		return database + "." + tableName
	case "postgresql":
		pgConn, ok := details.(*config.PostgresConnection)
		if ok && pgConn != nil && pgConn.Schema != "" {
			return pgConn.Schema + "." + tableName
		}
	case "redshift":
		redshiftConn, ok := details.(*config.RedshiftConnection)
		if ok && redshiftConn != nil && redshiftConn.Schema != "" {
			return redshiftConn.Schema + "." + tableName
		}
	case "snowflake":
		snowflakeConn, ok := details.(*config.SnowflakeConnection)
		if ok && snowflakeConn != nil && snowflakeConn.Schema != "" {
			return snowflakeConn.Schema + "." + tableName
		}
	case "clickhouse":
		database := "default"
		clickhouseConn, ok := details.(*config.ClickHouseConnection)
		if ok && clickhouseConn != nil && clickhouseConn.Database != "" {
			database = clickhouseConn.Database
		}
		return database + "." + tableName
	}

	return tableName
}
