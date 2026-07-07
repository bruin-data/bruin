package ingestr

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
	"golang.org/x/mod/semver"
)

// versionPattern matches the bare family marker (vMAJOR) or a fully-qualified
// vMAJOR.MINOR.PATCH. MAJOR is any non-negative integer with no leading zero
// (other than the literal "0").
var versionPattern = regexp.MustCompile(`^v(0|[1-9]\d*)(\.\d+\.\d+)?$`)

const (
	versionFamilyV0 = "v0"
	versionFamilyV1 = "v1"
)

// resolvedEngine is the outcome of parsing parameters.version on an ingestr asset.
type resolvedEngine struct {
	family         string
	ingestrVersion string
}

// resolveIngestrEngine reads parameters.version, validates it, and returns the
// resolved engine. An empty version defaults to v1 (the current pinned release).
// Bare family markers (v0, v1, ...) resolve to the corresponding pinned PyPI
// version; fully-qualified vMAJOR.MINOR.PATCH overrides to an exact PyPI version.
func resolveIngestrEngine(asset *pipeline.Asset) (resolvedEngine, error) {
	versionRaw, _ := asset.Parameters.GetString("version")
	versionParam := strings.TrimSpace(versionRaw)

	if versionParam == "" {
		return resolvedEngine{family: versionFamilyV1, ingestrVersion: python.IngestrVersionV1}, nil
	}

	match := versionPattern.FindStringSubmatch(versionParam)
	if match == nil {
		return resolvedEngine{}, fmt.Errorf("invalid parameters.version %q: expected vMAJOR or vMAJOR.MINOR.PATCH", versionParam)
	}

	if match[2] != "" {
		family := versionFamilyV1
		if match[1] == "0" {
			family = versionFamilyV0
		}
		return resolvedEngine{family: family, ingestrVersion: strings.TrimPrefix(versionParam, "v")}, nil
	}

	if match[1] == "0" {
		return resolvedEngine{family: versionFamilyV0, ingestrVersion: python.IngestrVersionV0}, nil
	}
	return resolvedEngine{family: versionFamilyV1, ingestrVersion: python.IngestrVersionV1}, nil
}

// appendQueryAnnotations adds the --query-annotations flag carrying the
// pipeline/asset identity merged with the run-level annotations supplied via
// --query-annotations / BRUIN_QUERY_ANNOTATIONS (e.g. project/run_id/try_number
// from the orchestrator). ingestr adds type and ingestr_step itself.
//
// It uses ansisql.BuildAnnotationJSON — the same merge the native SQL operators
// use — so ingestr queries are annotated and attributed identically. Like those
// operators it is opt-in: when no run-level annotations are configured
// BuildAnnotationJSON returns "" and nothing is forwarded. An invalid
// annotations payload surfaces as an error (matching the SQL path) rather than
// being silently dropped.
//
// Only the v1 engine (the Go ingestr) understands the flag; the legacy v0
// PyPI release does not, so passing it there would fail with an unknown flag.
// The flag is therefore gated on the resolved engine family.
func appendQueryAnnotations(ctx context.Context, args []string, engine resolvedEngine, ti scheduler.TaskInstance) ([]string, error) {
	if engine.family != versionFamilyV1 {
		return args, nil
	}

	asset := ti.GetAsset()
	p := ti.GetPipeline()
	if asset == nil || p == nil || asset.Name == "" || p.Name == "" {
		return args, nil
	}

	baseline := map[string]interface{}{
		"pipeline": p.Name,
		"asset":    asset.Name,
	}

	payload, err := ansisql.BuildAnnotationJSON(ctx, baseline)
	if err != nil {
		return nil, fmt.Errorf("failed to build query annotations: %w", err)
	}
	if payload == "" {
		return args, nil
	}

	return append(args, "--query-annotations", payload), nil
}

// fabricMinIngestrVersion is the first ingestr release that ships the Microsoft
// Fabric source and destination.
const fabricMinIngestrVersion = "1.0.5"

// ensureFabricEngineSupport rejects fabric ingestr assets resolved to an ingestr
// version that predates Fabric support (added in 1.0.5). The v0 family never had
// fabric, and explicit v1 pins below 1.0.5 lack it too.
func ensureFabricEngineSupport(engine resolvedEngine, uris ...string) error {
	usesFabric := false
	for _, uri := range uris {
		if strings.HasPrefix(uri, "fabric://") {
			usesFabric = true
			break
		}
	}
	if !usesFabric {
		return nil
	}

	if semver.Compare("v"+engine.ingestrVersion, "v"+fabricMinIngestrVersion) < 0 {
		return fmt.Errorf("microsoft fabric ingestr assets require ingestr %s or newer, but parameters.version resolved to %s", fabricMinIngestrVersion, engine.ingestrVersion)
	}

	return nil
}

// applyIngestrEngine stashes the resolved PyPI version on the context so the
// uv runner installs the requested release.
func applyIngestrEngine(ctx context.Context, engine resolvedEngine) context.Context {
	if engine.ingestrVersion == "" {
		return ctx
	}
	return context.WithValue(ctx, python.CtxIngestrVersion, engine.ingestrVersion)
}

type repoFinder interface {
	Repo(path string) (*git.Repo, error)
}

type ingestrRunner interface {
	RunIngestr(ctx context.Context, args, extraPackages []string, repo *git.Repo) error
}

type BasicOperator struct {
	conn          config.ConnectionGetter
	runner        ingestrRunner
	finder        repoFinder
	jinjaRenderer jinja.RendererInterface
}

type SeedOperator struct {
	conn          config.ConnectionGetter
	runner        ingestrRunner
	finder        repoFinder
	jinjaRenderer jinja.RendererInterface
}

type pipelineConnection interface {
	GetIngestrURI() (string, error)
}

func NewBasicOperator(conn config.ConnectionGetter, j jinja.RendererInterface) (*BasicOperator, error) {
	uvRunner := &python.UvPythonRunner{
		UvInstaller: &python.UvChecker{},
		Cmd:         &python.CommandRunner{},
	}

	return &BasicOperator{conn: conn, runner: uvRunner, finder: &git.RepoFinder{}, jinjaRenderer: j}, nil
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

	if err := applyMaterializationParameters(asset); err != nil {
		return err
	}

	// Source connection
	sourceConnectionName, ok := asset.Parameters.GetString("source_connection")
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
	if sourceVal, _ := asset.Parameters.GetString("source"); sourceVal == "gsheets" {
		sourceURI = strings.ReplaceAll(sourceURI, "bigquery://", "gsheets://")
	}

	// Handle CDC mode - transform the source URI into ingestr's CDC scheme and auto-set merge strategy.
	// Supported today: PostgreSQL (postgres+cdc), the MySQL family (mysql+cdc / mariadb+cdc),
	// Vitess (vitess+cdc, VStream) and PlanetScale (ps_mysql+cdc, psdbconnect).
	if cdcVal, _ := asset.Parameters.GetString("cdc"); cdcVal == "true" {
		// url.Parse rejects schemes containing an underscore (PlanetScale's ps_mysql), so split
		// the scheme off manually, parse the remainder under a placeholder, and restore the real
		// scheme. This mirrors how ingestr parses its own MySQL-family URIs.
		scheme, rest, found := strings.Cut(sourceURI, "://")
		if !found {
			return fmt.Errorf("source URI %q has no scheme, cannot enable CDC", sourceURI)
		}
		parsedURI, err := url.Parse("placeholder://" + rest)
		if err != nil {
			return fmt.Errorf("failed to parse source URI for CDC: %w", err)
		}
		parsedURI.Scheme = scheme

		switch {
		case strings.Contains(parsedURI.Scheme, "postgresql"):
			parsedURI.Scheme = strings.ReplaceAll(parsedURI.Scheme, "postgresql", "postgres+cdc")
		case strings.HasPrefix(parsedURI.Scheme, "mysql"), strings.HasPrefix(parsedURI.Scheme, "mariadb"),
			strings.HasPrefix(parsedURI.Scheme, "vitess"), strings.HasPrefix(parsedURI.Scheme, "ps_mysql"):
			// mysql+cdc / mariadb+cdc / vitess+cdc / ps_mysql+cdc are all valid CDC schemes.
			if !strings.HasSuffix(parsedURI.Scheme, "+cdc") {
				parsedURI.Scheme += "+cdc"
			}
		}

		q := parsedURI.Query()
		// PostgreSQL logical-replication parameters.
		if pub, _ := asset.Parameters.GetString("cdc_publication"); pub != "" {
			q.Set("publication", pub)
		}
		if slot, _ := asset.Parameters.GetString("cdc_slot"); slot != "" {
			q.Set("slot", slot)
		}
		// Vitess VStream parameters. The old cdc_backend selector is gone: ingestr routes
		// Vitess/PlanetScale purely by scheme (vitess+cdc / ps_mysql+cdc) since v1.0.62.
		if grpcPort, _ := asset.Parameters.GetString("cdc_grpc_port"); grpcPort != "" {
			q.Set("grpc_port", grpcPort)
		}
		if grpcHost, _ := asset.Parameters.GetString("cdc_grpc_host"); grpcHost != "" {
			q.Set("grpc_host", grpcHost)
		}
		if grpcTLS, _ := asset.Parameters.GetString("cdc_grpc_tls"); grpcTLS != "" {
			q.Set("grpc_tls", grpcTLS)
		}
		// MySQL binlog replication identifier.
		if serverID, _ := asset.Parameters.GetString("cdc_server_id"); serverID != "" {
			q.Set("server_id", serverID)
		}
		if tls, _ := asset.Parameters.GetString("cdc_tls"); tls != "" {
			q.Set("tls", tls)
		}
		// Shared parameters.
		if mode, _ := asset.Parameters.GetString("cdc_mode"); mode != "" {
			q.Set("mode", mode)
		}
		if destSchema, _ := asset.Parameters.GetString("cdc_dest_schema"); destSchema != "" {
			q.Set("dest_schema", destSchema)
		}
		parsedURI.RawQuery = q.Encode()

		sourceURI = parsedURI.String()

		// Auto-set merge strategy for CDC if not already set
		if _, exists := asset.Parameters["incremental_strategy"]; !exists {
			asset.Parameters["incremental_strategy"] = "merge"
		}
	}

	sourceTable, ok := asset.Parameters.GetString("source_table")
	if !ok {
		return errors.New("source table not configured")
	}

	fileType, ok := asset.Parameters.GetString("file_type")
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

	destTable := resolveDestinationTableName(asset)

	extraPackages = python.AddExtraPackages(destURI, sourceURI, extraPackages)

	baseArgs := []string{
		"ingest",
		"--source-uri",
		sourceURI,
	}

	// Omit --source-table for CDC wildcard mode so ingestr replicates all tables
	if cdcWild, _ := asset.Parameters.GetString("cdc"); cdcWild != "true" || sourceTable != "*" {
		baseArgs = append(baseArgs, "--source-table", sourceTable)
	}

	baseArgs = append(
		baseArgs,
		"--dest-uri",
		destURI,
		"--dest-table",
		destTable,
		"--yes",
		"--progress",
		"log",
	)

	if executor.IsDebugMode(ctx) {
		baseArgs = append(baseArgs, "--debug")
	}

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
	if err := ensureFabricEngineSupport(engine, sourceURI, destURI); err != nil {
		return err
	}
	ctx = applyIngestrEngine(ctx, engine)

	cmdArgs, err = appendQueryAnnotations(ctx, cmdArgs, engine, ti)
	if err != nil {
		return err
	}

	return o.runner.RunIngestr(ctx, cmdArgs, extraPackages, repo)
}

// seedFileSchemes maps a file_type / extension token to the URI scheme that
// ingestr expects for local file sources. Keep the keys lower-case.
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

func applyClickHouseEngineParams(destURI string, params pipeline.ParameterMap) string {
	parsedURI, err := url.Parse(destURI)
	if err != nil {
		return destURI
	}

	q := parsedURI.Query()

	if engine, exists := params.GetString("engine"); exists && engine != "" {
		q.Set("engine", engine)
	}

	for key := range params {
		key = strings.TrimSpace(key)
		value, _ := params.GetString(key)
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

func applyMaterializationParameters(asset *pipeline.Asset) error {
	if isCDCAsset(asset) {
		strategy, _ := asset.Parameters.GetString("incremental_strategy")
		if strategy = strings.TrimSpace(strategy); strategy != "" && strategy != "merge" {
			return errors.New("cdc ingestr assets require incremental_strategy \"merge\"")
		}
	}

	mat := asset.Materialization
	if mat.Type == pipeline.MaterializationTypeNone {
		return nil
	}

	if mat.Type != pipeline.MaterializationTypeTable {
		return fmt.Errorf("ingestr assets only support materialization type %q", pipeline.MaterializationTypeTable)
	}

	if asset.Parameters == nil {
		asset.Parameters = pipeline.ParameterMap{}
	}

	effectiveStrategy := ""
	if current, exists := asset.Parameters.GetString("incremental_strategy"); exists {
		effectiveStrategy = strings.TrimSpace(current)
	}

	if mat.Strategy != pipeline.MaterializationStrategyNone {
		strategy, ok := python.TranslateBruinMaterializationStrategyToIngestr(mat.Strategy)
		if !ok {
			return fmt.Errorf("materialization strategy %q is not supported for ingestr assets", mat.Strategy)
		}
		if err := setMaterializationParameter(asset.Parameters, "incremental_strategy", strategy, "materialization.strategy"); err != nil {
			return err
		}
		effectiveStrategy = strategy
	}

	if isCDCAsset(asset) && effectiveStrategy != "" && effectiveStrategy != "merge" {
		return errors.New("cdc ingestr assets require incremental_strategy \"merge\"")
	}

	if hasIngestrIncrementalKey(asset, mat) && !python.IsIngestrIncrementalKeyStrategy(effectiveStrategy) {
		return errors.New("materialization.incremental_key is only supported for append, merge, and delete+insert strategies on ingestr assets")
	}

	if err := setMaterializationParameter(asset.Parameters, "incremental_key", mat.IncrementalKey, "materialization.incremental_key"); err != nil {
		return err
	}
	if err := setMaterializationParameter(asset.Parameters, "partition_by", mat.PartitionBy, "materialization.partition_by"); err != nil {
		return err
	}
	if err := setMaterializationParameter(asset.Parameters, "cluster_by", materializationClusterBy(mat.ClusterBy), "materialization.cluster_by"); err != nil {
		return err
	}

	return nil
}

func isCDCAsset(asset *pipeline.Asset) bool {
	cdc, _ := asset.Parameters.GetString("cdc")
	return cdc == "true"
}

func hasIngestrIncrementalKey(asset *pipeline.Asset, mat pipeline.Materialization) bool {
	if strings.TrimSpace(mat.IncrementalKey) != "" {
		return true
	}
	key, _ := asset.Parameters.GetString("incremental_key")
	return strings.TrimSpace(key) != ""
}

func setMaterializationParameter(params pipeline.ParameterMap, key, value, source string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	if current, exists := params.GetString(key); exists && strings.TrimSpace(current) != "" {
		if normalizeMaterializationParameter(key, current) != normalizeMaterializationParameter(key, value) {
			return fmt.Errorf("ingestr asset defines both parameters.%s=%q and %s=%q", key, current, source, value)
		}
		return nil
	}

	params[key] = value
	return nil
}

func materializationClusterBy(clusterBy []string) string {
	cleaned := make([]string, 0, len(clusterBy))
	for _, value := range clusterBy {
		value = strings.TrimSpace(value)
		if value != "" {
			cleaned = append(cleaned, value)
		}
	}
	return strings.Join(cleaned, ",")
}

func normalizeMaterializationParameter(key, value string) string {
	value = strings.TrimSpace(value)
	if key != "cluster_by" {
		return value
	}

	parts := strings.Split(value, ",")
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			cleaned = append(cleaned, part)
		}
	}
	return strings.Join(cleaned, ",")
}

func resolveDestinationTableName(asset *pipeline.Asset) string {
	if value, exists := asset.Parameters.GetString("destination_table"); exists {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}

	return asset.Name
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

	sourceConnectionPath, ok := asset.Parameters.GetString("path")
	if !ok {
		return errors.New("source connection not configured")
	}

	seedFileType, _ := asset.Parameters.GetString("file_type")
	sourceURI, err := resolveSeedSourceURI(sourceConnectionPath, seedFileType, filepath.Dir(asset.ExecutableFile.Path))
	if err != nil {
		return err
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

	baseArgs := []string{
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
	}

	if executor.IsDebugMode(ctx) {
		baseArgs = append(baseArgs, "--debug")
	}

	cmdArgs, err := python.ConsolidatedParameters(ctx, asset, baseArgs, &python.ColumnHintOptions{
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
	if err := ensureFabricEngineSupport(engine, destURI); err != nil {
		return err
	}
	ctx = applyIngestrEngine(ctx, engine)

	cmdArgs, err = appendQueryAnnotations(ctx, cmdArgs, engine, ti)
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
