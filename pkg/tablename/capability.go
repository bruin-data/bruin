// Package tablename centralizes how Bruin parses and validates multi-component
// asset/table names (e.g. `table`, `schema.table`, `catalog.schema.table`).
//
// Historically every platform package re-implemented its own
// `strings.Split(name, ".")` switch with subtly different rules, which led to
// the database component of three-part names being silently dropped. This
// package replaces those ad-hoc switches with a single capability-driven parser.
//
// It is intentionally a leaf package: it must not import pkg/pipeline (which
// owns AssetTypeConnectionMapping and would create an import cycle). Callers
// pass the platform string produced by pipeline.AssetTypeConnectionMapping.
package tablename

// Capability describes how a single platform interprets multi-component table
// names. It is the single source of truth for both runtime parsing and lint
// validation.
type Capability struct {
	// Platform is the connection/platform key, matching the values in
	// pipeline.AssetTypeConnectionMapping (e.g. "snowflake", "mssql").
	Platform string

	// MinComponents and MaxComponents bound how many dot-separated components a
	// name may have. A three-level platform sets MaxComponents to 3.
	MinComponents int
	MaxComponents int

	// Labels names each level for the widest supported form, used in error
	// messages and to document intent: [catalog-ish, schema-ish, table].
	// e.g. Snowflake -> {"database", "schema", "table"};
	//      BigQuery  -> {"project", "dataset", "table"}.
	Labels [3]string

	// Unbounded marks platforms (e.g. Dremio) that accept arbitrary-depth paths.
	// Component-count validation is skipped for them and they are not parsed into
	// the fixed catalog/schema/table shape.
	Unbounded bool

	// FormatDesc is the human-readable description of accepted formats, surfaced
	// in lint messages (e.g. "`schema.table` or `database.schema.table`").
	FormatDesc string
}

// registry maps platform strings to their table-name capability. Platforms with
// a genuine, single-connection-reachable third level set MaxComponents to 3;
// two-level engines cap at 2 (or 1) so an over-long name is rejected rather than
// silently mishandled.
var registry = map[string]Capability{
	// --- three-level platforms (3-part names enabled) ---
	"google_cloud_platform": {
		Platform: "google_cloud_platform", MinComponents: 2, MaxComponents: 3,
		Labels:     [3]string{"project", "dataset", "table"},
		FormatDesc: "`dataset.table` or `project.dataset.table`",
	},
	"snowflake": {
		// Min 1: a bare table resolves database+schema from connection config.
		Platform: "snowflake", MinComponents: 1, MaxComponents: 3,
		Labels:     [3]string{"database", "schema", "table"},
		FormatDesc: "`table`, `schema.table`, or `database.schema.table`",
	},
	"databricks": {
		// Min 2: Databricks requires at least schema.table (no bare-table form).
		Platform: "databricks", MinComponents: 2, MaxComponents: 3,
		Labels:     [3]string{"catalog", "schema", "table"},
		FormatDesc: "`schema.table` or `catalog.schema.table`",
	},
	"trino": {
		Platform: "trino", MinComponents: 1, MaxComponents: 3,
		Labels:     [3]string{"catalog", "schema", "table"},
		FormatDesc: "`table`, `schema.table`, or `catalog.schema.table`",
	},
	"duckdb": {
		Platform: "duckdb", MinComponents: 1, MaxComponents: 3,
		Labels:     [3]string{"catalog", "schema", "table"},
		FormatDesc: "`table`, `schema.table`, or `catalog.schema.table`",
	},
	"motherduck": {
		Platform: "motherduck", MinComponents: 1, MaxComponents: 3,
		Labels:     [3]string{"catalog", "schema", "table"},
		FormatDesc: "`table`, `schema.table`, or `catalog.schema.table`",
	},
	// --- two-level engines (3-part rejected) ---
	// NOTE: MSSQL is a genuine three-level engine (database.schema.table) and is
	// a planned next step, but its metadata path (GetColumns takes the database
	// separately via `USE [db]`) needs a caller-spanning change before 3-part can
	// be enabled end-to-end, so it is capped at 2 for now.
	"mssql": {
		Platform: "mssql", MinComponents: 1, MaxComponents: 2,
		Labels:     [3]string{"", "schema", "table"},
		FormatDesc: "`table` or `schema.table`",
	},
	"postgres": {
		Platform: "postgres", MinComponents: 1, MaxComponents: 2,
		Labels:     [3]string{"", "schema", "table"},
		FormatDesc: "`table` or `schema.table`",
	},
	"redshift": {
		Platform: "redshift", MinComponents: 1, MaxComponents: 2,
		Labels:     [3]string{"", "schema", "table"},
		FormatDesc: "`table` or `schema.table`",
	},
	"mysql": {
		Platform: "mysql", MinComponents: 1, MaxComponents: 2,
		Labels:     [3]string{"", "database", "table"},
		FormatDesc: "`table` or `schema.table`",
	},
	"clickhouse": {
		Platform: "clickhouse", MinComponents: 1, MaxComponents: 2,
		Labels:     [3]string{"", "database", "table"},
		FormatDesc: "`table` or `schema.table`",
	},
	"vertica": {
		Platform: "vertica", MinComponents: 1, MaxComponents: 2,
		Labels:     [3]string{"", "schema", "table"},
		FormatDesc: "`table` or `schema.table`",
	},
	"fabric": {
		Platform: "fabric", MinComponents: 1, MaxComponents: 2,
		Labels:     [3]string{"", "schema", "table"},
		FormatDesc: "`table` or `schema.table`",
	},
	"oracle": {
		Platform: "oracle", MinComponents: 1, MaxComponents: 2,
		Labels:     [3]string{"", "schema", "table"},
		FormatDesc: "`table` or `schema.table`",
	},
	"sail": {
		Platform: "sail", MinComponents: 1, MaxComponents: 2,
		Labels:     [3]string{"", "schema", "table"},
		FormatDesc: "`table` or `schema.table`",
	},
	"hana": {
		Platform: "hana", MinComponents: 1, MaxComponents: 2,
		Labels:     [3]string{"", "schema", "table"},
		FormatDesc: "`table` or `schema.table`",
	},

	// --- deferred: cross-catalog/cross-db reachability not yet verified ---
	"synapse": {
		Platform: "synapse", MinComponents: 1, MaxComponents: 2,
		Labels:     [3]string{"", "schema", "table"},
		FormatDesc: "`table` or `schema.table`",
	},
	"athena": {
		// Athena currently accepts a bare table only (database from config).
		Platform: "athena", MinComponents: 1, MaxComponents: 1,
		Labels:     [3]string{"", "", "table"},
		FormatDesc: "`table`",
	},

	// --- special: arbitrary-depth paths ---
	"dremio": {
		Platform: "dremio", Unbounded: true,
		Labels:     [3]string{"", "", "table"},
		FormatDesc: "a dot-separated path",
	},
}

// For returns the table-name capability for a platform string (as produced by
// pipeline.AssetTypeConnectionMapping). The boolean is false for platforms that
// do not have table-name semantics (e.g. non-SQL targets), in which case callers
// must skip component-count validation entirely.
func For(platform string) (Capability, bool) {
	c, ok := registry[platform]
	return c, ok
}
