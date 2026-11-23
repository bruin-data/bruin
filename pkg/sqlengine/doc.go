// Package sqlengine parses, generates and analyzes SQL across Bruin's dialects: tokenizer,
// expression tree, per-dialect parser and generator, transforms, scope analysis, schema,
// optimizer rules (qualify, type annotation, simplification, subquery unnesting/merging, ...) and
// column lineage.
//
// Supported dialects: the base dialect (""), athena, bigquery, clickhouse, databricks, doris,
// duckdb, fabric, hive, mysql, oracle, postgres, presto, redshift, snowflake, spark, spark2,
// sqlite, starrocks, trino and tsql. Dialect-specific behavior lives in d_<dialect>*.go and
// typing_<dialect>.go; the per-dialect settings tables are in *_settings.go.
//
// Errors: entry points return ParseError, TokenError, ValueError, OptimizeError, SchemaError or
// UnsupportedError values.
//
// The package is safe for concurrent use; dialect prototypes are built once and shared read-only.
//
// The package is a Go fork of sqlglot (MIT licensed, see LICENSE.sqlglot), originally ported from
// v30.13.0. The golden tests in testdata/golden pin its behavior; see README.md.
package sqlengine
